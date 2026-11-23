//! One native resource-safety policy for bounded optional reads, independent of the scan/collector adapter.
use crate::read::{NativeRead, ReadError};
use culprit_core::telemetry::CollectionStop;

const MAX_SAMPLE_AGE_SECONDS: i64 = 60;
const PRESSURE_CPU_PERCENT: f64 = 70.0;
const PRESSURE_WORKER_PERCENT: f64 = 50.0;
const PRESSURE_IO_PERCENT: f64 = 80.0;

pub(crate) async fn admit_resources<R: NativeRead>(reader: &mut R) -> std::result::Result<(), ReadError> {
    let latest = crate::resources::latest(reader).await.map_err(|error| match error {
        ReadError::Source(ref source)
            if matches!(
                source.detail,
                Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::InvalidPage))
            ) =>
        {
            ReadError::Stopped(CollectionStop::PressureUnavailable)
        }
        error => error,
    })?;
    pressure_admission(latest.as_ref()).map_err(ReadError::Stopped)
}
fn pressure_admission(latest: Option<&crate::resources::Latest>) -> std::result::Result<(), CollectionStop> {
    let latest = latest.ok_or(CollectionStop::PressureUnavailable)?;
    admit_values(latest.age_seconds(), |metric| latest.sample.value(metric))
}
fn admit_values(
    age: Option<i64>,
    value: impl Fn(crate::resources::Metric) -> Option<f64>,
) -> std::result::Result<(), CollectionStop> {
    use crate::resources::Metric;
    let age = age.ok_or(CollectionStop::PressureUnavailable)?;
    if age > MAX_SAMPLE_AGE_SECONDS {
        return Err(CollectionStop::PressureUnavailable);
    }
    for (metric, threshold) in [
        (Metric::Cpu, PRESSURE_CPU_PERCENT),
        (Metric::InstanceCpu, PRESSURE_CPU_PERCENT),
        (Metric::Workers, PRESSURE_WORKER_PERCENT),
        (Metric::DataIo, PRESSURE_IO_PERCENT),
        (Metric::LogWrite, PRESSURE_IO_PERCENT),
    ] {
        let value = value(metric).ok_or(CollectionStop::PressureUnavailable)?;
        if value >= threshold {
            return Err(CollectionStop::Pressure);
        }
    }
    Ok(())
}

/// The pool route checks the pool, never substitutes an idle anchor's database pressure.
pub(crate) async fn admit_pool_resources(reader: &mut impl NativeRead) -> std::result::Result<(), ReadError> {
    let rows = reader.query(&super::sql::POOL.latest(), vec![], 1).await?;
    let [row] = rows.as_slice() else {
        return Err(ReadError::Stopped(CollectionStop::PressureUnavailable));
    };
    let sample = super::pool::sample::decode(row, super::pool::Pricing::Unknown)
        .map_err(|_| ReadError::Stopped(CollectionStop::PressureUnavailable))?;
    let observed =
        crate::read::text(row, "observed_at").ok().and_then(|at| chrono::DateTime::parse_from_rfc3339(&at).ok());
    let sampled = chrono::DateTime::parse_from_rfc3339(&sample.at).ok();
    let age = observed
        .zip(sampled)
        .filter(|(observed, sampled)| observed >= sampled)
        .map(|(observed, sampled)| (observed - sampled).num_seconds());
    admit_values(age, |metric| super::pool::sample::admission_value(&sample, metric)).map_err(ReadError::Stopped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{resources::test_support::*, value::Val};
    #[tokio::test]
    async fn database_cpu_saturation_refuses_new_source_work_even_when_host_cpu_is_low() {
        let row = replace(
            &latest_row("2020-01-01T06:01:59Z"),
            &[
                ("avg_cpu_percent", Val::Float(95.0)),
                ("avg_instance_cpu_percent", Val::Float(10.0)),
                ("max_worker_percent", Val::Float(0.0)),
                ("avg_data_io_percent", Val::Float(0.0)),
                ("avg_log_write_percent", Val::Float(0.0)),
            ],
        );
        let mut io = FakeRead::new(vec![Ok(vec![row])]);
        assert!(matches!(admit_resources(&mut io).await, Err(ReadError::Stopped(CollectionStop::Pressure))));
        assert_eq!(io.calls.len(), 1, "one pressure observation, with no retry");
    }

    #[tokio::test]
    async fn pressure_admission_uses_native_scope_and_rejects_missing_stale_invalid_or_future_samples() {
        let healthy = replace(
            &latest_row("2020-01-01T06:01:59Z"),
            &[
                ("avg_instance_cpu_percent", Val::Float(20.0)),
                ("max_worker_percent", Val::Float(0.0)),
                ("avg_data_io_percent", Val::Float(0.0)),
                ("avg_log_write_percent", Val::Float(0.0)),
            ],
        );
        let mut io = FakeRead::new(vec![Ok(vec![healthy.clone()])]);
        assert!(admit_resources(&mut io).await.is_ok());
        assert_eq!(io.calls.len(), 1);
        for rows in [
            vec![],
            vec![replace(&healthy, &[("sample_at", Val::Text("2020-01-01T06:00:59Z".into()))])],
            vec![replace(&healthy, &[("avg_instance_cpu_percent", Val::Null)])],
            vec![replace(&healthy, &[("avg_cpu_percent", Val::Null)])],
            vec![replace(&healthy, &[("avg_instance_cpu_percent", Val::Float(f64::NAN))])],
            vec![replace(&healthy, &[("sample_at", Val::Text("2020-01-01T06:02:00.0000001Z".into()))])],
        ] {
            let mut io = FakeRead::new(vec![Ok(rows)]);
            assert!(matches!(
                admit_resources(&mut io).await,
                Err(ReadError::Stopped(CollectionStop::PressureUnavailable))
            ));
        }
        let mut io = FakeRead::new(vec![Ok(vec![replace(
            &healthy,
            &[("avg_instance_cpu_percent", Val::Float(80.0)), ("avg_cpu_percent", Val::Float(0.0))],
        )])]);
        assert!(matches!(admit_resources(&mut io).await, Err(ReadError::Stopped(CollectionStop::Pressure))));
        let mut io = FakeRead::new(vec![Err(ReadError::Stopped(CollectionStop::Cancelled))]);
        assert!(matches!(admit_resources(&mut io).await, Err(ReadError::Stopped(CollectionStop::Cancelled))));
    }
}
