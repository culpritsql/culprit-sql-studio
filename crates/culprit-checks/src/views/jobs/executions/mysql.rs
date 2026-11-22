use super::*;
pub(super) fn entry(
    row: &OperationalObservation,
    acquisition: &JobAcquisitionReference,
    catalog_time_zone: Option<&str>,
) -> Result<Option<JobExecution>> {
    let OperationalFact::MysqlScheduledEvent(point) = &row.fact else {
        return Ok(None);
    };
    let Some(started) = &point.last_started_without_offset else {
        return Ok(None);
    };
    Ok(Some(JobExecution {
        reference: reference(
            row,
            acquisition,
            JobExecutionIdentity::MysqlLastStart {
                schema: point.schema.clone(),
                name: point.name.clone(),
                created_without_offset: point.created_without_offset.clone(),
                altered_without_offset: point.altered_without_offset.clone(),
                started_without_offset: started.clone(),
            },
        ),
        status: JobExecutionStatus::Unknown,
        label: Some(JobExecutionLabel {
            text: format!("{}.{}", point.schema, point.name),
            origin: JobExecutionLabelOrigin::CurrentDefinition,
        }),
        time: JobExecutionTime::EventLocal {
            started_without_offset: started.clone(),
            event_time_zone: point.time_zone.clone(),
            catalog_time_zone: catalog_time_zone.map(str::to_owned),
        },
        diagnostics: JobExecutionDiagnostics::Unavailable,
        placement: JobExecutionPlacement::NativeLocal,
    }))
}
