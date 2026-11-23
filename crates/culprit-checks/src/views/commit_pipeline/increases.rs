//! Endpoint quantities are independent of native admission. Transaction and catalog rules stay separate.
use super::*;
mod allocations;
mod transactions;

pub(super) fn append(originals: &[Original<'_>], report: &mut CommitPipelineReport) -> Result<()> {
    if instant(&report.observed_at).is_none() {
        return Ok(());
    }
    transactions::append(originals, report)?;
    allocations::append(originals, report)?;
    if report.increases.len() > MAX_COMMIT_PIPELINE_FACTS {
        report.increases.truncate(MAX_COMMIT_PIPELINE_FACTS);
        report.facts_limited = true;
    }
    Ok(())
}
fn context(original: &Original<'_>, primary: &NativeReadContext) -> Option<NativeReadContext> {
    if !original.read_complete {
        return None;
    }
    let context = original.context().ok()?;
    (context.same_local_database_owner(primary)
        && ObservationBounds::new(&context.observed_from, &context.observed_until).is_some())
    .then_some(context)
}
fn increase(after: &str, before: &str) -> Option<String> {
    let value = after.parse::<u64>().ok()?.checked_sub(before.parse::<u64>().ok()?)?;
    (value > 0).then(|| value.to_string())
}
fn instant(value: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    ObservationBounds::new(value, value)?;
    chrono::DateTime::parse_from_rfc3339(value).ok()
}
fn local_before(before: &str, after: &str) -> bool {
    let parse = |value: &str| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok();
    parse(before).zip(parse(after)).is_some_and(|(before, after)| before < after)
}
