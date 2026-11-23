//! Row placement uses exact authenticated header membership, never arrival time, native labels or current identity.
use super::*;

#[derive(Default)]
pub(super) struct ObservationTimes {
    inputs: BTreeMap<String, WindowOriginalTime>,
}
impl ObservationTimes {
    pub fn push(&mut self, original: DiagnosticCaptureRef, native_clock: WindowNativeClock) -> Result<()> {
        let key = original.key()?;
        if self.inputs.insert(key, WindowOriginalTime { original, native_clock }).is_some() {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn single_clock(&self) -> WindowNativeClock {
        self.inputs
            .values()
            .next()
            .filter(|_| self.inputs.len() == 1)
            .map(|input| input.native_clock.clone())
            .unwrap_or(WindowNativeClock::Unknown)
    }
    pub fn finding(
        &self,
        finding: &DerivedFinding,
        read_complete: bool,
        window: &PublishedResourceWindow,
    ) -> Result<WindowRowPlacement> {
        let mut referenced = BTreeSet::new();
        let mut has_rows = false;
        for reference in finding.referenced_evidence() {
            has_rows = true;
            referenced.insert(reference.capture_ref().key()?);
        }
        let inputs = self
            .inputs
            .iter()
            .filter(|(key, _)| !has_rows || referenced.contains(*key))
            .map(|(_, input)| input.clone())
            .collect::<Vec<_>>();
        let mut overlap = false;
        let mut known = !inputs.is_empty();
        for input in &inputs {
            match input.native_clock.overlaps(window)? {
                Some(matches) => overlap |= matches,
                None => known = false,
            }
        }
        let time = if inputs.is_empty() { WindowRowTime::Unknown } else { WindowRowTime::Originals { inputs } };
        Ok(WindowRowPlacement {
            time,
            overlaps_window: known.then_some(overlap),
            read_complete,
            qualification: String::new(),
        })
    }
}

pub(super) fn change(
    change: &DerivedChange,
    read_complete: bool,
    window: &PublishedResourceWindow,
) -> Result<WindowRowPlacement> {
    let event = &change.change;
    let (Some(at), Some(after), Some(until)) =
        (utc(&event.at), optional_utc(event.after.as_deref()), optional_utc(event.until.as_deref()))
    else {
        return Ok(WindowRowPlacement {
            time: WindowRowTime::Unknown,
            overlaps_window: None,
            read_complete,
            qualification: String::new(),
        });
    };
    let from = after.as_ref().unwrap_or(&at);
    let end = until.as_ref().unwrap_or(&at);
    let parse = |value: &str| chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid());
    if parse(from)? > parse(&at)? || parse(end)? < parse(&at)? {
        return Err(invalid());
    }
    let native = WindowNativeClock::Observation { from: from.clone(), until: end.clone() };
    let overlaps_window = native.overlaps(window)?;
    Ok(WindowRowPlacement {
        time: WindowRowTime::RecordedChange { at, after, until },
        overlaps_window,
        read_complete,
        qualification: String::new(),
    })
}
fn utc(value: &str) -> Option<String> {
    let value = chrono::DateTime::parse_from_rfc3339(value).ok()?;
    (value.offset().local_minus_utc() == 0).then(|| value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}
fn optional_utc(value: Option<&str>) -> Option<Option<String>> {
    match value {
        Some(value) => utc(value).map(Some),
        None => Some(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn offset_free_and_non_utc_event_labels_do_not_become_utc_window_placement() {
        assert!(utc("2020-01-01T07:30:00").is_none());
        assert!(utc("2020-01-01T07:30:00+01:00").is_none());
        assert_eq!(utc("2020-01-01T07:30:00Z").as_deref(), Some("2020-01-01T07:30:00.000000000Z"));
    }
}
