use super::*;
fn assessment() -> JobExecutionAssessment {
    JobExecutionAssessment {
        read_complete: true,
        sources: vec![JobExecutionSourceCoverage {
            source: TIMESCALE_JOB_RUNS_SOURCE.into(),
            state: SourceState::Partial,
            complete_scope: false,
            native_rows: 1,
            observed_rows: 1,
            stopped: None,
            error_code: None,
        }],
        run_records: 1,
        step_records: 0,
        error_records: 0,
        last_start_records: 0,
        active_window_records: 0,
        utc_placed_records: 1,
        unplaced_records: 0,
        omitted_entries: 0,
    }
}
#[test]
fn coverage_and_record_categories_are_checked_without_promoting_zero_or_partial_to_complete_history() {
    let partial = assessment();
    assert!(partial.is_valid());
    assert!(!partial.sources[0].complete_scope);
    let mut unavailable = partial.clone();
    unavailable.sources[0].state = SourceState::Unavailable;
    unavailable.sources[0].native_rows = 0;
    unavailable.sources[0].observed_rows = 0;
    unavailable.sources[0].error_code = Some(crate::ErrorCode::PermissionDenied);
    unavailable.run_records = 0;
    unavailable.utc_placed_records = 0;
    assert!(unavailable.is_valid());
    assert_eq!(unavailable.record_count(), Some(0));
    assert_eq!(unavailable.sources[0].state, SourceState::Unavailable);
    for mutation in [0, 1, 2, 3, 4] {
        let mut invalid = partial.clone();
        match mutation {
            0 => invalid.step_records = 1,
            1 => invalid.unplaced_records = 1,
            2 => invalid.sources[0].observed_rows = 0,
            3 => invalid.sources.push(invalid.sources[0].clone()),
            _ => invalid.sources[0].state = SourceState::Unavailable,
        }
        assert!(!invalid.is_valid(), "mutation {mutation}");
    }
    let mut unread = partial;
    unread.read_complete = false;
    unread.run_records = 0;
    unread.utc_placed_records = 0;
    unread.sources[0].observed_rows = 0;
    assert!(unread.is_valid());
}
