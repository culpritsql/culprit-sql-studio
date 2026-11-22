use super::*;
use crate::telemetry::digest;
use crate::{
    JobAcquisitionReference, JobExecutionEvent, JobExecutionIdentity, JobExecutionReference, JobExecutionStatus,
};
fn event(status: JobExecutionStatus, observed_until: &str) -> ChangeEvent {
    let reference = JobExecutionReference {
        acquisition: JobAcquisitionReference {
            target_id: digest(b"target"),
            member_id: Some(digest(b"member")),
            process_generation: Some(digest(b"process")),
            namespace: digest(b"view"),
            observed_from: "2026-10-11T14:00:00Z".into(),
            observed_until: observed_until.into(),
        },
        source: "timescale-job-runs".into(),
        observation_key: digest(b"original row"),
        native: JobExecutionIdentity::TimescaleHistory {
            job_id: "1001".into(),
            run_id: "123".into(),
            started_at: "2026-10-11T12:00:00Z".into(),
        },
    };
    let mut event = ChangeEvent::new(
        EventKind::JobExecution,
        EventSource::Database,
        &reference.key().unwrap(),
        "2026-10-11T12:00:00Z",
        "retained job record",
    );
    event.until = Some("2026-10-11T12:01:00Z".into());
    event.job_execution = Some(JobExecutionEvent { reference, status });
    assert!(event.job_execution_is_valid());
    event
}
#[test]
fn reobserving_a_native_slot_selects_only_a_later_recorded_verdict_and_withholds_equal_clock_conflicts() {
    let first = event(JobExecutionStatus::Unknown, "2026-10-11T14:00:01Z");
    let later = event(JobExecutionStatus::Failed, "2026-10-11T14:00:02Z");
    for records in [vec![first.clone(), later.clone()], vec![later.clone(), first.clone()]] {
        assert_eq!(in_range(records, 0.0, f64::MAX), vec![later.clone()]);
    }
    let conflict = event(JobExecutionStatus::Succeeded, "2026-10-11T14:00:02Z");
    for records in [vec![later.clone(), conflict.clone()], vec![conflict, later.clone()]] {
        assert!(in_range(records, 0.0, f64::MAX).is_empty());
    }
    let mut different_epoch = later.clone();
    let job = different_epoch.job_execution.as_mut().unwrap();
    job.reference.acquisition.process_generation = Some(digest(b"another process"));
    different_epoch.id = format!("job-execution:{}:{}", job.reference.key().unwrap(), different_epoch.at);
    assert_eq!(
        in_range(vec![later, different_epoch], 0.0, f64::MAX).len(),
        2,
        "reader epochs are not deduplicated into a lifetime run identity"
    );
}
#[test]
fn a_later_original_observation_supersedes_an_older_conflict_independently_of_read_order() {
    let records = [
        event(JobExecutionStatus::Failed, "2026-10-11T14:00:01Z"),
        event(JobExecutionStatus::Succeeded, "2026-10-11T14:00:01Z"),
        event(JobExecutionStatus::Succeeded, "2026-10-11T14:00:02Z"),
    ];
    for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        let selected = in_range(order.map(|index| records[index].clone()).to_vec(), 0.0, f64::MAX);
        assert_eq!(selected, vec![records[2].clone()], "{order:?}");
    }
}

#[test]
fn local_history_cannot_impersonate_a_utc_event_or_acquire_current_table_authority() {
    let original = event(JobExecutionStatus::Failed, "2026-10-11T14:00:01Z");
    let mut local = original.clone();
    local.job_execution.as_mut().unwrap().reference.native = JobExecutionIdentity::MssqlAgentHistory {
        job_id: "a".into(),
        history_id: "1".into(),
        step_id: "1".into(),
        run_date: "20261025".into(),
        run_time: "33000".into(),
    };
    assert!(!local.job_execution_is_valid());
    assert!(in_range(vec![local], 0.0, f64::MAX).is_empty());
    let mut named = original.clone();
    named.table = Some("public.orders".into());
    assert!(!named.job_execution_is_valid());
    let mut no_metadata = original;
    no_metadata.job_execution = None;
    assert!(!no_metadata.job_execution_is_valid());
}
#[test]
fn overlap_is_a_weak_context_link_even_for_a_matching_current_procedure() {
    let event = event(JobExecutionStatus::Failed, "2026-10-11T14:00:01Z");
    let query = QueryChange {
        query_id: "q".into(),
        text: "SELECT * FROM public.orders".into(),
        object_name: Some("public.job1001".into()),
        status: ChangeStatus::Regressed,
        baseline: None,
        recent: None,
        ratio: None,
        impact_ms: 0.0,
        recent_share: 0.0,
        plan_switch: None,
        same_plan: false,
        headline: "unchanged headline".into(),
        reason: None,
        context: vec![],
    };
    let (score, relation) =
        link(&event, &query, &["public.orders".into()], None, (event.start(), event.end())).unwrap();
    assert_eq!(score, 0.2);
    assert!(!relation.strong);
    assert_eq!(relation.why, t!("core.events.why.job_execution"));
    assert!(around_then(&[event]).is_some());
}

#[test]
fn saved_catalog_wording_can_change_locale_without_changing_native_slot_status_or_clock() {
    let mut event = event(JobExecutionStatus::Failed, "2026-10-11T14:00:01Z");
    event.title =
        LocalizedText::new(crate::message!("view.jobs.execution.event.title", job = "1001", status = "failed"));
    event.detail = Some(LocalizedText::new(crate::message!("view.jobs.execution.event.detail")));
    let native = event.job_execution.clone();
    let id = event.id.clone();
    let encoded = serde_json::to_vec(&event).unwrap();
    let mut saved: ChangeEvent = serde_json::from_slice(&encoded).unwrap();
    let english = crate::i18n::scope("en", || {
        saved.refresh_language();
        saved.title.text().to_string()
    });
    let french = crate::i18n::scope("fr", || {
        saved.refresh_language();
        saved.title.text().to_string()
    });
    assert_ne!(english, french);
    assert_eq!(saved.job_execution, native);
    assert_eq!(saved.id, id);
    assert!(saved.job_execution_is_valid());
}
