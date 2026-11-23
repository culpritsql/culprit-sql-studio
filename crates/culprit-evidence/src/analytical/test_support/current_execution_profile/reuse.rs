//! Canonical row constructors over controlled native-shaped originals; no verified handles are made here.
use super::*;

pub fn current_execution_profile_context_reuse_fixture(case: ExecutionProfileCase) -> ExecutionProfileFixture {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let execution = SourceReadExecutionScope::from_context(
        fixture.focus.question,
        &fixture.focus.original_context(&fixture.basis_source).unwrap(),
    )
    .unwrap();
    let (source, rows) = super::current_execution_profile_result_with_input_context(&fixture.focus, &execution, case);
    finish(fixture, source, rows)
}
fn finish(
    mut fixture: ExecutionProfileFixture,
    source: CollectionDescription,
    mut rows: Vec<TelemetryRecord>,
) -> ExecutionProfileFixture {
    let retained = SourceReadFocus::from_followup_input(
        SourceReadQuestion::RelationalInputContext,
        &fixture.episode,
        &fixture.work_source,
        &fixture.work_ref,
        &fixture.work,
    )
    .unwrap()
    .unwrap();
    let row = ExportedRecord::new(&source, &capture("profile-shared-context", "11"), rows.remove(0)).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-profile-shared-context"), &row, 0).unwrap();
    let result = reference.capture_ref();
    let answer = CurrentExecutionProfileAnswer::from_originals(
        &fixture.focus,
        &fixture.basis_source,
        &fixture.basis,
        &source,
        &result,
        &[(&reference, &row)],
    )
    .unwrap();
    let episode = fixture
        .episode
        .enroll_source_read(retained.clone())
        .unwrap()
        .transition_source_read(
            &retained,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::EvidenceUnavailable },
        )
        .unwrap()
        .enroll_source_read(fixture.focus.clone())
        .unwrap()
        .transition_source_read(&fixture.focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &fixture.focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(answer.clone()))),
            },
        )
        .unwrap();
    fixture.source = source;
    fixture.row = row;
    fixture.reference = reference;
    fixture.capture = result;
    fixture.answer = answer;
    fixture.episode = episode;
    fixture
}
#[cfg(test)]
mod tests;
