//! Exercise the actual finite roster with a genuine existing question and its original receipts.
use super::*;
use culprit_evidence::analytical::test_support::{ExecutionProfileCase, current_execution_profile_fixture};

#[test]
fn declared_child_refusals_never_remove_the_existing_primary_roster() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    for issue in [
        DiagnosticAnalysisIssue::CostUnqualified,
        DiagnosticAnalysisIssue::Unsupported,
        DiagnosticAnalysisIssue::MissingNativeIdentity,
    ] {
        let mut episode = fixture
            .episode
            .enroll_source_read(fixture.focus.clone())
            .unwrap()
            .transition_source_read(&fixture.focus, SourceReadStageState::Deferred { issue })
            .unwrap();
        episode.analysis = DiagnosticAnalysisStatus::Deferred;
        episode.issue = Some(DiagnosticAnalysisIssue::PolicyChanged);
        assert!(episode.is_valid());
        let stage = episode.source_reads.get(fixture.focus.question).unwrap();
        assert_eq!(
            possible_questions(&episode),
            vec![
                EpisodeQuestion::Primary(RecipeSelector::CurrentRequestWork),
                EpisodeQuestion::Primary(RecipeSelector::PageWaitTasks),
                EpisodeQuestion::Followup(stage),
            ]
        );
        let TelemetryRecord::Activity(original) = &fixture.basis.record else { panic!() };
        let DiagnosticFocusPlan::Ready(primary) =
            episode.plan_focus(&fixture.basis_source, original, &episode.first).unwrap()
        else {
            panic!("one existing native primary question is still eligible")
        };
        assert_eq!(primary.selector(), RecipeSelector::CurrentRequestWork);
        assert!(episode.needs_question(&primary));
        let terminal = episode
            .transition_source_read(
                &fixture.focus,
                SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::Unsupported },
            )
            .unwrap();
        assert_eq!(
            possible_questions(&terminal),
            vec![
                EpisodeQuestion::Primary(RecipeSelector::CurrentRequestWork),
                EpisodeQuestion::Primary(RecipeSelector::PageWaitTasks),
            ]
        );
        assert!(terminal.has_owed_question());
    }
}

#[test]
fn completed_primary_remains_settled_while_the_same_finite_roster_keeps_a_child() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let episode = fixture.episode.enroll_source_read(fixture.focus.clone()).unwrap();
    assert_eq!(episode.analysis, DiagnosticAnalysisStatus::Analyzed);
    let TelemetryRecord::Activity(original) = &fixture.basis.record else { panic!() };
    assert_eq!(
        episode.plan_focus(&fixture.basis_source, original, &episode.first).unwrap(),
        DiagnosticFocusPlan::NotRequired
    );
    let roster = possible_questions(&episode);
    assert_eq!(roster.len(), MAX_PRIMARY_STAGES_PER_EPISODE + 1);
    assert!(matches!(roster.last(), Some(EpisodeQuestion::Followup(stage))
        if stage.focus == fixture.focus));
    assert!(roster.len() <= MAX_PRIMARY_STAGES_PER_EPISODE + SourceReadQuestion::ALL.len());
}
