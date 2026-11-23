//! Controlled source facts for canonical reader/controller tests; no native execution or qualification claim.
use super::*;

/// The selected XML/header input was acquired under the same current point, with no positive variable-input type.
/// This exercises the metadata carrier and its real validator, rather than substituting a readiness Boolean.
pub fn current_execution_profile_result_with_input_context(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: ExecutionProfileCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (source, mut rows) = current_execution_profile_result(focus, execution, case);
    if matches!(case, ExecutionProfileCase::NoPlanContext) {
        return (source, rows);
    }
    let TelemetryRecord::Operational(row) = &mut rows[0] else { panic!() };
    let OperationalFact::MssqlCurrentExecutionProfile(point) = &mut row.fact else { panic!() };
    let Some(work) = &point.work else { return (source, rows) };
    let MssqlCurrentRequestPlan::Observed { context, plan: DefinitionPayload::Available { text: plan, .. }, .. } =
        &work.current_plan
    else {
        return (source, rows);
    };
    let DefinitionPayload::Available { text: statement, .. } = &point.statement else { return (source, rows) };
    point.relational_inputs = Some(Box::new(MssqlExecutionRelationalInputs {
        context: context.clone(),
        module_parameters: None,
        statement_utf16_bytes: (statement.encode_utf16().count() * 2).to_string(),
        statement_native_sha256: digest(statement.as_bytes()),
        plan_utf16_bytes: (plan.encode_utf16().count() * 2).to_string(),
        plan_native_sha256: digest(plan.as_bytes()),
        proof: None,
    }));
    assert!(point.fits_source(&source));
    assert!(CurrentExecutionProfileEvidence::from_point(point).unwrap().has_relational_input_context());
    row.key = point.key().unwrap();
    (source, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_input_context_remains_independent_of_type_and_profile_availability() {
        for (case, available) in [
            (ExecutionProfileCase::Observed, true),
            (ExecutionProfileCase::Unavailable, true),
            (ExecutionProfileCase::NoPlanContext, false),
            (ExecutionProfileCase::ChangedRequest, false),
        ] {
            let fixture = current_execution_profile_fixture(case);
            let execution = SourceReadExecutionScope::from_context(
                fixture.focus.question,
                &fixture.focus.original_context(&fixture.basis_source).unwrap(),
            )
            .unwrap();
            let (source, rows) = current_execution_profile_result_with_input_context(&fixture.focus, &execution, case);
            let row = ExportedRecord::new(&source, &fixture.capture.capture, rows.into_iter().next().unwrap()).unwrap();
            let reference =
                DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-input-context-manifest"), &row, 0)
                    .unwrap();
            let answer = CurrentExecutionProfileAnswer::from_originals(
                &fixture.focus,
                &fixture.basis_source,
                &fixture.basis,
                &source,
                &reference.capture_ref(),
                &[(&reference, &row)],
            )
            .unwrap();
            assert_eq!(answer.evidence.has_relational_input_context(), available, "{case:?}");
            assert!(answer.evidence.relational_inputs.as_ref().is_none_or(|inputs| inputs.proof.is_none()));
        }
    }
}
