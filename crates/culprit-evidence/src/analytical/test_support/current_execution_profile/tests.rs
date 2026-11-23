use super::*;
#[test]
fn exact_adverse_original_and_matched_current_work_nominate_distinct_question_and_keep_original_scope() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    assert_eq!(fixture.focus.question, SourceReadQuestion::CurrentExecutionProfile);
    assert!(fixture.focus.matches_prerequisite_input(
        &fixture.basis_source,
        &fixture.basis,
        &fixture.work_source,
        [(
            &fixture.work_ref,
            match &fixture.work.record {
                TelemetryRecord::Operational(row) => row,
                _ => panic!(),
            }
        )]
    ));
    let episode = fixture
        .episode
        .enroll_source_read(fixture.focus.clone())
        .unwrap()
        .transition_source_read(&fixture.focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &fixture.focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(fixture.answer.clone()))),
            },
        )
        .unwrap();
    assert_eq!(episode.first, fixture.episode.first);
    assert_eq!(episode.report, fixture.episode.report);
    assert!(episode.enroll_source_read(fixture.focus).unwrap().source_reads == episode.source_reads);
    let restored: DiagnosticEpisode = serde_json::from_slice(&serde_json::to_vec(&episode).unwrap()).unwrap();
    assert_eq!(restored, episode);
}
#[test]
fn a_native_parallel_wait_without_an_adverse_original_or_exact_current_work_cannot_enroll() {
    for defect in ["adverse", "report", "scope", "role", "participant", "partial"] {
        let mut fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
        match defect {
            "adverse" => {
                fixture.episode.signal = DiagnosticSignal::WorkerStarvation {
                    key: fixture.episode.signal.activity_key().unwrap().clone(),
                    statement_identity: None,
                };
            }
            "report" => fixture.episode.report = None,
            _ => {
                let SourceObservation::CurrentRequestWork { result, .. } = &mut fixture.work_source.observation else {
                    panic!()
                };
                let request = result.focus.as_mut().unwrap();
                match defect {
                    "scope" => request.partition = SourcePartition::Target,
                    "role" => request.database_writable = None,
                    "participant" => {
                        request.original.key.request.as_mut().unwrap().generation = "2020-01-01T11:59:01.0030000".into()
                    }
                    _ => {
                        let SourceObservation::CurrentRequestWork { snapshot, .. } =
                            &mut fixture.work_source.observation
                        else {
                            panic!()
                        };
                        snapshot.sources[0] = StateSourceCoverage::observed(
                            culprit_core::source_id::MSSQL_CURRENT_REQUEST_WORK.into(),
                            1,
                            false,
                        );
                    }
                }
            }
        }
        let result = SourceReadFocus::from_followup_input(
            SourceReadQuestion::CurrentExecutionProfile,
            &fixture.episode,
            &fixture.work_source,
            &fixture.work_ref,
            &fixture.work,
        );
        assert!(!matches!(result, Ok(Some(_))), "{defect}");
    }
}
#[test]
fn answer_requires_exact_original_scope_capture_ordinal_native_point_and_input_clocks() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    for defect in ["scope", "clock", "ordinal", "body", "missing"] {
        let mut source = fixture.source.clone();
        let mut reference = fixture.reference.clone();
        let mut row = fixture.row.clone();
        match defect {
            "scope" => {
                let SourceObservation::SourceRead { execution, .. } = &mut source.observation else { panic!() };
                let SourcePartition::InstanceLocal { epoch, .. } = &mut execution.partition else { panic!() };
                *epoch = Some("foreign-process".into());
            }
            "clock" => {
                let SourceObservation::SourceRead { snapshot, .. } = &mut source.observation else { panic!() };
                snapshot.observed_from = "2020-01-01T11:59:59Z".into();
            }
            "ordinal" => reference.row_ordinal = 1,
            "body" => {
                let TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlCurrentExecutionProfile(point),
                    ..
                }) = &mut row.record
                else {
                    panic!()
                };
                let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
                rows[0].counters.rows_returned = Some("2".into());
            }
            _ => (),
        }
        let rows = if defect == "missing" { vec![] } else { vec![(&reference, &row)] };
        assert!(
            CurrentExecutionProfileAnswer::from_originals(
                &fixture.focus,
                &fixture.basis_source,
                &fixture.basis,
                &source,
                &fixture.capture,
                &rows
            )
            .is_err(),
            "{defect}"
        );
    }
}

#[test]
fn adverse_serial_or_unknown_exchange_context_uses_same_profile_without_needing_a_parallel_wait() {
    let fixture = current_execution_profile_fixture_without_exchange_wait(ExecutionProfileCase::Observed);
    let SourceReadPrerequisite::ObservedCurrentExecutionRequest { exchange_wait, .. } = &fixture.focus.prerequisite
    else {
        panic!()
    };
    assert_eq!(*exchange_wait, None);
    assert_eq!(fixture.answer.result, fixture.capture);
    assert!(fixture.answer.is_valid());
}
#[test]
fn curated_profile_keeps_exact_counters_and_payload_locations_without_exposing_native_focus_authority() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let mut public = serde_json::json!({"record":fixture.row.record});
    crate::analytical::prepared::prepare(&mut public, &fixture.row.record, &fixture.source.target).unwrap();
    let details = &public["_projection"]["details"];
    assert!(details["context"].get("target").is_none() && details["context"].get("partition").is_none());
    assert!(details["focus"].get("original").is_none() && details["focus"].get("epoch").is_none());
    assert_eq!(details["profiles"]["rows"][1]["counters"]["rows_returned"], "9007199254740993");
    assert!(details["statement"].get("text").is_none());
    assert!(details["work"]["current_plan"]["plan"].get("text").is_none());
    assert!(
        serde_json::from_value::<MssqlCurrentExecutionProfilePoint>(details.clone()).is_err(),
        "public keys never recreate native admission"
    );
    assert!(fixture.row.record.is_valid());
}

#[test]
fn later_authenticated_request_parent_enrolls_without_rebinding_first_or_recursing_stage_validation() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let mut earlier_source = fixture.basis_source.clone();
    let SourceObservation::Activity(header) = &mut earlier_source.observation else { panic!() };
    header.at = "2020-01-01T11:59:59Z".into();
    header.observed_from = header.at.clone();
    header.observed_until = header.at.clone();
    let earlier =
        ExportedRecord::new(&earlier_source, &capture("profile-earlier-original", "9"), fixture.basis.record.clone())
            .unwrap();
    let first =
        DiagnosticEvidenceRef::from_export(&earlier_source, &digest(b"profile-earlier-original"), &earlier, 0).unwrap();
    let mut episode = DiagnosticEpisode::start(
        fixture.episode.scope.clone(),
        fixture.episode.signal.clone(),
        "2020-01-01T11:59:59Z".into(),
        first.clone(),
    )
    .unwrap();
    episode.latest = fixture.focus.basis.as_ref().clone();
    episode.last_seen_at = fixture.episode.last_seen_at.clone();
    episode.analysis = fixture.episode.analysis;
    episode.report = fixture.episode.report.clone();
    assert!(episode.is_valid());
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        &fixture.work_source,
        &fixture.work_ref,
        &fixture.work,
    )
    .unwrap()
    .unwrap();
    assert_eq!(focus.basis, fixture.focus.basis);
    assert_ne!(focus.basis.as_ref(), &first);
    let enrolled = episode.enroll_source_read(focus.clone()).unwrap();
    assert!(enrolled.is_valid());
    assert_eq!(enrolled.first, first);
    let restored: DiagnosticEpisode = serde_json::from_slice(&serde_json::to_vec(&enrolled).unwrap()).unwrap();
    assert_eq!(restored, enrolled);
    for defect in ["source", "member", "epoch", "clock", "key"] {
        let mut changed = focus.clone();
        let SourceReadPrerequisite::ObservedCurrentExecutionRequest { request, .. } = &mut changed.prerequisite else {
            panic!()
        };
        match defect {
            "source" => request.source = "foreign-source".into(),
            "member" => {
                let SourcePartition::InstanceLocal { member, .. } = &mut request.partition else { panic!() };
                member.as_mut().unwrap().native.id = "foreign-member".into();
            }
            "epoch" => request.epoch = "foreign-epoch".into(),
            "clock" => request.original_observed_at = "2020-01-01T12:00:30Z".into(),
            _ => request.original.key.session.generation = "foreign-login".into(),
        }
        assert!(episode.enroll_source_read(changed).is_err(), "{defect}");
    }
}
