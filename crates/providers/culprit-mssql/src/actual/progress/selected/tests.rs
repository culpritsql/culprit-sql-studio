use super::*;
use crate::{
    activity::{current_execution_profile::tests as profile, current_request_work::tests as current},
    instance::test_support::{epoch, metadata},
    resources::test_support::replace,
    value::Val,
};

#[test]
fn both_projection_modes_share_native_request_ownership_while_thread_mode_retains_native_rows_and_mars_selection() {
    let nodes = sql::profiles();
    let threads = sql::profiles_for(sql::ProfileProjection::Threads { limit: 16 });
    for predicate in [
        "r.session_id=@P1",
        "r.request_id=@P2",
        "r.database_id=DB_ID() AND r.database_id=@P9",
        "CAST(s.login_time AS datetime2(7))=CONVERT(datetime2(7),@P3,126)",
        "CAST(r.start_time AS datetime2(7))=CONVERT(datetime2(7),@P4,126)",
        "r.sql_handle=CONVERT(varbinary(64),@P5,2)",
        "r.plan_handle=CONVERT(varbinary(64),@P6,2)",
        "r.statement_start_offset=@P7 AND r.statement_end_offset=@P8",
        "THEN 'guard' ELSE 'profile' END AS row_kind",
    ] {
        assert!(nodes.contains(predicate) && threads.contains(predicate), "{predicate}");
    }
    assert!(nodes.contains("SUM(p.row_count)") && nodes.contains("GROUP BY p.node_id"));
    assert!(!threads.contains("SUM(") && !threads.contains("GROUP BY"));
    assert!(
        threads.contains("TOP(17)")
            && threads.contains("p.thread_id")
            && threads.contains("p.task_address")
            && threads.contains("p.actual_read_row_count")
    );
    assert!(threads.contains("ar.database_id=DB_ID() AND ar.request_id=@P2"));
    assert!(
        nodes.contains("ar.database_id=DB_ID()) AS request_count"),
        "ordinary one-session MARS refusal stays strict"
    );
}
#[tokio::test]
async fn held_profile_read_uses_original_nine_bound_parameters_and_final_guard_even_when_empty() {
    let mut io = current::io(vec![
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![current::authority("2020-01-01T06:00:00Z", Some("READ_WRITE"))]),
        Ok(vec![profile::guard()]),
    ]);
    let owner = Context::read(&mut io, "fixture.invalid", 1433).await.unwrap();
    let original = Check::decode(&current::point("2020-01-01T06:00:00.1Z")).unwrap();
    let (_, plan) = original.identities(&original).unwrap();
    let observed = read(&mut io, &owner, &original, &plan.unwrap(), 16).await.unwrap();
    let SelectedProfiles::Observed(observed) = observed else { panic!() };
    assert!(observed.rows.is_empty());
    assert_eq!(io.inner.calls[3].parameters.len(), 9);
    assert_eq!(io.inner.calls[3].limit, 18);
    assert_eq!(io.queries(), 4);
}
#[tokio::test]
async fn missing_guard_native_scope_or_changed_native_request_cannot_hide_behind_empty_profile_rows() {
    for defect in ["guard", "scope", "role", "generation", "clock", "absent"] {
        let original = Check::decode(&current::point("2020-01-01T06:00:00.1Z")).unwrap();
        let changed = match defect {
            "scope" => replace(&profile::guard(), &[("selected_database_id", Val::Int(9))]),
            "role" => replace(&profile::guard(), &[("database_updateability", Val::Text("READ_ONLY".into()))]),
            "generation" => replace(
                &profile::guard(),
                &[("checked_request_generation", Val::Text("2020-01-01T05:59:01.0030000".into()))],
            ),
            "clock" => {
                replace(&profile::guard(), &[("profiles_observed_from", Val::Text("2020-01-01T05:59:59Z".into()))])
            }
            "absent" => {
                replace(&profile::guard(), &[("checked_request_id", Val::Null), ("request_count", Val::Int(0))])
            }
            _ => profile::guard(),
        };
        let mut io = current::io(vec![
            Ok(vec![metadata()]),
            Ok(vec![epoch(100)]),
            Ok(vec![current::authority("2020-01-01T06:00:00Z", Some("READ_WRITE"))]),
        ]);
        let owner = Context::read(&mut io, "fixture.invalid", 1433).await.unwrap();
        let result = decode(&owner, &original, 16, if defect == "guard" { vec![] } else { vec![changed] });
        if matches!(defect, "generation" | "absent") {
            assert!(matches!(result.unwrap(), SelectedProfiles::Unavailable(_)));
        } else {
            assert!(result.is_err(), "{defect}");
        }
    }
}
