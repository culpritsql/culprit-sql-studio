use super::*;
use crate::{
    read::integer,
    resources::test_support::replace,
    value::{Rec, Val},
};
use culprit_core::{Engine, telemetry::StatementOrigin};

pub(crate) fn row(wait_capture: bool, capture_policy: bool) -> Rec {
    let layout = crate::telemetry::history_tests::options(3, false);
    let mut columns = layout
        .columns()
        .filter(|(name, _)| !FIELDS.iter().any(|field| field.name == *name))
        .map(|(name, value)| (name, value.clone()))
        .collect::<Vec<_>>();
    columns.extend([
        ("desired_state", Val::Int(2)),
        ("desired_state_desc", Val::Text("READ_WRITE".into())),
        ("actual_state", Val::Int(1)),
        ("actual_state_desc", Val::Text("READ_ONLY".into())),
        ("readonly_reason", Val::Int(65544)),
        ("current_storage_size_mb", Val::Int(9_007_199_254_740_993)),
        ("flush_interval_seconds", Val::Int(900)),
        ("interval_length_minutes", Val::Int(60)),
        ("max_storage_size_mb", Val::Int(2048)),
        ("stale_query_threshold_days", Val::Int(30)),
        ("max_plans_per_query", Val::Int(0)),
        ("query_capture_mode", Val::Int(2)),
        ("query_capture_mode_desc", Val::Text("AUTO".into())),
        ("size_based_cleanup_mode", Val::Int(1)),
        ("size_based_cleanup_mode_desc", Val::Text("AUTO".into())),
        ("wait_stats_capture_mode", if wait_capture { Val::Int(1) } else { Val::Null }),
        ("wait_stats_capture_mode_desc", if wait_capture { Val::Text("ON".into()) } else { Val::Null }),
        ("capture_policy_execution_count", if capture_policy { Val::Int(30) } else { Val::Null }),
        ("capture_policy_total_compile_cpu_time_ms", if capture_policy { Val::Int(1000) } else { Val::Null }),
        ("capture_policy_total_execution_cpu_time_ms", if capture_policy { Val::Int(100) } else { Val::Null }),
        ("capture_policy_stale_threshold_hours", if capture_policy { Val::Int(24) } else { Val::Null }),
    ]);
    replace(&Rec::of(&columns), &[("waits", Val::Int(i64::from(wait_capture)))])
}

#[test]
fn public_optional_cohorts_are_independently_visible_and_absence_is_typed_null() {
    assert_eq!(FIELDS.len(), 21);
    let unique = FIELDS.iter().map(|field| field.name).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), FIELDS.len());
    let batch = query("", "");
    for field in FIELDS {
        if field.availability == Availability::Base {
            continue;
        }
        assert!(
            batch.contains(&format!("COL_LENGTH(N'sys.database_query_store_options',N'{}') IS NOT NULL", field.name))
        );
    }
    for (waits, policy, available) in [(false, false, 15), (true, false, 17), (false, true, 19), (true, true, 21)] {
        let sql = statement("", waits, policy);
        assert_eq!(sql.matches("o.[").count(), available);
        for field in FIELDS {
            let present = match field.availability {
                Availability::Base => true,
                Availability::WaitCapture => waits,
                Availability::CapturePolicy => policy,
            };
            assert_eq!(sql.contains(&format!("o.[{}]", field.name)), present, "{}", field.name);
            assert_eq!(sql.contains(&unavailable(field)), !present, "{}", field.name);
        }
    }
    assert!(!batch.contains("o.*"));
    assert!(!batch.contains("actual_state_additional_info"), "the documented unused field has no setting meaning");
    assert_eq!(batch.matches("EXEC /* culprit */ sys.sp_executesql").count(), 1);
    assert!(!batch.contains("BEGIN CATCH"), "native errors stay source failures without retries or fallback defaults");
}

#[test]
fn each_complete_dynamic_select_has_real_monitoring_provenance_before_literal_escaping() {
    let ordinary = query("", "");
    let native_flags = Rec::of(&[("engine_internal", Val::Int(0)), ("cloud_internal", Val::Int(0))]);
    for waits in [false, true] {
        for policy in [false, true] {
            let inner = statement("", waits, policy);
            assert!(ordinary.contains(&format!("N'{}'", inner.replace('\'', "''"))));
            let provenance = crate::statement_origin::read(&native_flags, Some(&inner));
            assert_eq!(provenance.origin, StatementOrigin::CulpritMonitoring);
            assert!(provenance.template_id.is_some());
            let comments = culprit_core::sql::block_comments(&inner, Engine::Mssql).unwrap().unwrap();
            assert!(comments.contains(&crate::statement_origin::MONITORING_QUERY_MARKER));
        }
    }
    // The outer builder cannot grant inner ownership: quoted markers are not comments in the outer batch.
    assert_eq!(crate::statement_origin::read(&native_flags, Some(&ordinary)).origin, StatementOrigin::Culprit);
}

#[test]
fn all_native_layouts_keep_exact_values_distinct_from_absent_settings_and_layout_facts() {
    for (waits, policy, available) in [(false, false, 15), (true, false, 17), (false, true, 19), (true, true, 21)] {
        let row = row(waits, policy);
        let layout = super::super::layout::NativeLayout::decode(&row).unwrap();
        assert_eq!(layout.waits, waits);
        assert_eq!(layout.edition, 3);
        let values = crate::settings::query_store_options(&row).unwrap();
        let settings =
            values.iter().filter(|value| FIELDS.iter().any(|field| field.name == value.name)).collect::<Vec<_>>();
        assert_eq!(settings.len(), 21);
        assert_eq!(settings.iter().filter(|value| value.value.is_some()).count(), available);
        assert!(settings.iter().all(|value| value.default_value.is_none() && value.native_is_default.is_none()));
        let get = |name| values.iter().find(|value| value.name == name).unwrap();
        assert_eq!(get("current_storage_size_mb").value.as_deref(), Some("9007199254740993"));
        assert_eq!(get("current_storage_size_mb").nature, SettingNature::Measurement);
        assert_eq!(get("readonly_reason").value.as_deref(), Some("65544"));
        assert_eq!(get("readonly_reason").nature, SettingNature::State);
        assert_eq!(get("max_plans_per_query").value.as_deref(), Some("0"));
        assert_eq!(get("max_plans_per_query").nature, SettingNature::Configuration);
        assert_eq!(values.len(), FIELDS.len());
        assert!(values.iter().all(|value| value.nature != SettingNature::Unknown));
        assert!(values.iter().all(|value| !matches!(value.name.as_str(), "engine_edition" | "metadata_visibility")));
        assert_eq!(get("wait_stats_capture_mode").value.is_none(), !waits);
        assert_eq!(get("capture_policy_execution_count").value.is_none(), !policy);
        assert_eq!(integer(&row, "actual_state").unwrap(), 1);
        if !waits {
            assert!(crate::settings::application(get("wait_stats_capture_mode")).is_none());
        }
    }
}

#[test]
fn secondary_capture_and_native_nulls_remain_original_values_without_state_or_default_inference() {
    let row = replace(
        &row(true, true),
        &[
            ("desired_state", Val::Int(4)),
            ("desired_state_desc", Val::Text("READ_CAPTURE_SECONDARY".into())),
            ("actual_state", Val::Int(4)),
            ("actual_state_desc", Val::Text("READ_CAPTURE_SECONDARY".into())),
            ("readonly_reason", Val::Null),
            ("max_storage_size_mb", Val::Null),
        ],
    );
    assert_eq!(
        super::super::layout::NativeLayout::decode(&row).unwrap().mode,
        super::super::mode::ReadMode::ReadCaptureSecondary
    );
    let values = crate::settings::query_store_options(&row).unwrap();
    for name in ["desired_state", "actual_state"] {
        assert_eq!(values.iter().find(|value| value.name == name).unwrap().value.as_deref(), Some("4"));
    }
    for name in ["readonly_reason", "max_storage_size_mb"] {
        let value = values.iter().find(|value| value.name == name).unwrap();
        assert_eq!(value.value, None);
        assert_eq!(value.default_value, None);
        assert!(crate::settings::application(value).is_none());
    }
}
