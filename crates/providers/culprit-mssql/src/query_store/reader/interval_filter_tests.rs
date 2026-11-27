use super::*;
use crate::{
    query_store::window_sql::{
        self,
        tests::{layout_row, row},
    },
    resources::test_support::{Call, FakeRead, replace},
    value::Val,
};

fn window_with_ids(ids: &[i64]) -> Rec {
    let start = chrono::DateTime::parse_from_rfc3339("2020-01-01T06:00:00Z").unwrap();
    let instant = |seconds: usize| (start + chrono::Duration::seconds(seconds as i64)).to_rfc3339();
    let mut xml = String::from("<intervals>");
    for (second, id) in ids.iter().enumerate() {
        xml.push_str(&format!(r#"<interval id="{id}" starts="{}" ends="{}"/>"#, instant(second), instant(second + 1)));
    }
    xml.push_str("</intervals>");
    let (first, last, hours) = if ids.is_empty() {
        (Val::Null, Val::Null, Val::Null)
    } else {
        (Val::Text(instant(0)), Val::Text(instant(ids.len())), Val::Float(ids.len() as f64 / 3600.0))
    };
    replace(
        &row(),
        &[
            ("native_intervals", Val::Text(xml)),
            ("window_clock", Val::Text(instant(ids.len()))),
            ("window_start", first),
            ("window_end", last),
            ("window_hours", hours),
        ],
    )
}

/// Remove the separately bound plan IDs so each case can check its interval parameters.
async fn selected_waits(metadata: Rec) -> Call {
    let mut reader = Reader::new(FakeRead::new(vec![Ok(vec![layout_row()]), Ok(vec![metadata]), Ok(vec![])]));
    reader.waits(60, Some(&["42".into()])).await.unwrap();
    let mut call = reader.io.calls.pop().unwrap();
    assert!(call.sql.contains(&format!("@ids xml=CONVERT(xml,@P{})", call.parameters.len())));
    assert!(matches!(call.parameters.pop(), Some(Parameter::Text(value)) if value == r#"<ids><id id="42"/></ids>"#));
    call
}

#[tokio::test]
async fn empty_interval_set_matches_no_rows_and_leaves_the_first_parameter_for_plan_ids() {
    let call = selected_waits(window_with_ids(&[])).await;
    assert!(call.sql.contains("WHERE 1=0 AND"));
    assert!(call.parameters.is_empty());
}

#[tokio::test]
async fn out_of_order_ids_bind_only_consecutive_runs_including_the_largest_native_id() {
    let call = selected_waits(window_with_ids(&[i64::MAX, 0, i64::MAX - 1])).await;
    assert!(
        call.sql
            .contains("WHERE (i.runtime_stats_interval_id=@P1 OR i.runtime_stats_interval_id BETWEEN @P2 AND @P3) AND")
    );
    assert!(
        matches!(call.parameters.as_slice(), [Parameter::Integer(0), Parameter::Integer(first), Parameter::Integer(i64::MAX)] if *first == i64::MAX - 1)
    );
}

#[tokio::test]
async fn a_full_contiguous_window_binds_only_its_endpoints() {
    let ids: Vec<i64> = (0..window_sql::MAX_INTERVALS as i64).collect();
    let call = selected_waits(window_with_ids(&ids)).await;
    assert!(call.sql.contains("WHERE (i.runtime_stats_interval_id BETWEEN @P1 AND @P2) AND"));
    assert!(
        matches!(call.parameters.as_slice(), [Parameter::Integer(0), Parameter::Integer(last)] if Some(last) == ids.last())
    );
}

#[tokio::test]
async fn fragmented_windows_switch_to_the_original_xml_only_above_the_range_limit() {
    let sparse: Vec<i64> = (0..25).map(|id| id * 2).collect();
    let inline = selected_waits(window_with_ids(&sparse[..24])).await;
    assert_eq!(inline.parameters.len(), 24);
    for (index, (parameter, id)) in inline.parameters.iter().zip(&sparse).enumerate() {
        assert!(matches!(parameter, Parameter::Integer(value) if value == id));
        assert!(inline.sql.contains(&format!("i.runtime_stats_interval_id=@P{}", index + 1)));
    }
    let metadata = window_with_ids(&sparse);
    let xml = metadata.str("native_intervals").unwrap();
    let fallback = selected_waits(metadata).await;
    assert!(fallback.sql.contains(window_sql::DECLARATION));
    assert!(fallback.sql.contains(window_sql::MEMBERSHIP));
    assert!(matches!(fallback.parameters.as_slice(), [Parameter::Text(value)] if value == &xml));
}
