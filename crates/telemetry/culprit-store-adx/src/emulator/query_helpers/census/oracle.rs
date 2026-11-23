//! Independent pre-census selection order over explicit synthetic raw rows; no native authority is minted.
use std::collections::{BTreeMap, BTreeSet};

pub(super) type Key = [u8; 6];
#[derive(Clone)]
pub(super) struct Row {
    pub key: Key,
    pub revision: Option<u64>,
    pub original_id: &'static str,
    pub original_at: Option<u32>,
    pub lifecycle: &'static str,
    pub reference: &'static str,
    pub recorded_at: Option<u32>,
    pub analyzer_matches: bool,
    pub rules_match: bool,
    pub analysis: &'static str,
    pub detail: &'static str,
    pub input_complete: bool,
}
impl Default for Row {
    fn default() -> Self {
        Self {
            key: [0; 6],
            revision: Some(1),
            original_id: "revision-1",
            original_at: Some(1),
            lifecycle: "active",
            reference: r#"{"capture":"original"}"#,
            recorded_at: Some(1),
            analyzer_matches: true,
            rules_match: true,
            analysis: "analysis-a",
            detail: "detail-a",
            input_complete: true,
        }
    }
}
#[derive(Debug, Eq, PartialEq)]
pub(super) struct Flags {
    pub selected: bool,
    pub first: bool,
}
pub(super) fn legacy(rows: &[Row]) -> BTreeMap<Key, Flags> {
    let mut episodes = BTreeMap::<Key, Vec<&Row>>::new();
    for row in rows {
        episodes.entry(row.key).or_default().push(row);
    }
    episodes
        .into_iter()
        .map(|(key, rows)| {
            let first = consistent_original(&rows, Some(1));
            (key, Flags { selected: selected_head(&rows), first })
        })
        .collect()
}
fn selected_head(rows: &[&Row]) -> bool {
    let Some(revision) = rows.iter().filter_map(|row| row.revision).max() else {
        return false;
    };
    if !consistent_original(rows, Some(revision)) {
        return false;
    }
    let configured = rows
        .iter()
        .copied()
        .filter(|row| {
            row.revision == Some(revision) && row.analyzer_matches && row.rules_match && row.recorded_at.is_some()
        })
        .collect::<Vec<_>>();
    let Some(latest) = configured.iter().filter_map(|row| row.recorded_at).max() else {
        return false;
    };
    configured
        .iter()
        .filter(|row| row.recorded_at == Some(latest))
        .map(|row| (row.analysis, row.detail, row.input_complete))
        .collect::<BTreeSet<_>>()
        .len()
        == 1
}
fn consistent_original(rows: &[&Row], revision: Option<u64>) -> bool {
    let original = rows.iter().copied().filter(|row| row.revision == revision).collect::<Vec<_>>();
    if original.is_empty() || original.iter().any(|row| !known_original(row)) {
        return false;
    }
    original
        .iter()
        .map(|row| (row.original_id, row.original_at, row.lifecycle, reference_text(row.reference)))
        .collect::<BTreeSet<_>>()
        .len()
        == 1
}
fn known_original(row: &Row) -> bool {
    !row.original_id.is_empty()
        && row.original_at.is_some()
        && matches!(row.lifecycle, "active" | "resolved" | "uncertain")
        && !matches!(reference_text(row.reference).as_str(), "" | "null" | "{}")
}
fn reference_text(input: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(input).unwrap() {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(value) => value,
        value => value.to_string(),
    }
}
fn changed(change: impl FnOnce(&mut Row)) -> Row {
    let mut row = Row::default();
    change(&mut row);
    row
}
pub(super) fn cohorts() -> Vec<Vec<Row>> {
    let variants = [
        Row::default(),
        changed(|row| row.revision = Some(2)),
        changed(|row| row.revision = Some(u64::MAX)),
        changed(|row| row.revision = None),
        changed(|row| row.recorded_at = None),
        changed(|row| row.rules_match = false),
        changed(|row| row.analyzer_matches = false),
        changed(|row| row.original_at = None),
        changed(|row| row.reference = r#"{"capture":"different"}"#),
        changed(|row| row.reference = "null"),
        changed(|row| row.reference = "{}"),
        changed(|row| row.original_id = ""),
        changed(|row| row.original_id = "revision-\"\\,\nvalue"),
        changed(|row| row.lifecycle = "unknown"),
        changed(|row| row.detail = "detail-b"),
        changed(|row| row.input_complete = false),
    ];
    let mut cases = vec![vec![]];
    for (index, left) in variants.iter().enumerate() {
        cases.push(vec![left.clone()]);
        for right in &variants[index..] {
            cases.push(vec![left.clone(), right.clone()]);
        }
    }
    let later = changed(|row| row.recorded_at = Some(2));
    let conflict = changed(|row| {
        row.recorded_at = Some(2);
        row.detail = "detail-b";
    });
    cases.push(vec![Row::default(), later.clone(), conflict.clone()]);
    cases.push(vec![later, conflict]);
    cases.push(vec![
        Row::default(),
        changed(|row| {
            row.revision = Some(2);
            row.rules_match = false;
        }),
    ]);
    cases.push(vec![
        Row::default(),
        changed(|row| {
            row.recorded_at = Some(2);
            row.rules_match = false;
        }),
    ]);
    cases.push(vec![
        Row::default(),
        changed(|row| {
            row.revision = Some(2);
            row.original_at = None;
        }),
    ]);
    for dimension in 0..6 {
        cases.push(vec![Row::default(), changed(|row| row.key[dimension] = 1)]);
    }
    cases
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_order_preserves_first_before_refusing_a_newer_unconfigured_raw_head() {
        let rows = vec![
            Row::default(),
            changed(|row| {
                row.revision = Some(2);
                row.rules_match = false;
            }),
        ];
        assert_eq!(legacy(&rows)[&[0; 6]], Flags { selected: false, first: true });
    }
    #[test]
    fn legacy_order_withholds_conflicting_latest_instead_of_reviving_older_unique_rows() {
        let rows = vec![
            Row::default(),
            changed(|row| row.recorded_at = Some(2)),
            changed(|row| {
                row.recorded_at = Some(2);
                row.detail = "detail-b";
            }),
        ];
        assert_eq!(legacy(&rows)[&[0; 6]], Flags { selected: false, first: true });
    }
    #[test]
    fn all_six_native_episode_dimensions_are_independent() {
        for dimension in 0..6 {
            let rows = vec![Row::default(), changed(|row| row.key[dimension] = 1)];
            let result = legacy(&rows);
            assert_eq!(result.len(), 2);
            assert!(result.values().all(|flags| flags.selected && flags.first));
        }
    }
}
