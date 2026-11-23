//! Finite endpoint selection follows exact native owner/clock/role checks. Ties stay explicitly ambiguous.
use super::*;

pub(super) fn select(conn: &Connection, anchor: &Anchor) -> Result<PointSelection> {
    let Some(database_scope) = scope(&anchor.context)? else {
        return Ok(PointSelection { hints: vec![], issues: vec![SynthesisContextIssue::UnqualifiedPointScope] });
    };
    let mut candidates = heads::select(conn, &database_scope)?;
    validate_scope(&candidates, &database_scope)?;
    if let (CollectionTarget::Database(_), SourcePartition::InstanceLocal { member: Some(member), .. }) =
        (&anchor.context.target, &anchor.context.partition)
    {
        let mut shared = anchor.context.clone();
        shared.target = CollectionTarget::Shared(member.clone());
        let shared_scope = scope(&shared)?.ok_or_else(integrity)?;
        let found = heads::select(conn, &shared_scope)?;
        validate_scope(&found, &shared_scope)?;
        candidates.extend(found);
    }
    candidates.retain(|hint| {
        hint.capture != anchor.capture && dataset(&hint.source).is_some_and(|dataset| anchor.accepts(dataset))
    });
    let mut issues = Vec::new();
    if candidates.is_empty() {
        issues.push(SynthesisContextIssue::PointNotRetainedNearObservation);
    }
    let mut grouped = BTreeMap::<String, Vec<heads::Hint>>::new();
    let mut outside_window = false;
    for hint in candidates {
        let dataset = dataset(&hint.source).ok_or_else(integrity)?;
        let context = native(&hint.source).ok_or_else(integrity)?;
        if !owner_matches(&context, &anchor.context) {
            return Err(integrity());
        }
        if !near(&context, anchor) {
            outside_window = true;
            continue;
        }
        if !role_matches(dataset, &context, &anchor.context) {
            add_issue(&mut issues, SynthesisContextIssue::IndependentPointRole);
            continue;
        }
        if matches!(context.target, CollectionTarget::Database(_))
            && (context.database_writable.is_none() || context.database_writable != anchor.context.database_writable)
        {
            add_issue(&mut issues, SynthesisContextIssue::IndependentPointRole);
        }
        // Separate selected-DB and shared populations even if a reader uses the same native roster.
        let key = crate::synthesis::synthesis_metadata_key(&(&context.target, &hint.family))?;
        grouped.entry(key).or_default().push(hint);
    }
    let mut hints = Vec::new();
    for family in grouped.into_values() {
        if choose(family, anchor, &mut hints)? {
            add_issue(&mut issues, SynthesisContextIssue::AmbiguousPointContext);
        }
    }
    if outside_window {
        add_issue(&mut issues, SynthesisContextIssue::PointOutsideObservationWindow);
    }
    Ok(PointSelection { hints, issues })
}
fn add_issue(issues: &mut Vec<SynthesisContextIssue>, issue: SynthesisContextIssue) {
    if !issues.contains(&issue) {
        issues.push(issue);
    }
}
fn validate_scope(hints: &[heads::Hint], expected: &str) -> Result<()> {
    for hint in hints {
        let context = native(&hint.source).ok_or_else(integrity)?;
        if scope(&context)?.as_deref() != Some(expected) || family(&hint.source)? != hint.family {
            return Err(integrity());
        }
    }
    Ok(())
}
fn choose(hints: Vec<heads::Hint>, anchor: &Anchor, selected: &mut Vec<heads::Hint>) -> Result<bool> {
    let mut before = None;
    let mut after = None;
    let mut overlap = None;
    for (index, hint) in hints.iter().enumerate() {
        let context = native(&hint.source).ok_or_else(integrity)?;
        let from = instant(&context.observed_from).ok_or_else(integrity)?;
        let until = instant(&context.observed_until).ok_or_else(integrity)?;
        if until <= anchor.at {
            update(&mut before, index, until, true);
        } else if from >= anchor.at {
            update(&mut after, index, from, false);
        } else {
            update(&mut overlap, index, until, false);
        }
    }
    let choices = [before, overlap, after];
    let ambiguous = choices.iter().flatten().any(|choice| choice.ambiguous);
    let wanted = choices
        .into_iter()
        .flatten()
        .filter(|choice| !choice.ambiguous)
        .map(|choice| choice.index)
        .collect::<std::collections::BTreeSet<_>>();
    for (index, hint) in hints.into_iter().enumerate() {
        if wanted.contains(&index) {
            selected.push(hint);
        }
    }
    Ok(ambiguous)
}
struct Choice {
    index: usize,
    time: chrono::DateTime<chrono::FixedOffset>,
    ambiguous: bool,
}
fn update(choice: &mut Option<Choice>, index: usize, time: chrono::DateTime<chrono::FixedOffset>, latest: bool) {
    match choice {
        Some(current) if time == current.time => current.ambiguous = true,
        Some(current) if (latest && time > current.time) || (!latest && time < current.time) => {
            *current = Choice { index, time, ambiguous: false };
        }
        Some(_) => (),
        None => *choice = Some(Choice { index, time, ambiguous: false }),
    }
}
