//! One saved incident has one shared detail budget across affected, causal and candidate query roles.
//! Omitting presentation detail never changes the original capture, native evidence or established role.
use super::*;

pub(super) fn limit_details(
    affected: &mut Vec<DerivedIncidentQuery>,
    culprits: &mut Vec<DerivedIncidentQuery>,
    candidates: &mut Vec<DerivedIncidentQuery>,
) -> Result<Option<String>> {
    if affected.iter().chain(culprits.iter()).chain(candidates.iter()).any(|query| !query.is_valid()) {
        return Err(invalid());
    }
    *affected = incidents::distinct(std::mem::take(affected))?;
    *culprits = incidents::distinct(std::mem::take(culprits))?;
    *candidates = incidents::distinct(std::mem::take(candidates))?;
    let before = [affected.len(), culprits.len(), candidates.len()];
    if affected.len() + culprits.len() + candidates.len() > MAX_DERIVED_QUERIES {
        order_by_identity(affected)?;
        order_by_identity(culprits)?;
        order_by_identity(candidates)?;
    }
    // A large affected roster cannot erase every established owner or the only supported candidate.
    let supporting_role = usize::from(!culprits.is_empty() || !candidates.is_empty());
    affected.truncate(MAX_DERIVED_QUERIES - supporting_role);
    let remaining = MAX_DERIVED_QUERIES - affected.len();
    culprits.truncate(remaining);
    candidates.truncate(remaining - culprits.len());
    let omitted = [before[0] - affected.len(), before[1] - culprits.len(), before[2] - candidates.len()];
    if omitted.iter().all(|count| *count == 0) {
        return Ok(None);
    }
    Ok(Some(t!(
        "view.synthesis.limit.query_details",
        affected = format::digits(omitted[0].to_string()),
        culprits = format::digits(omitted[1].to_string()),
        candidates = format::digits(omitted[2].to_string())
    )))
}

fn order_by_identity(queries: &mut Vec<DerivedIncidentQuery>) -> Result<()> {
    let mut ordered = std::mem::take(queries)
        .into_iter()
        .map(|query| identity_key(&query).map(|key| (key, query)))
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    *queries = ordered.into_iter().map(|(_, query)| query).collect();
    Ok(())
}

/// Native identity/role and exact original references select details. SQL, caller labels and rendered wording
/// never order them. A shared activity's optional database name is a display label, not its native identity.
fn identity_key(query: &DerivedIncidentQuery) -> Result<Vec<u8>> {
    let mut identity = query.identity.clone();
    if let Some(DerivedQueryIdentity::SharedActivity { database, .. }) = &mut identity {
        *database = None;
    }
    let role = query.causal_role.as_ref().map(|role| &role.kind);
    let mut evidence = query.evidence.iter().map(DiagnosticEvidenceRef::key).collect::<Result<Vec<_>>>()?;
    evidence.sort();
    serde_json::to_vec(&(identity, role, evidence)).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(id: usize, cause: bool) -> DerivedIncidentQuery {
        let (_, output) = culprit_evidence::analytical::test_support::derived_diagnosis_output(false, false);
        let mut query = output.incidents[0].affected_queries[0].clone();
        query.identity = Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
            id: format!("observed-actor-{id}"),
            generation: Some("observed-generation".into()),
        }));
        query.sql = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
        query.causal_role = cause.then(|| DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner));
        assert!(query.is_valid());
        query
    }

    #[test]
    fn one_shared_budget_keeps_affected_and_native_causal_roles_before_concurrent_candidates() {
        let mut affected = vec![query(0, false)];
        let mut culprits = (1..5).map(|id| query(id, true)).collect::<Vec<_>>();
        let mut candidates = (5..50).map(|id| query(id, false)).collect::<Vec<_>>();
        let original_affected = affected.clone();
        let original_culprits = culprits.clone();
        let reason = limit_details(&mut affected, &mut culprits, &mut candidates).unwrap().unwrap();
        assert_eq!(affected, original_affected);
        assert!(original_culprits.iter().all(|query| culprits.contains(query)));
        assert_eq!(affected.len() + culprits.len() + candidates.len(), MAX_DERIVED_QUERIES);
        assert!(reason.contains("18"));
        assert!(affected.iter().chain(&candidates).all(|query| query.causal_role.is_none()));
    }

    #[test]
    fn a_large_victim_roster_keeps_one_supported_owner_or_candidate_instead_of_losing_attribution() {
        for established in [false, true] {
            let mut affected = (0..40).map(|id| query(id, false)).collect::<Vec<_>>();
            let mut culprits = if established { vec![query(100, true)] } else { vec![] };
            let mut candidates = vec![query(101, false)];
            assert!(limit_details(&mut affected, &mut culprits, &mut candidates).unwrap().is_some());
            assert_eq!(affected.len(), MAX_DERIVED_QUERIES - 1);
            assert_eq!(culprits.len(), usize::from(established));
            assert_eq!(candidates.len(), usize::from(!established));
        }
    }

    #[test]
    fn ordinary_output_and_exact_duplicates_keep_the_same_rows_without_an_omission_claim() {
        let mut affected = vec![query(0, false)];
        let mut culprits = vec![query(1, true)];
        let mut candidates = vec![query(2, false); 100];
        let expected = (affected.clone(), culprits.clone(), vec![candidates[0].clone()]);
        assert_eq!(limit_details(&mut affected, &mut culprits, &mut candidates).unwrap(), None);
        assert_eq!((affected, culprits, candidates), expected);
    }

    #[test]
    fn overflow_selection_is_independent_of_roster_order_and_localized_causal_wording() {
        let affected = vec![query(0, false)];
        let culprits = vec![query(100, true)];
        let candidates = (1..45).map(|id| query(id, false)).collect::<Vec<_>>();
        let mut first = (affected.clone(), culprits.clone(), candidates.clone());
        let mut second = (affected, culprits, candidates);
        second.2.reverse();
        second.1[0].causal_role.as_mut().unwrap().explanation = LocalizedText::raw("Localized role wording");
        limit_details(&mut first.0, &mut first.1, &mut first.2).unwrap();
        limit_details(&mut second.0, &mut second.1, &mut second.2).unwrap();
        assert_eq!(first.2, second.2);
        assert_eq!(first.1[0].identity, second.1[0].identity);
        assert_eq!(first.1[0].evidence, second.1[0].evidence);
        assert_eq!(first.1.len(), 1, "late supported cause is retained before concurrent candidates");
    }

    #[test]
    fn an_invalid_unselected_detail_is_still_refused_before_truncation() {
        let mut affected = vec![query(0, false)];
        let mut candidates = (1..45).map(|id| query(id, false)).collect::<Vec<_>>();
        candidates.last_mut().unwrap().evidence.clear();
        assert!(limit_details(&mut affected, &mut vec![], &mut candidates).is_err());
    }
}
