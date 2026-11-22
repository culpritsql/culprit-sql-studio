use culprit_core::Plan;

// Lightweight profiling keeps row counts for past runs, not time. Withdraw its placeholder zeros in both models.
pub fn clear_unrecorded_times(plan: &mut Plan) {
    let facts = plan.facts_mut();
    if facts.elapsed_ms == Some(0.0) {
        facts.elapsed_ms = None;
    }
    if facts.cpu_ms == Some(0.0) {
        facts.cpu_ms = None;
    }
    for key in ["ElapsedMs", "CpuMs"] {
        if plan.props.get(key).is_some_and(|v| v == "0") {
            plan.props.remove(key);
        }
    }
}
