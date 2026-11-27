use super::{DECLARATION, FrozenWindow, MEMBERSHIP, Parameter};

// Bound the optimizer's OR tree for sparse populations. XML still represents every frozen interval exactly.
const MAX_INLINE_RANGES: usize = 24;
const INTERVAL_ID: &str = "i.runtime_stats_interval_id";

pub(in crate::query_store) struct IntervalFilter {
    pub declaration: &'static str,
    pub membership: String,
    pub parameters: Vec<Parameter>,
}

impl FrozenWindow {
    pub(in crate::query_store) fn interval_filter(&self) -> IntervalFilter {
        let mut ids = self.intervals.keys().copied().peekable();
        let mut predicates = Vec::new();
        let mut parameters = Vec::new();
        while let Some(first) = ids.next() {
            if predicates.len() == MAX_INLINE_RANGES {
                return IntervalFilter {
                    declaration: DECLARATION,
                    membership: MEMBERSHIP.into(),
                    parameters: vec![self.parameter()],
                };
            }
            let mut last = first;
            while let Some(next) = ids.next_if(|id| Some(*id) == last.checked_add(1)) {
                last = next;
            }
            parameters.push(Parameter::Integer(first));
            let start = parameters.len();
            predicates.push(if first == last {
                format!("{INTERVAL_ID}=@P{start}")
            } else {
                parameters.push(Parameter::Integer(last));
                format!("{INTERVAL_ID} BETWEEN @P{start} AND @P{}", parameters.len())
            });
        }
        let membership = if predicates.is_empty() { "1=0".into() } else { format!("({})", predicates.join(" OR ")) };
        IntervalFilter { declaration: "", membership, parameters }
    }
}
