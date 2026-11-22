//! Parameter-selectivity facts from the supplied native plan; catalog enrichment remains provider-owned.
use culprit_core::{PlanNode, t};

/// PostgreSQL/YSQL parameter evidence, independent of heap storage and catalog I/O. The annotation makes no promise
/// that an index exists or can serve the predicate; distributed hash keys need their own lookup evidence.
pub fn parameter_guess(node: &PlanNode) -> Option<String> {
    (node.act_rows.is_none()
        && [node.predicate.as_deref(), node.seek_predicate.as_deref()].into_iter().flatten().any(guessed_selectivity))
    .then(|| t!("provider.postgres.plan.guessed"))
}

/// Operators whose estimate for an unknown value is a fixed default (a third for a range, a fraction of a percent for
/// a pattern), not the column's statistics: `col > $1`, `col ~~ $1` (LIKE), `col ~ $1`.
const DEFAULT_SELECTIVITY: &[&str] =
    &["<", ">", "<=", ">=", "~~", "~~*", "!~~", "!~~*", "~", "~*", "!~", "!~*", "@>", "<@", "&&"];

/// Operators that combine a parameter with another value before any comparison sees it: `now() - $1`, `'%' || $1`.
const COMBINING: &[&str] = &["+", "-", "*", "/", "%", "||"];

/// Whether a filter compares a column with a generic plan's parameter (`$1`) by a range or a pattern, the parameter
/// perhaps inside arithmetic: `created_at > (now() - $1)` compares with a value the planner doesn't know either. Or
/// with every value of an array parameter (`= ANY ($1)`, `<> ALL ($1)`): the planner doesn't know how many it holds and
/// takes 10, so the rows it expects are that guess times a value's.
pub fn guessed_selectivity(filter: &str) -> bool {
    let bytes = filter.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'$' || !bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            i += 1;
            continue;
        }
        let end = i + 1 + bytes[i + 1..].iter().take_while(|b| b.is_ascii_digit()).count();
        // The operators around the parameter and, while those combine it with something (PostgreSQL parenthesizes
        // every such expression), the operators around the parentheses that hold it.
        let (mut start, mut stop) = (i, end);
        loop {
            let around = [operator_before(&filter[..start]), operator_after(&filter[stop..])];
            if around.iter().flatten().any(|op| DEFAULT_SELECTIVITY.contains(&op.as_str()))
                || array_argument(&filter[..start])
            {
                return true;
            }
            if !around.iter().flatten().any(|op| COMBINING.contains(&op.as_str())) {
                break;
            }
            let Some((open, close)) = enclosing(filter, start, stop) else { break };
            (start, stop) = (open, close + 1);
        }
        i = end;
    }
    false
}

/// The parentheses around `filter[start..end]`: the nearest `(` before it that isn't closed before it, and the `)`
/// that closes it.
fn enclosing(filter: &str, start: usize, end: usize) -> Option<(usize, usize)> {
    let bytes = filter.as_bytes();
    let mut depth = 0;
    let open = (0..start).rev().find(|&at| {
        match bytes[at] {
            b')' => depth += 1,
            b'(' if depth == 0 => return true,
            b'(' => depth -= 1,
            _ => {}
        }
        false
    })?;
    let mut depth = 0;
    let close = (end..bytes.len()).find(|&at| {
        match bytes[at] {
            b'(' => depth += 1,
            b')' if depth == 0 => return true,
            b')' => depth -= 1,
            _ => {}
        }
        false
    })?;
    Some((open, close))
}

fn is_operator_char(c: char) -> bool {
    "<>=~!*@&|#%^-+/".contains(c)
}

/// Whether what comes right before a parameter makes it the array of `ANY (…)` or `ALL (…)`: `= ANY (($1)::text[])`.
fn array_argument(before: &str) -> bool {
    let rest = before.trim_end_matches(|c: char| c.is_whitespace() || c == '(');
    let word: Vec<char> = rest.chars().rev().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    let word: String = word.into_iter().rev().collect();
    word.eq_ignore_ascii_case("ANY") || word.eq_ignore_ascii_case("ALL")
}

/// The operator right before a parameter: `created_at > ($1)` → `>`; `= ANY ($1)` → `=`.
fn operator_before(text: &str) -> Option<String> {
    let mut rest = text.trim_end_matches(|c: char| c.is_whitespace() || c == '(');
    for word in ["ANY", "ALL", "any", "all"] {
        if let Some(stripped) = rest.strip_suffix(word) {
            rest = stripped.trim_end_matches(|c: char| c.is_whitespace() || c == '(');
        }
    }
    let op: String =
        rest.chars().rev().take_while(|c| is_operator_char(*c)).collect::<Vec<_>>().into_iter().rev().collect();
    (!op.is_empty()).then_some(op)
}

/// The operator right after a parameter: `($1)::date < created_at` → `<`.
fn operator_after(text: &str) -> Option<String> {
    let mut rest = text.trim_start_matches(|c: char| c.is_whitespace() || c == ')');
    while let Some(cast) = rest.strip_prefix("::") {
        let type_end = cast.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"')).unwrap_or(cast.len());
        rest = cast[type_end..].trim_start_matches(|c: char| c.is_whitespace() || c == ')');
    }
    let op: String = rest.chars().take_while(|c| is_operator_char(*c)).collect();
    (!op.is_empty()).then_some(op)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn which_filters_on_parameters_are_guesses() {
        assert!(guessed_selectivity("(created_at > $1)"));
        assert!(guessed_selectivity("((name)::text ~~ $2)"));
        assert!(guessed_selectivity("(($1)::date <= created_at)"));
        assert!(guessed_selectivity("((status = $1) AND (amount >= $2))"));
        assert!(!guessed_selectivity("(customer_id = $1)"));
        // An array parameter's length is a guess (10 values), whatever the operator.
        assert!(guessed_selectivity("(status = ANY ($1))"));
        assert!(guessed_selectivity("((a.id)::text = ANY (($1)::text[]))"));
        assert!(guessed_selectivity("(id <> ALL ($2))"));
        assert!(!guessed_selectivity("(status = ANY ('{a,b}'::text[]))"));
        assert!(!guessed_selectivity("(company = $1)"));
        assert!(!guessed_selectivity("(status <> $1)"));
        assert!(!guessed_selectivity("(created_at > now())"));
        assert!(!guessed_selectivity("(price > 10)"));
    }
}
