//! Consume only a closed no-operand locking-hint clause. Hints are syntax, never acquired lock/row evidence.
use super::{Engine, Token, word};
use crate::sql::lexer::literal_fragments;
use std::{collections::BTreeSet, ops::Range};

const MAX_LOCKING_HINTS: usize = 4;
const LOCKING_HINTS: &[&str] = &["READCOMMITTEDLOCK", "ROWLOCK", "UPDLOCK", "XLOCK", "HOLDLOCK", "NOWAIT"];

pub(super) fn read(text: &str, tokens: &[Token<'_>], at: &mut usize) -> Option<Range<usize>> {
    let start = *at;
    let Token::Word(first) = tokens.get(start)? else { return None };
    if !word(tokens.get(start), "WITH") || tokens.get(start + 1) != Some(&Token::Punct('(')) {
        return None;
    }
    *at += 2;
    let mut seen = BTreeSet::new();
    loop {
        let Token::Word(hint) = tokens.get(*at)? else { return None };
        let canonical = LOCKING_HINTS.iter().find(|name| hint.eq_ignore_ascii_case(name))?;
        if !seen.insert(*canonical) || seen.len() > MAX_LOCKING_HINTS {
            return None;
        }
        *at += 1;
        match tokens.get(*at)? {
            Token::Punct(')') => {
                *at += 1;
                break;
            }
            Token::Punct(',') => *at += 1,
            _ => return None,
        }
    }
    if seen.contains("READCOMMITTEDLOCK") && seen.contains("ROWLOCK")
        || seen.contains("UPDLOCK") && seen.contains("XLOCK")
    {
        return None;
    }
    let from = (first.as_ptr() as usize).checked_sub(text.as_ptr() as usize)?;
    let until = tokens[*at..]
        .iter()
        .find_map(|token| match token {
            Token::Word(word) | Token::Quoted(word, _) => (word.as_ptr() as usize).checked_sub(text.as_ptr() as usize),
            Token::Semicolon(offset) => Some(*offset),
            Token::Punct(_) => None,
        })
        .unwrap_or(text.len());
    literal_fragments(text.get(from..until)?, Engine::Mssql)?.is_empty().then_some(start..*at)
}
