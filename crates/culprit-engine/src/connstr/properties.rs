//! Complete property values retain their native separator, quoting and escaping grammar.
use culprit_core::{Error, Result, t};

#[derive(Clone, Copy)]
pub(super) enum Grammar {
    Ado,
    Libpq,
}

pub(super) fn invalid() -> Error {
    Error::invalid(t!("engine.connstr.invalid_properties"))
}

pub(super) fn parse(mut input: &str, grammar: Grammar) -> Result<Vec<(&str, String)>> {
    let mut properties = Vec::new();
    loop {
        input = input.trim_start();
        if input.is_empty() {
            return Ok(properties);
        }
        if matches!(grammar, Grammar::Ado)
            && let Some(rest) = input.strip_prefix(';')
        {
            input = rest;
            continue;
        }
        let (key, rest) = input.split_once('=').ok_or_else(invalid)?;
        let key = key.trim();
        if !valid_key(key, grammar) {
            return Err(invalid());
        }
        let input_value = rest.trim_start();
        let (value, rest) = value(input_value, grammar)?;
        if !matches!(key.to_ascii_lowercase().as_str(), "password" | "pwd") && value.contains_following_assignment() {
            return Err(invalid());
        }
        properties.push((key, value.text));
        input = separator(rest, grammar)?;
    }
}

fn valid_key(key: &str, grammar: Grammar) -> bool {
    !key.is_empty()
        && key.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'-')
                || matches!(grammar, Grammar::Ado) && byte == b' '
        })
}

/// Only characters actually read as syntax can introduce another property. Quotes and escapes own literals.
#[derive(Default)]
struct DecodedValue {
    text: String,
    syntax: Vec<(usize, char)>,
}
impl DecodedValue {
    fn push_literal(&mut self, character: char) {
        self.text.push(character);
    }
    fn push_unescaped(&mut self, character: char) {
        if character == '=' || character == ';' || character.is_whitespace() {
            self.syntax.push((self.text.len(), character));
        }
        self.text.push(character);
    }
    fn contains_following_assignment(&self) -> bool {
        self.syntax.iter().enumerate().any(|(index, &(offset, character))| {
            if character != '=' {
                return false;
            }
            let prefix = &self.syntax[..index];
            let mut end = offset;
            for &(offset, character) in prefix.iter().rev() {
                if character.is_whitespace() && offset + character.len_utf8() == end {
                    end = offset;
                } else {
                    break;
                }
            }
            prefix
                .iter()
                .rfind(|&&(offset, character)| offset < end && (character.is_whitespace() || character == ';'))
                .is_some_and(|&(offset, character)| {
                    valid_key(&self.text[offset + character.len_utf8()..end], Grammar::Libpq)
                })
        })
    }
}

fn separator(input: &str, grammar: Grammar) -> Result<&str> {
    if input.is_empty() {
        return Ok(input);
    }
    match grammar {
        Grammar::Ado => {
            let rest = input.trim_start();
            if rest.is_empty() { Ok(rest) } else { rest.strip_prefix(';').ok_or_else(invalid) }
        }
        Grammar::Libpq if input.chars().next().is_some_and(char::is_whitespace) => Ok(input),
        Grammar::Libpq => Err(invalid()),
    }
}

fn value(input: &str, grammar: Grammar) -> Result<(DecodedValue, &str)> {
    if let Some(quote) = input.chars().next()
        && (quote == '\'' || quote == '"' && matches!(grammar, Grammar::Ado))
    {
        return quoted(&input[quote.len_utf8()..], quote, grammar);
    }
    if matches!(grammar, Grammar::Ado) {
        let end = input.find(';').unwrap_or(input.len());
        let mut value = DecodedValue::default();
        for character in input[..end].trim_end().chars() {
            value.push_unescaped(character);
        }
        return Ok((value, &input[end..]));
    }
    let mut rest = input;
    let mut value = DecodedValue::default();
    while rest.chars().next().is_some_and(|character| !character.is_whitespace()) {
        let character = take(&mut rest).ok_or_else(invalid)?;
        if character == '\\' {
            value.push_literal(take(&mut rest).ok_or_else(invalid)?);
        } else {
            value.push_unescaped(character);
        }
    }
    Ok((value, rest))
}

fn quoted(mut input: &str, quote: char, grammar: Grammar) -> Result<(DecodedValue, &str)> {
    let mut value = DecodedValue::default();
    while let Some(character) = take(&mut input) {
        if character == quote {
            if matches!(grammar, Grammar::Ado) && input.starts_with(quote) {
                let _ = take(&mut input);
                value.push_literal(quote);
                continue;
            }
            return Ok((value, input));
        }
        value.push_literal(if character == '\\' && matches!(grammar, Grammar::Libpq) {
            take(&mut input).ok_or_else(invalid)?
        } else {
            character
        });
    }
    Err(invalid())
}

fn take(input: &mut &str) -> Option<char> {
    let character = input.chars().next()?;
    *input = &input[character.len_utf8()..];
    Some(character)
}
