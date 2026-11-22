//! Bounded text excerpts for terminal and agent output. Escape controls before applying the display limit.

#[derive(Debug, PartialEq, Eq)]
pub struct TextPreview {
    pub text: String,
    pub truncated: bool,
}

/// Keep at most `limit` escaped characters, followed by an ellipsis when any characters were omitted.
pub fn escaped_preview(text: &str, limit: usize) -> TextPreview {
    let mut characters = text.chars().flat_map(char::escape_debug);
    let mut text = characters.by_ref().take(limit).collect::<String>();
    let truncated = characters.next().is_some();
    if truncated {
        text.push('…');
    }
    TextPreview { text, truncated }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controls_are_visible_and_the_escaped_length_owns_truncation() {
        assert_eq!(escaped_preview("a\nb", 4), TextPreview { text: "a\\nb".into(), truncated: false });
        assert_eq!(escaped_preview("\n\n", 3), TextPreview { text: "\\n\\…".into(), truncated: true });
        let escape = escaped_preview("\u{1b}[31m", 32);
        assert_eq!(escape.text, "\\u{1b}[31m");
        assert!(!escape.truncated);
    }

    #[test]
    fn unicode_is_preserved_at_the_exact_character_boundary() {
        assert_eq!(escaped_preview("é🦀", 2), TextPreview { text: "é🦀".into(), truncated: false });
        assert_eq!(escaped_preview("é🦀x", 2), TextPreview { text: "é🦀…".into(), truncated: true });
    }

    #[test]
    fn zero_characters_distinguishes_empty_from_omitted_text() {
        assert_eq!(escaped_preview("", 0), TextPreview { text: String::new(), truncated: false });
        assert_eq!(escaped_preview("x", 0), TextPreview { text: "…".into(), truncated: true });
    }
}
