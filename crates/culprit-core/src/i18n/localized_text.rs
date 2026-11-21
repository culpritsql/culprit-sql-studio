//! Rendered saved wording and its authoritative catalog source have one mutation owner.
use super::Message;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct LocalizedText {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    source: Option<Message>,
}
impl LocalizedText {
    /// Native identifiers and user-authored text remain raw even when they resemble a catalog key.
    pub fn raw(text: impl Into<String>) -> Self {
        Self { text: text.into(), source: None }
    }
    pub fn new(message: impl Into<Message>) -> Self {
        let message = message.into();
        Self { text: message.render(), source: message.is_translatable().then_some(message) }
    }
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn source(&self) -> Option<&Message> {
        self.source.as_ref()
    }
    pub fn message(&self) -> Message {
        self.source.clone().unwrap_or_else(|| Message::from(self.text.clone()))
    }
    pub fn set(&mut self, message: impl Into<Message>) {
        *self = Self::new(message);
    }
    /// Explicitly rerender for this reader; deserialization itself preserves the saved bytes and wording.
    pub fn render(&mut self) {
        if let Some(message) = &self.source {
            self.text = message.render();
        }
    }
    /// Storage may omit a cached rendering, while native/user text must remain available verbatim.
    pub fn clear_rendered(&mut self) {
        if self.source.is_some() {
            self.text.clear();
        }
    }
    pub fn clear(&mut self) {
        *self = Self::default();
    }
    /// Redaction transforms the actual source once, then atomically refreshes its rendering. Raw input stays raw.
    pub fn redact(&mut self, redact: impl FnOnce(&mut Message)) {
        let mut message = self.message();
        redact(&mut message);
        self.set(message);
    }
}
impl From<Message> for LocalizedText {
    fn from(message: Message) -> Self {
        Self::new(message)
    }
}
impl From<String> for LocalizedText {
    fn from(text: String) -> Self {
        Self::raw(text)
    }
}
impl From<&str> for LocalizedText {
    fn from(text: &str) -> Self {
        Self::raw(text)
    }
}
impl std::fmt::Display for LocalizedText {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.text.fmt(formatter)
    }
}

impl From<&LocalizedText> for super::Arg {
    fn from(text: &LocalizedText) -> Self {
        text.message().render().into()
    }
}
impl From<LocalizedText> for super::Arg {
    fn from(text: LocalizedText) -> Self {
        Message::from(text).render().into()
    }
}
impl From<&LocalizedText> for Message {
    fn from(text: &LocalizedText) -> Self {
        text.message()
    }
}
impl From<LocalizedText> for Message {
    fn from(text: LocalizedText) -> Self {
        match text.source {
            Some(message) => message,
            None => text.text.into(),
        }
    }
}
impl From<&LocalizedText> for super::MessageValue {
    fn from(text: &LocalizedText) -> Self {
        text.message().into()
    }
}
impl From<LocalizedText> for super::MessageValue {
    fn from(text: LocalizedText) -> Self {
        Message::from(text).into()
    }
}

#[cfg(test)]
mod tests;
