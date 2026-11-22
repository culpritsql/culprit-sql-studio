//! Reuse the existing SQL/prose sanitizer once at canonical export. Derived SQL is retained for direct reading;
//! it is not a native full-definition payload or executable current-catalog authority.
use super::*;
use crate::{
    Engine,
    redact::{redact_finding, redact_prose},
};

fn sql(payload: &mut DefinitionPayload, engine: Engine) {
    if let DefinitionPayload::Available { text, .. } = payload {
        *payload = DefinitionPayload::sql(text, engine);
    }
}
fn words(value: &mut String, engine: Engine) {
    *value = redact_prose(value, engine);
}
impl DerivedIncident {
    pub fn redact(&mut self, engine: Engine) {
        for value in [
            &mut self.victim_summary,
            &mut self.developer_action,
            &mut self.verification,
            &mut self.entry.title,
            &mut self.entry.phase,
            &mut self.entry.next_step,
        ] {
            words(value, engine);
        }
        if let Some(value) = &mut self.mechanism {
            words(value, engine);
        }
        for evidence in &mut self.entry.evidence {
            words(&mut evidence.label, engine);
            words(&mut evidence.availability, engine);
        }
        for value in &mut self.entry.reasons {
            words(value, engine);
        }
        for query in
            self.affected_queries.iter_mut().chain(&mut self.culprit_queries).chain(&mut self.candidate_queries)
        {
            sql(&mut query.sql, engine);
        }
    }
}
impl DerivedFinding {
    pub fn redact(&mut self, engine: Engine) {
        self.finding = redact_finding(&self.finding, engine);
        if let Some(query) = &mut self.query {
            sql(&mut query.sql, engine);
        }
        if let Some(cause) = &mut self.cause {
            words(&mut cause.summary, engine);
        }
    }
}
impl DerivedChange {
    pub fn redact(&mut self, engine: Engine) {
        self.change.title.redact(|message| redact_event_message(message, engine));
        if let Some(detail) = &mut self.change.detail {
            detail.redact(|message| redact_event_message(message, engine));
        }
    }
}

/// Retain catalog authority while redacting the saved arguments, not just the cached English rendering.
fn redact_event_message(message: &mut crate::i18n::Message, engine: Engine) {
    use crate::i18n::{Message, MessageValue};
    match message {
        Message::Text { text } => words(text, engine),
        Message::Sequence { messages, separator } => {
            words(separator, engine);
            for message in messages {
                redact_event_message(message, engine);
            }
        }
        Message::Catalog { arguments, .. } => {
            for value in arguments.values_mut() {
                match value {
                    MessageValue::Text(text) => words(text, engine),
                    MessageValue::Message(message) => redact_event_message(message, engine),
                    _ => {}
                }
            }
        }
    }
}
