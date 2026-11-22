//! Portable plan export options and output. Rendering and file I/O belong to their respective consumers.
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum ExportFormat {
    /// Markdown for a pull request, an issue or a chat.
    #[default]
    Markdown,
    /// One self-contained HTML page with the plan diagram.
    Html,
    /// The plan's own text (.sqlplan, EXPLAIN JSON or text), so it opens in the engine's tools.
    Native,
}

/// Something to say about a plan beyond Culprit's own summary, e.g. a plan insight: what it is, why it matters, how to
/// fix it and which operator it is about. Rendered under the plan in every export format.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ExportNote {
    pub title: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub fix: Option<String>,
    /// Plan node id, to mark the operator in the diagram.
    #[serde(default)]
    pub node: Option<u32>,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ExportOptions {
    /// Replace literal values in SQL text, predicates and parameter values with placeholders. On unless asked.
    #[serde(default = "on")]
    pub redact: bool,
    /// Notes to include with the plan (e.g. insights). The insights engine fills these; exports render them.
    #[serde(default)]
    pub notes: Vec<ExportNote>,
}

fn on() -> bool {
    true
}

impl Default for ExportOptions {
    fn default() -> Self {
        ExportOptions { redact: true, notes: Vec::new() }
    }
}

/// A file to save or copy: a suggested name and the contents.
#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[ts(export)]
pub struct ExportFile {
    pub name: String,
    pub contents: String,
    pub format: ExportFormat,
}
