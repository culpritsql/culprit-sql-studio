//! Bind checkout bytes, compiled assets and the actual stored bodies before semantic checks.
use super::*;

pub(super) fn assert_compiled_template() {
    let expected =
        std::env::var("CULPRIT_TEST_KUSTO_FRIENDLY_SHA256").expect("owned supervisor's friendly template digest");
    let actual = ring::digest::digest(&ring::digest::SHA256, include_bytes!("../../kql/friendly.kql"))
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, expected, "the compiled template must match the owned checkout");
    eprintln!("Owned emulator compiled friendly template SHA256: {actual}");
}

pub(super) fn assert_stored_bodies(emulator: &Emulator, commands: &[String]) {
    let mut expected = commands.iter().map(|command| definition(command)).collect::<BTreeMap<_, _>>();
    assert_eq!(expected.len(), commands.len(), "generated function names are unique");
    // This is the same bounded management owner used to install the functions. The documented
    // .show functions result supplies Name/Body; unrelated deployment functions are ignored.
    // https://learn.microsoft.com/en-us/kusto/management/show-function?view=azure-data-explorer
    let reply = emulator.management(true, ".show functions");
    let tables = reply["Tables"].as_array().expect("management tables");
    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    let columns = table["Columns"].as_array().expect("management columns");
    let column =
        |name: &str| columns.iter().position(|value| value["ColumnName"] == name).expect("function metadata column");
    let name = column("Name");
    let body = column("Body");
    for row in table["Rows"].as_array().expect("management rows") {
        let name = row[name].as_str().expect("stored function name");
        if let Some(expected_body) = expected.remove(name) {
            let stored = row[body].as_str().expect("stored function body");
            assert_eq!(body_text(stored), expected_body, "stored body differs from generated {name}");
        }
    }
    assert!(expected.is_empty(), "generated functions not returned by storage: {:?}", expected.keys());
    eprintln!("Owned emulator verified {} generated stored function bodies", commands.len());
}

fn definition(command: &str) -> (String, String) {
    let definition = command.strip_prefix(".create-or-alter function ").expect("generated function command");
    let (name, _) = definition.split_once('(').expect("generated function name");
    let body = &definition[definition.find('{').expect("generated function body")..];
    (name.to_string(), body_text(body))
}

fn body_text(body: &str) -> String {
    // Stored Body includes the enclosing braces. Ignore only their surrounding newlines/space and
    // transport line endings; preserve every token and all interior/string-literal whitespace.
    body.trim()
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .expect("stored function body braces")
        .trim()
        .replace("\r\n", "\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_body_framing_preserves_tokens_and_quoted_whitespace() {
        let command = ".create-or-alter function FriendlySmoke(a:string) {\nprint Name=\"two  spaces\",Value=1\n}";
        let (name, generated) = definition(command);
        assert_eq!(name, "FriendlySmoke");
        assert_eq!(generated, body_text("{\r\nprint Name=\"two  spaces\",Value=1\r\n}"));
        assert_ne!(generated, body_text("{print Name=\"two spaces\",Value=1}"));
        assert_ne!(generated, body_text("{print Name=\"two  spaces\",Value=2}"));
    }
}
