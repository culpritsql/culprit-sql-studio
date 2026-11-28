//! Turns whatever a person pastes (URL, ADO, JDBC, libpq key/value, bare host) into a `ConnectionSpec`.

use culprit_core::{AuthKind, ConnectionSpec, Engine, Error, Result, t};

mod properties;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionSyntax {
    Explicit,
    ServerProperties,
    Inferred,
}

struct ParsedInput {
    syntax: ConnectionSyntax,
    spec: Result<ConnectionSpec>,
}

pub fn parse(input: &str) -> Result<ConnectionSpec> {
    read(input).spec
}

/// What a person pasted after picking the engine: the text's own word wins (a URL's scheme, libpq's key=value pairs, a
/// SQLite file's name), and the pick settles what the text leaves open: a bare host or an unknown port, an ADO.NET
/// string (SqlClient, Npgsql and MySqlConnector all read one), a path whatever its name.
pub fn parse_as(input: &str, picked: Option<Engine>) -> Result<ConnectionSpec> {
    let ParsedInput { syntax, spec } = read(input);
    match (picked, spec) {
        (None, text) => text,
        (Some(Engine::Yugabyte), Ok(spec)) if spec.engine == Some(Engine::Postgres) => {
            Ok(ConnectionSpec { engine: Some(Engine::Yugabyte), ..spec })
        }
        (Some(_), text) if syntax == ConnectionSyntax::Explicit => text,
        (Some(Engine::Sqlite), text) if syntax == ConnectionSyntax::ServerProperties => text,
        (Some(Engine::Sqlite), _) => parse_sqlite(input),
        (Some(engine), text) => text.map(|spec| ConnectionSpec { engine: Some(engine), ..spec }),
    }
}

/// Keep syntax authority even when parsing fails: a named connection or server-property string must never
/// become a literal file path merely because a different engine was selected.
fn read(input: &str) -> ParsedInput {
    let input = input.trim();
    if input.is_empty() {
        return ParsedInput {
            syntax: ConnectionSyntax::Inferred,
            spec: Err(Error::invalid(t!("engine.connstr.empty"))),
        };
    }
    let lower = input.to_ascii_lowercase();
    let explicit = |spec| ParsedInput { syntax: ConnectionSyntax::Explicit, spec };
    if let Some(rest) = strip_any(&lower, input, &["jdbc:sqlite:", "sqlite://", "sqlite:", "file:"]) {
        return explicit(sqlite(rest.strip_prefix("file:").unwrap_or(rest)));
    }
    if is_anchored_path(&lower) {
        return if has_sqlite_extension(&lower) {
            explicit(sqlite(input))
        } else {
            ParsedInput { syntax: ConnectionSyntax::Inferred, spec: parse_bare(input) }
        };
    }
    if let Some(parsed) = read_server_url(input, &lower) {
        return explicit(parsed);
    }
    if input.contains("://") {
        return explicit(Err(Error::invalid(t!("engine.connstr.invalid_url"))));
    }
    if input.contains('=') {
        return read_properties(input);
    }
    if has_sqlite_extension(&lower) {
        return explicit(sqlite(input));
    }
    ParsedInput { syntax: ConnectionSyntax::Inferred, spec: parse_bare(input) }
}

fn read_properties(input: &str) -> ParsedInput {
    if let Ok(spec) = parse_libpq(input) {
        return ParsedInput { syntax: ConnectionSyntax::Explicit, spec: Ok(spec) };
    }
    if let Ok(spec) = parse_ado(input) {
        return ParsedInput { syntax: ConnectionSyntax::ServerProperties, spec: Ok(spec) };
    }
    ParsedInput { syntax: ConnectionSyntax::Explicit, spec: Err(properties::invalid()) }
}

/// A path as a SQLite file whatever its name (`db/production`), when the person said the engine is SQLite.
pub fn parse_sqlite(input: &str) -> Result<ConnectionSpec> {
    let input = input.trim();
    if let Some(path) = input.strip_prefix("file:") {
        return sqlite(path);
    }
    if is_anchored_path(&input.to_ascii_lowercase()) {
        return sqlite(input);
    }
    let ParsedInput { syntax, spec } = read(input);
    if syntax != ConnectionSyntax::Inferred {
        return match spec {
            Ok(spec) if spec.engine == Some(Engine::Sqlite) => Ok(spec),
            _ => Err(Error::invalid(t!("engine.connstr.sqlite_path"))),
        };
    }
    sqlite(input)
}

/// A SQLite file: `sqlite:///abs/app.db`, `sqlite:app.db`, or a path ending in `.db`, `.sqlite` or `.sqlite3`. No host,
/// no sign-in: the path is the database.
fn sqlite(path: &str) -> Result<ConnectionSpec> {
    let path = path.split('?').next().unwrap_or(path).trim();
    if path.is_empty() {
        return Err(Error::invalid(t!("engine.connstr.sqlite_path")));
    }
    Ok(ConnectionSpec {
        engine: Some(Engine::Sqlite),
        database: Some(path.to_string()),
        auth: AuthKind::None,
        ..Default::default()
    })
}

fn is_anchored_path(lower: &str) -> bool {
    lower.starts_with('/')
        || lower.starts_with("./")
        || lower.starts_with("../")
        || lower.starts_with("~/")
        || lower.starts_with("\\\\")
        || matches!(lower.as_bytes(), [drive, b':', b'/' | b'\\', ..] if drive.is_ascii_alphabetic())
}

fn has_sqlite_extension(lower: &str) -> bool {
    [".db", ".sqlite", ".sqlite3", ".db3"].iter().any(|extension| lower.ends_with(extension))
}

#[derive(Clone, Copy)]
enum ServerUrlGrammar {
    Authority(Engine),
    SqlServerJdbc,
}

fn read_server_url(input: &str, lower: &str) -> Option<Result<ConnectionSpec>> {
    use ServerUrlGrammar::{Authority, SqlServerJdbc};
    const SCHEMES: &[(&str, ServerUrlGrammar)] = &[
        ("jdbc:sqlserver:", SqlServerJdbc),
        ("clickhouse:", Authority(Engine::Clickhouse)),
        ("yugabyte:", Authority(Engine::Yugabyte)),
        ("ysql:", Authority(Engine::Yugabyte)),
        ("jdbc:yugabytedb:", Authority(Engine::Yugabyte)),
        ("postgres:", Authority(Engine::Postgres)),
        ("postgresql:", Authority(Engine::Postgres)),
        ("jdbc:postgresql:", Authority(Engine::Postgres)),
        ("mssql:", Authority(Engine::Mssql)),
        ("sqlserver:", Authority(Engine::Mssql)),
        ("mysql:", Authority(Engine::Mysql)),
        ("mariadb:", Authority(Engine::Mysql)),
        ("jdbc:mysql:", Authority(Engine::Mysql)),
        ("jdbc:mariadb:", Authority(Engine::Mysql)),
    ];
    let (prefix, grammar) = SCHEMES.iter().find(|(prefix, _)| lower.starts_with(prefix))?;
    let Some(rest) = input[prefix.len()..].strip_prefix("//") else {
        return Some(Err(Error::invalid(t!("engine.connstr.invalid_url"))));
    };
    let parsed = match grammar {
        Authority(engine) => parse_url(*engine, rest),
        SqlServerJdbc => parse_jdbc_sqlserver(rest),
    };
    // A malformed authority can put userinfo where a port was expected. Never echo that token in an error.
    Some(parsed.map_err(|_| Error::invalid(t!("engine.connstr.invalid_url"))))
}

fn strip_any<'a>(lower: &str, input: &'a str, prefixes: &[&str]) -> Option<&'a str> {
    prefixes.iter().find(|p| lower.starts_with(*p)).map(|p| &input[p.len()..])
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Some(Ok(b)) = value.get(i + 1..i + 3).map(|hex| u8::from_str_radix(hex, 16))
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_url(engine: Engine, rest: &str) -> Result<ConnectionSpec> {
    let (main, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = main.split_once('/').unwrap_or((main, ""));
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let mut spec = ConnectionSpec { engine: Some(engine), ..Default::default() };
    if let Some(userinfo) = userinfo {
        let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
        if !user.is_empty() {
            spec.user = Some(decode(user));
        }
        if !password.is_empty() {
            spec.password = Some(decode(password));
        }
    }
    let (host, port) = split_host_port(hostport, ':')?;
    spec.host = host;
    spec.port = port;
    let database = decode(path.trim_matches('/'));
    if !database.is_empty() {
        spec.database = Some(database);
    }
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, "true"));
        absorb_option(&mut spec, &decode(k), &decode(v));
    }
    finish(spec)
}

fn parse_ado(input: &str) -> Result<ConnectionSpec> {
    let mut spec = ConnectionSpec { engine: Some(Engine::Mssql), ..Default::default() };
    let mut looks_postgres = false;
    for (key, value) in properties::parse(input, properties::Grammar::Ado)? {
        match key.to_ascii_lowercase().as_str() {
            "server" | "data source" | "address" | "addr" | "network address" => {
                let raw = value.trim_start_matches("tcp:");
                let (host, port) = split_host_port(raw, ',')?;
                spec.host = host;
                spec.port = port;
            }
            "host" => {
                looks_postgres = true;
                spec.host = value;
            }
            "port" => spec.port = Some(value.parse().map_err(|_| properties::invalid())?),
            "database" | "initial catalog" => spec.database = Some(value),
            "user id" | "uid" | "user" | "username" => spec.user = Some(value),
            "password" | "pwd" => spec.password = Some(value),
            "dbname" | "hostaddr" => return Err(properties::invalid()),
            _ => absorb_option(&mut spec, key, &value),
        }
    }
    if looks_postgres {
        spec.engine = Some(Engine::Postgres);
    }
    finish_properties(spec)
}

fn parse_jdbc_sqlserver(rest: &str) -> Result<ConnectionSpec> {
    let (hostport, props) = rest.split_once(';').unwrap_or((rest, ""));
    let (host, port) = split_host_port(hostport, ':')?;
    let mut spec = ConnectionSpec { engine: Some(Engine::Mssql), host, port, ..Default::default() };
    for segment in props.split(';') {
        let Some((k, v)) = segment.split_once('=') else {
            continue;
        };
        match k.trim().to_ascii_lowercase().as_str() {
            "databasename" | "database" => spec.database = Some(v.trim().to_string()),
            "user" | "username" => spec.user = Some(v.trim().to_string()),
            "password" => spec.password = Some(v.trim().to_string()),
            _ => absorb_option(&mut spec, k.trim(), v.trim()),
        }
    }
    finish(spec)
}

fn parse_libpq(input: &str) -> Result<ConnectionSpec> {
    let mut spec = ConnectionSpec { engine: Some(Engine::Postgres), ..Default::default() };
    for (key, value) in properties::parse(input, properties::Grammar::Libpq)? {
        if key.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(properties::invalid());
        }
        match key {
            "host" | "hostaddr" => spec.host = value,
            "port" => spec.port = Some(value.parse().map_err(|_| properties::invalid())?),
            "dbname" => spec.database = Some(value),
            "user" => spec.user = Some(value),
            "password" => spec.password = Some(value),
            "server" | "database" | "uid" | "pwd" | "username" => return Err(properties::invalid()),
            _ => absorb_option(&mut spec, key, &value),
        }
    }
    finish_properties(spec)
}

fn finish_properties(spec: ConnectionSpec) -> Result<ConnectionSpec> {
    if spec.host.chars().any(char::is_whitespace) || spec.host.contains(['=', ';', '@']) {
        return Err(properties::invalid());
    }
    finish(spec)
}

fn parse_bare(input: &str) -> Result<ConnectionSpec> {
    let (hostport, database) = input.split_once('/').unwrap_or((input, ""));
    let (host, port) = split_host_port(hostport, if hostport.contains(',') { ',' } else { ':' })?;
    let engine = guess_engine(&host, port);
    let spec = ConnectionSpec {
        engine,
        host,
        port,
        database: (!database.is_empty()).then(|| database.to_string()),
        ..Default::default()
    };
    finish(spec)
}

fn guess_engine(host: &str, port: Option<u16>) -> Option<Engine> {
    let host = host.to_ascii_lowercase();
    if host.ends_with(".database.windows.net") || port == Some(1433) {
        return Some(Engine::Mssql);
    }
    if host.contains(".postgres.") || host.contains("postgres") || port == Some(5432) {
        return Some(Engine::Postgres);
    }
    if host.ends_with(".mysql.database.azure.com") || host.contains("mysql") || port == Some(3306) {
        return Some(Engine::Mysql);
    }
    None
}

fn split_host_port(raw: &str, separator: char) -> Result<(String, Option<u16>)> {
    let raw = raw.trim();
    if let Some(stripped) = raw.strip_prefix('[') {
        let (host, rest) =
            stripped.split_once(']').ok_or_else(|| Error::invalid(t!("engine.connstr.unclosed_ipv6")))?;
        let port = rest.trim_start_matches(separator).parse().ok();
        return Ok((host.to_string(), port));
    }
    match raw.rsplit_once(separator) {
        Some((host, port)) => {
            let port: u16 =
                port.trim().parse().map_err(|_| Error::invalid(t!("engine.connstr.bad_port", port = port)))?;
            Ok((host.trim().to_string(), Some(port)))
        }
        None => Ok((raw.to_string(), None)),
    }
}

fn absorb_option(spec: &mut ConnectionSpec, key: &str, value: &str) {
    let normalized = key.to_ascii_lowercase().replace([' ', '_', '-'], "");
    match normalized.as_str() {
        "authentication" => {
            let v = value.to_ascii_lowercase();
            if v.contains("activedirectory") || v.contains("active directory") || v.contains("entra") {
                spec.auth = AuthKind::Entra;
            }
        }
        "encrypt" => {
            spec.options.insert("encrypt".into(), value.to_string());
        }
        "trustservercertificate" => {
            spec.options.insert("trustServerCertificate".into(), value.to_string());
        }
        "trustservercertificateca" => {
            spec.options.insert("trustServerCertificateCA".into(), value.to_string());
        }
        "hostnameincertificate" => {
            spec.options.insert("hostNameInCertificate".into(), value.to_string());
        }
        "sslmode" => {
            spec.options.insert("sslmode".into(), value.to_string());
        }
        "sslrootcert" | "sslca" => {
            spec.options.insert("sslrootcert".into(), value.to_string());
        }
        "applicationintent" => {
            spec.options.insert("applicationIntent".into(), value.to_string());
        }
        "connecttimeout" | "connectiontimeout" | "logintimeout" => {
            spec.options.insert("connectTimeout".into(), value.to_string());
        }
        _ => {}
    }
}

fn finish(mut spec: ConnectionSpec) -> Result<ConnectionSpec> {
    if spec.host.is_empty() {
        return Err(Error::invalid(t!("engine.connstr.no_host")));
    }
    if spec.engine.is_none() {
        spec.engine = guess_engine(&spec.host, spec.port);
    }
    if spec.host.to_ascii_lowercase().ends_with(".database.windows.net") {
        spec.options.entry("encrypt".into()).or_insert_with(|| "true".into());
    }
    if spec.auth == AuthKind::Password && spec.user.is_none() && spec.password.is_none() && is_azure_sql(&spec.host) {
        spec.auth = AuthKind::Entra;
    }
    Ok(spec)
}

fn is_azure_sql(host: &str) -> bool {
    host.to_ascii_lowercase().ends_with(".database.windows.net")
}

/// A display form with no secrets, used in lists and error messages.
pub fn describe(spec: &ConnectionSpec) -> String {
    if spec.engine == Some(Engine::Sqlite) {
        return spec.database.clone().unwrap_or_default();
    }
    let mut out = String::new();
    if let Some(user) = &spec.user {
        out.push_str(user);
        out.push('@');
    }
    out.push_str(&spec.host);
    if let Some(port) = spec.port {
        out.push(':');
        out.push_str(&port.to_string());
    }
    if let Some(db) = &spec.database {
        out.push('/');
        out.push_str(db);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_property_credentials(input: &str, password: &str) {
        for picked in [
            None,
            Some(Engine::Mssql),
            Some(Engine::Postgres),
            Some(Engine::Mysql),
            Some(Engine::Sqlite),
            Some(Engine::Yugabyte),
        ] {
            let spec = parse_as(input, picked).unwrap();
            assert_eq!(spec.password.as_deref(), Some(password));
            let preview = crate::connection::parse_connection(input, picked, false).unwrap();
            assert!(preview.has_password);
            let shown = serde_json::to_string(&preview).unwrap();
            assert!(!shown.contains("credential_owned_42"));
            assert!(!describe(&spec).contains("credential_owned_42"));
        }
        assert!(parse_sqlite(input).is_err());
    }

    #[test]
    fn complete_property_grammars_keep_semicolons_quotes_and_escapes_in_the_password_owner() {
        for (input, password) in [
            ("host=db.example dbname=shop user=app password=credential_owned_42;tail", "credential_owned_42;tail"),
            ("password=credential_owned_42;tail host=db.example dbname=shop user=app", "credential_owned_42;tail"),
            (
                r"host = db.example password = 'credential_owned_42; space\'quote\\slash' dbname=shop user=app",
                r"credential_owned_42; space'quote\slash",
            ),
            (
                r"host=db.example password=credential_owned_42\ with\ spaces\\slash dbname=shop user=app",
                r"credential_owned_42 with spaces\slash",
            ),
            (
                r#"Server=db.example;Database=shop;User ID=app;Password="credential_owned_42; double""quote\slash"  "#,
                "credential_owned_42; double\"quote\\slash",
            ),
            (
                r"Pwd='credential_owned_42; single''quote\slash';Server=db.example;Database=shop;Uid=app;",
                r"credential_owned_42; single'quote\slash",
            ),
        ] {
            assert_property_credentials(input, password);
        }
    }

    #[test]
    fn property_values_preserve_quoted_names_and_both_native_separator_grammars() {
        let postgres =
            parse("host='db.example' user='app user' dbname='shop;scope password=public_name' password='credential_owned_42' ").unwrap();
        assert_eq!(postgres.engine, Some(Engine::Postgres));
        assert_eq!(postgres.host, "db.example");
        assert_eq!(postgres.user.as_deref(), Some("app user"));
        assert_eq!(postgres.database.as_deref(), Some("shop;scope password=public_name"));
        let ado = parse("Data Source='db.example,1433';Initial Catalog='shop;scope password=public_name';User ID='app user';Password='credential_owned_42';").unwrap();
        assert_eq!(ado.engine, Some(Engine::Mssql));
        assert_eq!(ado.host, "db.example");
        assert_eq!(ado.port, Some(1433));
        assert_eq!(ado.user.as_deref(), postgres.user.as_deref());
        assert_eq!(ado.database.as_deref(), postgres.database.as_deref());
        assert_eq!(ado.password.as_deref(), postgres.password.as_deref());
    }

    #[test]
    fn escaped_libpq_public_values_retain_literal_ownership_without_authorizing_later_raw_assignments() {
        for (value, expected) in [
            (r"my\ data\=store", "my data=store"),
            (r"my\ data=store", "my data=store"),
            ("'my data=store'", "my data=store"),
            (r"my\;data=store", "my;data=store"),
            ("'my;data=store'", "my;data=store"),
        ] {
            let input = format!("host=db dbname={value} user=app password=credential_owned_42");
            let spec = parse(&input).unwrap();
            assert_eq!(spec.database.as_deref(), Some(expected));
            assert_eq!(spec.host, "db");
            assert_eq!(spec.user.as_deref(), Some("app"));
            assert_property_credentials(&input, "credential_owned_42");
        }
    }

    #[test]
    fn malformed_mixed_or_unclosed_properties_refuse_before_any_raw_fragment_can_be_displayed() {
        for input in [
            "Host=db.example user=app password=credential_owned_42;tail",
            "host=db.example; dbname=shop user=app password=credential_owned_42",
            "host=db.example password='credential_owned_42",
            "Server=db.example;Password=\"credential_owned_42\"tail;Database=shop",
            "Server=db.example Password=credential_owned_42",
            "Server=db.example;Database=shop password = credential_owned_42",
            "host=db.example user=app;password=credential_owned_42",
            r"host=db.example user=a\pp;password=credential_owned_42",
            r"host=db.example user=app\x;password=credential_owned_42",
            r"host=db.example dbname=my\ data;password=credential_owned_42",
            "Password=credential_owned_42;host=db.example dbname=shop",
            "host=db.example password=credential_owned_42\\",
            "Server=db.example;Password='credential_owned_42''unterminated",
            "Server=db.example,credential_owned_42;Password=other",
        ] {
            for picked in [
                None,
                Some(Engine::Mssql),
                Some(Engine::Postgres),
                Some(Engine::Mysql),
                Some(Engine::Sqlite),
                Some(Engine::Yugabyte),
            ] {
                let error = crate::connection::parse_connection(input, picked, false).unwrap_err();
                assert_eq!(error.message, t!("engine.connstr.invalid_properties"));
                assert!(!serde_json::to_string(&error).unwrap().contains("credential_owned_42"));
            }
            let error = parse_sqlite(input).unwrap_err();
            assert!(!serde_json::to_string(&error).unwrap().contains("credential_owned_42"));
        }
    }

    #[test]
    fn sqlite_files() {
        let s = parse("sqlite:///Users/me/app.db").unwrap();
        assert_eq!((s.engine, s.database.as_deref()), (Some(Engine::Sqlite), Some("/Users/me/app.db")));
        assert_eq!(parse("./data/app.sqlite3").unwrap().engine, Some(Engine::Sqlite));
        // DBeaver and DataGrip write the JDBC form.
        assert_eq!(parse("jdbc:sqlite:/data/app.db").unwrap().database.as_deref(), Some("/data/app.db"));
        // Any name, when the person says it's SQLite.
        assert_eq!(parse_sqlite("db/production").unwrap().database.as_deref(), Some("db/production"));
        assert_eq!(parse("~/shop.db").unwrap().database.as_deref(), Some("~/shop.db"));
        assert_eq!(describe(&parse("sqlite:app.db").unwrap()), "app.db");
        // A host that happens to end in .db is still a host when it has a port or a scheme.
        assert_ne!(parse("postgres://reports.db:5432/x").unwrap().engine, Some(Engine::Sqlite));
    }

    #[test]
    fn server_properties_keep_credentials_out_of_file_paths_even_with_a_sqlite_pick() {
        for extension in [".db", ".Db", ".db3", ".sqlite", ".sqlite3"] {
            let password = format!("credential_not_a_file{extension}");
            for (input, engine) in [
                (format!("host=server.example port=5432 dbname=shop user=app password={password}"), Engine::Postgres),
                (format!("Server=server.example;Database=shop;User ID=app;Password={password}"), Engine::Mssql),
            ] {
                for picked in [None, Some(Engine::Sqlite)] {
                    let spec = parse_as(&input, picked).unwrap();
                    assert_eq!(spec.engine, Some(engine));
                    assert_eq!(spec.password.as_deref(), Some(password.as_str()));
                    assert!(!describe(&spec).contains(&password));
                    let preview = crate::connection::parse_connection(&input, picked, false).unwrap();
                    assert!(preview.has_password);
                    assert!(!serde_json::to_string(&preview).unwrap().contains(&password));
                }
                assert!(parse_sqlite(&input).is_err(), "the file-only API must reject server properties");
            }
        }
    }

    #[test]
    fn malformed_server_syntax_never_falls_back_to_a_literal_database_file() {
        for input in [
            "postgres://app:credential_not_a_file@server.example:invalid/shop",
            "postgres://app:credential_not_a_file/not-encoded@server.example/shop",
            "postgres:/app:credential_not_a_file@server.example/shop",
            "postgres:app:credential_not_a_file@server.example",
            "jdbc:mysql:/app:credential_not_a_file@server.example/shop",
            "Server=;Password=credential_not_a_file.db",
            "password=credential_not_a_file.db",
            "unknown://app:credential_not_a_file@server.example/shop",
        ] {
            for result in [parse_as(input, None), parse_as(input, Some(Engine::Sqlite)), parse_sqlite(input)] {
                let error = result.unwrap_err();
                assert!(!serde_json::to_string(&error).unwrap().contains("credential_not_a_file"));
            }
        }
        for path in [
            "./data/user=tenant.db",
            "../user=tenant.sqlite",
            "/var/data/user=tenant.db",
            "C:\\data\\user=tenant.db",
            "\\\\server\\files\\user=tenant.db",
        ] {
            let spec = parse(path).unwrap();
            assert_eq!(spec.engine, Some(Engine::Sqlite));
            assert_eq!(spec.database.as_deref(), Some(path));
        }
        for path in
            ["./data/user=tenant", "/var/data/user=tenant", "C:\\data\\user=tenant", "\\\\server\\files\\user=tenant"]
        {
            for input in [path.to_owned(), format!("file:{path}")] {
                for spec in [parse_as(&input, Some(Engine::Sqlite)).unwrap(), parse_sqlite(&input).unwrap()] {
                    assert_eq!(spec.engine, Some(Engine::Sqlite));
                    assert_eq!(spec.database.as_deref(), Some(path));
                }
            }
        }
    }

    #[test]
    fn the_engine_picked_settles_only_what_the_text_leaves_open() {
        // A bare host on a port no engine owns, and an ADO.NET string, which three engines' drivers read.
        assert_eq!(parse_as("db.internal:6000/shop", Some(Engine::Postgres)).unwrap().engine, Some(Engine::Postgres));
        let mysql =
            parse_as("Server=db.internal;Port=3306;Database=shop;Uid=app;Pwd=secret", Some(Engine::Mysql)).unwrap();
        assert_eq!(mysql.engine, Some(Engine::Mysql));
        assert_eq!((mysql.user.as_deref(), mysql.database.as_deref()), (Some("app"), Some("shop")));
        // A guess from the port gives way to the pick.
        assert_eq!(parse_as("db.internal:1433/shop", Some(Engine::Postgres)).unwrap().engine, Some(Engine::Postgres));
        // What names its engine keeps it, whatever was picked.
        assert_eq!(parse_as("postgres://app@h:5432/db", Some(Engine::Mssql)).unwrap().engine, Some(Engine::Postgres));
        assert_eq!(parse_as("host=h dbname=app", Some(Engine::Mysql)).unwrap().engine, Some(Engine::Postgres));
        assert_eq!(parse_as("~/shop.db", Some(Engine::Mssql)).unwrap().engine, Some(Engine::Sqlite));
        assert_eq!(parse_as("mysql://app@h/db", Some(Engine::Sqlite)).unwrap().engine, Some(Engine::Mysql));
        // Picked SQLite, any path is the file.
        for path in ["db/production", "/var/lib/app/data", "C:\\data\\app", "my data/app"] {
            let s = parse_as(path, Some(Engine::Sqlite)).unwrap();
            assert_eq!((s.engine, s.database.as_deref()), (Some(Engine::Sqlite), Some(path)));
        }
        // Nothing picked: as before.
        assert_eq!(parse_as("db.internal:6000/shop", None).unwrap().engine, None);
    }

    #[test]
    fn postgres_url() {
        let s = parse("postgres://app:p%40ss@db.example.com:6432/orders?sslmode=require").unwrap();
        assert_eq!(s.engine, Some(Engine::Postgres));
        assert_eq!(s.host, "db.example.com");
        assert_eq!(s.port, Some(6432));
        assert_eq!(s.user.as_deref(), Some("app"));
        assert_eq!(s.password.as_deref(), Some("p@ss"));
        assert_eq!(s.database.as_deref(), Some("orders"));
        assert_eq!(s.option("sslmode"), Some("require"));
    }

    #[test]
    fn ado_azure_with_entra() {
        let s = parse("Server=tcp:shop-staging.database.windows.net,1433;Initial Catalog=shop;Authentication=Active Directory Default;Encrypt=True").unwrap();
        assert_eq!(s.engine, Some(Engine::Mssql));
        assert_eq!(s.host, "shop-staging.database.windows.net");
        assert_eq!(s.port, Some(1433));
        assert_eq!(s.database.as_deref(), Some("shop"));
        assert_eq!(s.auth, AuthKind::Entra);
        assert_eq!(s.flag("encrypt"), Some(true));
    }

    #[test]
    fn mssql_url_and_jdbc() {
        let s = parse("mssql://sa:secret@127.0.0.1:14333/shop?encrypt=false&trustServerCertificate=true").unwrap();
        assert_eq!(s.engine, Some(Engine::Mssql));
        assert_eq!(s.flag("trustServerCertificate"), Some(true));
        assert_eq!(s.flag("encrypt"), Some(false));
        let j = parse("jdbc:sqlserver://host:1433;databaseName=app;encrypt=true").unwrap();
        assert_eq!(j.database.as_deref(), Some("app"));
        assert_eq!(j.port, Some(1433));
    }

    #[test]
    fn libpq_and_bare() {
        let s = parse("host=localhost port=5432 dbname=app user=me sslmode=disable").unwrap();
        assert_eq!(s.engine, Some(Engine::Postgres));
        assert_eq!(s.database.as_deref(), Some("app"));
        let b = parse("myserver.database.windows.net/shop").unwrap();
        assert_eq!(b.engine, Some(Engine::Mssql));
        assert_eq!(b.auth, AuthKind::Entra);
        assert!(parse("").is_err());
    }

    #[test]
    fn percent_signs_before_multibyte_characters_never_panic() {
        let s = parse("postgres://app:p%aé%20x@h:5432/db").unwrap();
        assert_eq!(s.password.as_deref(), Some("p%aé x"));
    }

    #[test]
    fn mysql_urls() {
        let s = parse("mysql://app:secret@db.example.com:3307/shop?ssl-mode=REQUIRED").unwrap();
        assert_eq!(s.engine, Some(Engine::Mysql));
        assert_eq!(s.port, Some(3307));
        assert_eq!(s.database.as_deref(), Some("shop"));
        assert_eq!(parse("db.example.com:3306/shop").unwrap().engine, Some(Engine::Mysql));
    }

    #[test]
    fn sql_server_certificate_options_survive_pasted_urls() {
        let spec = parse("mssql://reader@db.example/orders?encrypt=strict&trustServerCertificateCA=%2Ftmp%2Fcompany.pem&hostNameInCertificate=db.internal").unwrap();
        assert_eq!(spec.option("trustServerCertificateCA"), Some("/tmp/company.pem"));
        assert_eq!(spec.option("hostNameInCertificate"), Some("db.internal"));
        let security = culprit_core::connection_security(&spec).unwrap();
        assert_eq!(security.settings.ca_certificate.as_deref(), Some("/tmp/company.pem"));
        assert_eq!(security.settings.certificate_hostname.as_deref(), Some("db.internal"));
    }

    #[test]
    fn tls_options_survive_any_spelling() {
        let s = parse("mysql://app@db.example.com/shop?ssl-mode=DISABLED&ssl-ca=/etc/ca.pem").unwrap();
        assert_eq!(s.option("sslmode"), Some("DISABLED"));
        assert_eq!(s.option("sslrootcert"), Some("/etc/ca.pem"));
        let p = parse("postgres://app@db.example.com/shop?sslmode=verify-full&sslrootcert=/etc/ca.pem").unwrap();
        assert_eq!(p.option("sslmode"), Some("verify-full"));
        assert_eq!(p.option("sslrootcert"), Some("/etc/ca.pem"));
    }

    #[test]
    fn describe_hides_password() {
        let s = parse("postgres://app:secret@h:5432/db").unwrap();
        assert_eq!(describe(&s), "app@h:5432/db");
    }

    #[test]
    fn ysql_protocol_does_not_override_the_selected_native_provider() {
        for url in ["yugabyte://app@db:5433/shop", "ysql://app@db:5433/shop", "jdbc:yugabytedb://db:5433/shop"] {
            assert_eq!(parse_as(url, None).unwrap().engine, Some(Engine::Yugabyte));
        }
        for text in
            ["postgres://app@db:5433/shop", "postgresql://app@db/shop", "host=db port=5433 dbname=shop", "db:5433/shop"]
        {
            assert_eq!(parse_as(text, Some(Engine::Yugabyte)).unwrap().engine, Some(Engine::Yugabyte), "{text}");
        }
        assert_eq!(parse_as("mysql://app@db/shop", Some(Engine::Yugabyte)).unwrap().engine, Some(Engine::Mysql));
    }

    // What a person reads when the text isn't a connection, word for word: its messages come from locales/en.json.
    #[test]
    fn a_text_that_is_not_a_connection_says_why_in_english() {
        let said = |input: &str| parse(input).unwrap_err().message;
        assert_eq!(said("  "), "Paste a connection string or a host name");
        assert_eq!(said("sqlite:"), "A SQLite connection needs the database file's path");
        assert_eq!(said("[::1"), "Unclosed IPv6 address");
        assert_eq!(said("db.example:abc"), "'abc' is not a valid port");
        assert_eq!(said("postgres://"), "Use a complete database URL with a supported scheme.");
    }
}
