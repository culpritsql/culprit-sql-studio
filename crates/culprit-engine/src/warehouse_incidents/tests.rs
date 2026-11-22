use super::*;
use culprit_core::{AuthKind, Engine, Environment};
#[test]
fn saved_route_uses_native_default_port_and_preserves_spelling_without_current_or_last_report_identity() {
    for engine in Engine::ALL {
        let mut info = ConnectionInfo {
            name: "alias".into(),
            engine,
            environment: Environment::Production,
            host: "Saved-Endpoint.Invalid".into(),
            port: None,
            database: Some("ExactDatabase".into()),
            user: Some("not-an-owner".into()),
            auth: AuthKind::None,
            has_secret: false,
            display: "not-a-native-identity".into(),
            last_report: None,
            agent_access: Default::default(),
            query_log: None,
            ssh: None,
            security: None,
        };
        if let Some(port) = engine.spec().default_port() {
            let target = saved_incident_database(&info).unwrap();
            assert_eq!(target.database, "ExactDatabase");
            assert!(target.incarnation.is_none());
            assert_eq!(target, DatabaseIdentity::server(engine, "ExactDatabase", "Saved-Endpoint.Invalid", port, None));
            info.port = Some(54339);
            assert_eq!(
                saved_incident_database(&info).unwrap(),
                DatabaseIdentity::server(engine, "ExactDatabase", "Saved-Endpoint.Invalid", 54339, None)
            );
        } else {
            assert!(saved_incident_database(&info).is_err());
        }
        info.database = None;
        assert!(
            saved_incident_database(&info).is_err(),
            "an implicit current database is never guessed for {engine:?}"
        );
    }
}
