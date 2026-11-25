//! Maps an engine to its provider. New engines plug in here behind a cargo feature.

use crate::ProviderRegistration;
use culprit_core::{ConnectionSpec, Engine, Error, ErrorCode, Provider, Result, Source, t};
#[cfg(any(feature = "mssql", feature = "postgres", feature = "yugabyte", feature = "mysql", feature = "sqlite"))]
use std::sync::Arc;

/// Every source a scan reads on `engine`, as its provider declares them (`<provider>/src/sources.rs`); empty for an
/// engine this build has no provider for.
pub fn sources(engine: Engine) -> &'static [Source] {
    match engine {
        #[cfg(feature = "mssql")]
        Engine::Mssql => culprit_mssql::sources::ALL,
        #[cfg(feature = "postgres")]
        Engine::Postgres => culprit_postgres::sources::ALL,
        #[cfg(feature = "yugabyte")]
        Engine::Yugabyte => culprit_yugabyte::sources::ALL,
        #[cfg(feature = "mysql")]
        Engine::Mysql => culprit_mysql::sources::ALL,
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => culprit_sqlite::sources::ALL,
        #[allow(unreachable_patterns)]
        _ => &[],
    }
}

pub fn provider(spec: &ConnectionSpec) -> Result<Box<dyn Provider>> {
    registration(engine(spec)?)?.open(spec)
}

pub(crate) fn engine(spec: &ConnectionSpec) -> Result<Engine> {
    spec.engine.ok_or_else(|| {
        Error::invalid(t!("engine.connection.unknown_engine")).with_hint(t!("engine.registry.unknown_engine_hint"))
    })
}

/// Static source declarations and native construction are selected together; no factory runs while listing.
pub fn registration(engine: Engine) -> Result<ProviderRegistration> {
    match engine {
        #[cfg(feature = "mssql")]
        Engine::Mssql => mssql_registration(),
        #[cfg(feature = "postgres")]
        Engine::Postgres => postgres_registration(),
        #[cfg(feature = "yugabyte")]
        Engine::Yugabyte => ProviderRegistration::new(
            Arc::new(|spec, context| {
                Ok(Box::new(culprit_yugabyte::YugabyteProvider::new(spec)?.with_read_context(context)?))
            }),
            crate::ProviderConstructionPolicy::DeferredNativeWork,
            culprit_yugabyte::telemetry_recipes(),
        ),
        #[cfg(feature = "mysql")]
        Engine::Mysql => mysql_registration(),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => ProviderRegistration::new(
            Arc::new(|spec, context| {
                Ok(Box::new(culprit_sqlite::SqliteProvider::new(spec)?.with_read_context(context)?))
            }),
            crate::ProviderConstructionPolicy::DeferredNativeWork,
            culprit_sqlite::telemetry_recipes(),
        ),
        #[allow(unreachable_patterns)]
        other => Err(Error::new(ErrorCode::Unsupported, t!("engine.registry.unsupported", engine = other.label()))),
    }
}

#[cfg(feature = "mysql")]
fn mysql_registration() -> Result<ProviderRegistration> {
    let registration = ProviderRegistration::new(
        Arc::new(|spec, context| Ok(Box::new(culprit_mysql::MysqlProvider::new(spec)?.with_read_context(context)?))),
        crate::ProviderConstructionPolicy::DeferredNativeWork,
        culprit_mysql::telemetry_recipes(),
    )?
    .with_builtin_safety(culprit_mysql::component_version(), Arc::new(culprit_mysql::verify_builtin_safety))?;
    #[cfg(feature = "collector")]
    let registration = registration.with_profile_source(
        culprit_mysql::telemetry_profile_selectors(),
        Arc::new(|provider, request, profile| provider.telemetry_source_for_safety(request, profile)),
    )?;
    Ok(registration)
}

#[cfg(feature = "postgres")]
fn postgres_registration() -> Result<ProviderRegistration> {
    let registration = ProviderRegistration::new(
        Arc::new(|spec, context| {
            Ok(Box::new(culprit_postgres::PostgresProvider::new(spec)?.with_read_context(context)?))
        }),
        crate::ProviderConstructionPolicy::DeferredNativeWork,
        culprit_postgres::telemetry_recipes(),
    )?;
    let registration = registration.with_builtin_safety(
        culprit_postgres::component_version(),
        Arc::new(culprit_postgres::verify_builtin_safety),
    )?;
    #[cfg(feature = "collector")]
    let registration = registration.with_profile_source(
        culprit_postgres::telemetry_profile_selectors(),
        Arc::new(|provider, request, profile| provider.telemetry_source_for_profile(request, profile)),
    )?;
    Ok(registration)
}

#[cfg(feature = "mssql")]
fn profile_intake(registration: ProviderRegistration) -> Result<ProviderRegistration> {
    #[cfg(feature = "collector")]
    let registration = registration.with_profile_source(
        culprit_mssql::telemetry_profile_selectors(),
        Arc::new(|provider, request, profile| provider.telemetry_source_for_profile(request, profile)),
    )?;
    Ok(registration)
}

#[cfg(feature = "mssql")]
fn mssql_registration() -> Result<ProviderRegistration> {
    let registration = ProviderRegistration::new(
        Arc::new(|spec, context| Ok(Box::new(culprit_mssql::MssqlProvider::new(spec)?.with_read_context(context)?))),
        crate::ProviderConstructionPolicy::DeferredNativeWork,
        culprit_mssql::telemetry_recipes(),
    )?
    .with_builtin_safety(culprit_mssql::component_version(), Arc::new(culprit_mssql::verify_builtin_safety))?;
    #[cfg(feature = "collector")]
    let registration = registration.with_native_qualification_issuer(
        Arc::new(|spec, request| culprit_mssql::MssqlProvider::new(spec)?.qualification_source(request)),
        Arc::new(|evidence| culprit_mssql::assess_recipe_qualification(evidence.manifest, evidence.captured.source())),
    );
    profile_intake(registration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_declare_only_the_selected_feature_gated_provider_catalog() {
        for engine in Engine::ALL {
            match registration(engine) {
                Ok(registration) => {
                    assert!(!sources(engine).is_empty());
                    assert!(culprit_core::telemetry::recipe_catalog_is_valid(registration.telemetry_recipes()));
                    #[cfg(feature = "mssql")]
                    if engine == Engine::Mssql {
                        assert_eq!(registration.telemetry_recipes(), culprit_mssql::telemetry_recipes());
                        assert!(!registration.telemetry_recipes().is_empty());
                    }
                    assert!(
                        !registration.telemetry_recipes().is_empty(),
                        "{engine:?} hides implemented native sources"
                    );
                }
                Err(error) => {
                    assert!(sources(engine).is_empty());
                    assert_eq!(error.code, ErrorCode::Unsupported);
                }
            }
        }
    }

    #[test]
    fn every_builtin_registration_uses_its_own_native_dispatch_catalog() {
        type Catalog = fn() -> Vec<culprit_core::telemetry::RecipeDescriptor>;
        let catalogs: &[(Engine, Catalog)] = &[
            #[cfg(feature = "mssql")]
            (Engine::Mssql, culprit_mssql::telemetry_recipes),
            #[cfg(feature = "postgres")]
            (Engine::Postgres, culprit_postgres::telemetry_recipes),
            #[cfg(feature = "yugabyte")]
            (Engine::Yugabyte, culprit_yugabyte::telemetry_recipes),
            #[cfg(feature = "mysql")]
            (Engine::Mysql, culprit_mysql::telemetry_recipes),
            #[cfg(feature = "sqlite")]
            (Engine::Sqlite, culprit_sqlite::telemetry_recipes),
        ];
        for &(engine, catalog) in catalogs {
            assert_eq!(registration(engine).unwrap().telemetry_recipes(), catalog(), "{engine:?}");
        }
    }

    #[cfg(feature = "collector")]
    #[test]
    fn baseline_profile_intake_names_only_provider_owned_guarded_routes_without_approving_costs() {
        use culprit_core::telemetry::*;
        for engine in Engine::ALL {
            let Ok(registration) = registration(engine) else {
                continue;
            };
            let profiles = match engine {
                #[cfg(feature = "postgres")]
                Engine::Postgres => culprit_postgres::telemetry_profile_selectors(),
                #[cfg(feature = "mssql")]
                Engine::Mssql => culprit_mssql::telemetry_profile_selectors(),
                #[cfg(feature = "mysql")]
                Engine::Mysql => culprit_mysql::telemetry_profile_selectors(),
                _ => vec![],
            };
            for recipe in registration.telemetry_recipes() {
                assert!(!matches!(recipe.admission, RecipeAdmission::Measured(_)));
                assert_eq!(registration.supports_profile_source(recipe.selector), profiles.contains(&recipe.selector));
            }
        }
    }

    #[test]
    fn unresolved_forwarding_never_falls_back_to_a_direct_builtin_provider() {
        for engine in Engine::ALL {
            let spec = ConnectionSpec {
                engine: Some(engine),
                host: "db.example.com".into(),
                database: Some("shop".into()),
                user: Some("reader".into()),
                ssh: Some(culprit_core::SshConfig { destination: "bastion".into(), port: None, identity_file: None }),
                ..Default::default()
            };
            assert!(provider(&spec).is_err(), "{engine:?} ignored an unresolved SSH route");
        }
    }

    // Each engine's sources are one list anyone can read (`culprit sources`): every engine this build serves has one,
    // and a source's name, which its notes carry, says which one it was.
    #[test]
    fn every_engine_declares_its_sources_once() {
        for engine in Engine::ALL {
            let list = sources(engine);
            if list.is_empty() {
                let spec =
                    ConnectionSpec { engine: Some(engine), host: "fixture.invalid".into(), ..Default::default() };
                let error = provider(&spec).err().expect("a registered provider must declare its native sources");
                assert_eq!(error.code, ErrorCode::Unsupported, "{engine:?} lacks sources for a selectable provider");
                continue;
            }
            let mut names: Vec<&str> = list.iter().map(|s| s.name).collect();
            names.sort();
            let before = names.len();
            names.dedup();
            assert_eq!(names.len(), before, "{engine:?} names two sources the same");
        }
    }

    // What a person reads, word for word: its messages come from locales/en.json.
    #[test]
    fn a_spec_without_an_engine_says_how_to_name_one_in_english() {
        let Err(e) = provider(&ConnectionSpec::default()) else { panic!("no engine, no provider") };
        assert_eq!(e.message, "Can't tell which database engine this is");
        assert_eq!(e.hint.as_deref(), Some("Use a postgres://, mssql://, mysql:// or sqlite:// URL, or pass --engine"));
    }
}

#[cfg(all(test, feature = "postgres", feature = "collector"))]
mod builtin_safety_tests {
    use super::*;
    use culprit_core::telemetry::*;
    #[test]
    fn selected_registration_not_a_deserialized_policy_admits_only_declared_builtin_pg_readers() {
        let registration = postgres_registration().unwrap();
        for recipe in registration
            .telemetry_recipes()
            .iter()
            .filter(|recipe| matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)))
        {
            let RecipeAdmission::BuiltinSafety(policy) = &recipe.admission else { unreachable!() };
            assert!(registration.verify_builtin_safety(policy).unwrap());
            let mut forged = policy.as_ref().clone();
            forged.templates_sha256 = digest(b"not-the-selected-reader");
            assert!(!registration.verify_builtin_safety(&forged).unwrap());
            if recipe.purpose == RecipePurpose::Baseline {
                let request = CollectionRequest {
                    subject: CollectionSubject::Database,
                    selection: match recipe.selector {
                        RecipeSelector::Collection(TelemetryCollection::Activity) => CollectionSelection::Activity,
                        RecipeSelector::Collection(TelemetryCollection::Snapshot(dataset)) => {
                            CollectionSelection::Snapshot { dataset }
                        }
                        _ => panic!("baseline safety must retain its declared collection selector"),
                    },
                    budget: recipe.budget.clone(),
                };
                let profile = SourceProfileAdmission::for_builtin(
                    digest(b"saved-native-route"),
                    policy.as_ref().clone(),
                    None,
                    request.budget.clone(),
                    || true,
                )
                .unwrap();
                assert!(profile.qualification().is_none());
                registration.validate_profile(&request, &profile).unwrap();
                let untrusted = ProviderRegistration::new(
                    Arc::new(|_, _| panic!("listing/admission never opens a factory")),
                    crate::ProviderConstructionPolicy::DeferredNativeWork,
                    registration.telemetry_recipes().to_vec(),
                )
                .unwrap();
                assert!(!untrusted.verify_builtin_safety(policy).unwrap());
                assert!(untrusted.validate_profile(&request, &profile).is_err());
            }
        }
        #[cfg(feature = "mssql")]
        {
            let mssql = mssql_registration().unwrap();
            let mut admitted = mssql
                .telemetry_recipes()
                .iter()
                .filter(|recipe| matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)))
                .map(|recipe| {
                    let RecipeAdmission::BuiltinSafety(policy) = &recipe.admission else { unreachable!() };
                    assert!(mssql.verify_builtin_safety(policy).unwrap());
                    let mut forged = policy.as_ref().clone();
                    forged.templates_sha256 = digest(b"foreign-mssql-reader");
                    assert!(!mssql.verify_builtin_safety(&forged).unwrap());
                    recipe.id.as_str()
                })
                .collect::<Vec<_>>();
            admitted.sort_unstable();
            assert_eq!(
                admitted,
                ["mssql.activity", "mssql.blocking", "mssql.readable-secondary-recovery", "mssql.settings"]
            );
        }
    }
}

#[cfg(all(test, feature = "mysql", feature = "collector"))]
mod mysql_safety_tests {
    use super::*;
    use culprit_core::telemetry::*;
    #[test]
    fn trusted_mysql_registration_admits_baseline_safety_without_factory_work_or_measured_cost() {
        let registration = mysql_registration().unwrap();
        let recipes = registration.telemetry_recipes();
        let activity = recipes.iter().find(|recipe| recipe.id == "mysql.activity").unwrap();
        let RecipeAdmission::BuiltinSafety(policy) = &activity.admission else { panic!() };
        let request = CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::Activity,
            budget: activity.budget.clone(),
        };
        let profile = SourceProfileAdmission::for_builtin(
            digest(b"saved mysql native route"),
            policy.as_ref().clone(),
            None,
            request.budget.clone(),
            || true,
        )
        .unwrap();
        assert!(profile.qualification().is_none());
        registration.validate_profile(&request, &profile).unwrap();
        let provider = culprit_mysql::MysqlProvider::new(&ConnectionSpec {
            engine: Some(Engine::Mysql),
            host: "fixture.invalid".into(),
            database: Some("shop".into()),
            user: Some("reader".into()),
            ..Default::default()
        })
        .unwrap();
        let source = registration.profile_source(&provider, &request, &profile).unwrap();
        assert_eq!(source.query_count(), 0, "profile construction must not open native work");
        for recipe in recipes.iter().filter(|recipe| matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_))) {
            let RecipeAdmission::BuiltinSafety(policy) = &recipe.admission else { panic!() };
            assert!(registration.verify_builtin_safety(policy).unwrap());
            let mut foreign = policy.as_ref().clone();
            foreign.templates_sha256 = digest(b"foreign-reader");
            assert!(!registration.verify_builtin_safety(&foreign).unwrap());
        }
        let mut admitted = recipes
            .iter()
            .filter(|recipe| matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)))
            .map(|recipe| recipe.id.as_str())
            .collect::<Vec<_>>();
        admitted.sort_unstable();
        assert_eq!(admitted, ["mysql.activity", "mysql.blocking", "mysql.settings"]);
        let untrusted = ProviderRegistration::new(
            Arc::new(|_, _| panic!("metadata listing never calls a factory")),
            crate::ProviderConstructionPolicy::DeferredNativeWork,
            recipes.to_vec(),
        )
        .unwrap();
        assert!(!untrusted.verify_builtin_safety(policy).unwrap());
        assert!(untrusted.validate_profile(&request, &profile).is_err());
    }
}
