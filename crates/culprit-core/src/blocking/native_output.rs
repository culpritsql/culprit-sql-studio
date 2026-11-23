//! Native owner context at one dependency point. Request population is never inferred from retained row count.
use super::*;
use crate::telemetry::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct NativeBlockingOwnerContext {
    pub owner: ActivityWaitOwner,
    pub actor: Option<ActivityKey>,
    pub statement: Option<NativeIdentity>,
    pub native_wait: Option<String>,
    pub wait_ms: Option<String>,
    /// All native current requests of this exact session generation, across databases. None stays unknown.
    pub current_requests: Option<String>,
    pub complete_request_population: bool,
}
impl NativeBlockingOwnerContext {
    pub fn is_valid(&self) -> bool {
        self.owner.is_valid()
            && self
                .actor
                .as_ref()
                .is_none_or(|actor| actor.is_valid() && self.owner.session().as_ref() == Some(&actor.session))
            && self.statement.as_ref().is_none_or(|statement| {
                statement.is_valid() && self.actor.as_ref().is_some_and(|actor| actor.request.is_some())
            })
            && self.native_wait.as_deref().is_none_or(|native| {
                !native.is_empty()
                    && native.len() <= 256
                    && !native.chars().any(char::is_control)
                    && self.actor.is_some()
            })
            && self.wait_ms.as_deref().is_none_or(|value| native_count(value) && self.native_wait.is_some())
            && self.current_requests.as_deref().is_none_or(native_count)
            && (!self.complete_request_population || self.current_requests.is_some())
            && self.current_requests.as_deref().is_none_or(|count| match count {
                "0" => self.actor.as_ref().is_none_or(|actor| actor.request.is_none()) && self.statement.is_none(),
                "1" => true,
                _ => {
                    self.actor.is_none()
                        && self.statement.is_none()
                        && self.native_wait.is_none()
                        && self.wait_ms.is_none()
                }
            })
    }
    pub fn matches_row(&self, row: &ActivityObservation) -> bool {
        self.is_valid()
            && self.actor.as_ref() == Some(&row.key)
            && self.statement.is_some()
            && self.statement == row.statement_identity
            && row.has_session_generation_authority()
    }
    pub fn has_single_current_request(&self) -> bool {
        self.complete_request_population
            && self.current_requests.as_deref() == Some("1")
            && self.actor.as_ref().is_some_and(|actor| actor.request.is_some())
    }
}
fn native_count(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|number| number.to_string() == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn point() -> NativeBlockingOwnerContext {
        let session = NativeActivityIdentity { id: "55".into(), generation: "native-login".into() };
        NativeBlockingOwnerContext {
            owner: ActivityWaitOwner::Session { id: session.id.clone(), generation: Some(session.generation.clone()) },
            actor: Some(ActivityKey {
                session,
                request: Some(NativeActivityIdentity { id: "0".into(), generation: "native-request".into() }),
                transaction: None,
            }),
            statement: Some(NativeIdentity::new("observed-statement")),
            native_wait: Some("ASYNC_NETWORK_IO".into()),
            wait_ms: Some("37".into()),
            current_requests: Some("1".into()),
            complete_request_population: true,
        }
    }
    #[test]
    fn single_request_requires_positive_complete_native_population_and_preserves_unknown_or_multiple() {
        let original = point();
        assert!(original.is_valid() && original.has_single_current_request());
        let mut unknown = original.clone();
        unknown.current_requests = None;
        unknown.complete_request_population = false;
        assert!(unknown.is_valid());
        assert!(!unknown.has_single_current_request());
        let mut multiple = original.clone();
        multiple.current_requests = Some("2".into());
        assert!(!multiple.is_valid(), "no selected request may represent a multiple-request owner");
        multiple.actor = None;
        multiple.statement = None;
        multiple.native_wait = None;
        multiple.wait_ms = None;
        assert!(multiple.is_valid());
        assert!(!multiple.has_single_current_request());
        let mut mismatch = original;
        mismatch.owner = ActivityWaitOwner::Session { id: "56".into(), generation: Some("native-login".into()) };
        assert!(!mismatch.is_valid());
    }
    #[test]
    fn native_output_meaning_is_explicit_per_engine_and_never_a_generic_elapsed_or_client_read_inference() {
        assert_eq!(
            crate::Engine::Mssql.spec().native_wait_mechanism("ASYNC_NETWORK_IO"),
            Some(crate::telemetry::WaitMechanism::ClientResultOutput)
        );
        for engine in crate::Engine::ALL {
            let expected =
                (engine == crate::Engine::Postgres).then_some(crate::telemetry::WaitMechanism::ClientResultOutput);
            assert_eq!(engine.spec().native_wait_mechanism("Client: ClientWrite"), expected, "{engine:?}");
            assert_eq!(engine.spec().native_wait_mechanism("Client: ClientRead"), None, "{engine:?}");
        }
    }
}
