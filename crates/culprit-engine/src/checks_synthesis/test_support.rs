//! One controlled plugin check serves both authenticated intake adapters.
use super::*;

pub(super) struct CaptureFinding {
    pub many: bool,
}
impl Check for CaptureFinding {
    fn info(&self) -> CheckInfo {
        CheckInfo {
            id: "controlled-capture".into(),
            title: "Controlled capture".into(),
            category: Category::Queries,
            engines: Engine::ALL.to_vec(),
            requires: vec![],
            description: "Controlled source-free pipeline assertion".into(),
        }
    }
    fn run(&self, _: &Snapshot) -> Vec<Finding> {
        (0..if self.many { 150 } else { 1 })
            .map(|number| Finding {
                id: format!("controlled-capture:{number}"),
                check: "controlled-capture".into(),
                category: Category::Queries,
                severity: Severity::Low,
                actionability: Actionability::Information,
                confidence: Confidence::High,
                title: "Selected capture analyzed".into(),
                summary: if self.many {
                    "bounded output ".repeat(10_000)
                } else {
                    "Selected original evaluated".into()
                },
                subject: Subject { kind: SubjectKind::Database, name: "Selected database".into() },
                impact: Impact::default(),
                metrics: vec![],
                queries: vec![],
                fix: None,
                verify: vec!["Compare originals".into()],
                verification_basis: VerificationBasis::Rescan,
                caveats: vec![],
                last_seen: None,
                inactive: false,
                facts: Default::default(),
            })
            .collect()
    }
}
