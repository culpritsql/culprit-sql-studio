//! Bounded recorded observation stages. Each original answer is immutable; one closed question may add one continuation.
use super::*;

pub const MAX_TRANSACTION_METADATA_OBSERVATIONS: usize = 2;
pub const MAX_SOURCE_READ_STAGES: usize = SourceReadQuestion::ALL.len() + MAX_TRANSACTION_METADATA_OBSERVATIONS - 1;

#[derive(Serialize, TS, Clone, Debug, Default, PartialEq)]
#[serde(transparent)]
#[ts(export)]
pub struct SourceReadStages(Vec<SourceReadStage>);

impl SourceReadStages {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn from_stages(stages: Vec<SourceReadStage>) -> Result<Self> {
        let mut result = Self::new();
        for stage in stages {
            result.insert(stage)?;
        }
        if !result.is_valid() {
            return Err(invalid());
        }
        Ok(result)
    }
    pub fn is_valid(&self) -> bool {
        self.0.len() <= MAX_SOURCE_READ_STAGES
            && self.0.iter().enumerate().all(|(index, stage)| {
                stage.is_valid()
                    && SourceReadQuestion::ALL.contains(&stage.focus.question)
                    && self.0[..index].iter().all(|earlier| !same_observation(&earlier.focus, &stage.focus))
                    && self.continuation_is_valid(stage)
            })
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// The initial question's stage stays available after a separately admitted continuation.
    pub fn get(&self, question: SourceReadQuestion) -> Option<&SourceReadStage> {
        self.0.iter().find(|stage| stage.focus.question == question)
    }
    pub fn latest(&self, question: SourceReadQuestion) -> Option<&SourceReadStage> {
        self.0.iter().rev().find(|stage| stage.focus.question == question)
    }
    pub fn get_focus(&self, focus: &SourceReadFocus) -> Option<&SourceReadStage> {
        self.0.iter().find(|stage| stage.focus == *focus)
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SourceReadStage> + DoubleEndedIterator {
        self.0.iter()
    }
    pub(super) fn enroll(&mut self, stage: SourceReadStage) -> Result<()> {
        if self.get_focus(&stage.focus).is_some() {
            return Ok(());
        }
        self.insert(stage)?;
        if !self.is_valid() {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn transition(&mut self, focus: &SourceReadFocus, state: SourceReadStageState) -> Result<()> {
        let index = self.0.iter().position(|stage| stage.focus == *focus).ok_or_else(invalid)?;
        let next = self.0[index].transition(state)?;
        self.0[index] = next;
        Ok(())
    }
    pub(super) fn accepts_transition(&self, next: &Self) -> bool {
        self.is_valid()
            && next.is_valid()
            && self.iter().all(|before| {
                next.get_focus(&before.focus).is_some_and(|after| before.transition(after.state.clone()).is_ok())
            })
            && next.iter().all(|after| {
                self.get_focus(&after.focus).is_some()
                    || self.continuation_is_valid(after)
                        && matches!(after.state, SourceReadStageState::Pending | SourceReadStageState::Deferred { .. })
            })
    }
    /// A catalog refresh changes refusal/declaration status, never a native owner or completed answer.
    pub(super) fn accepts_admission_refresh(&self, next: &Self) -> bool {
        self.accepts_transition(next)
            && self.iter().all(|before| {
                next.get_focus(&before.focus).is_some_and(|after| {
                    before == after
                        || matches!(before.state, SourceReadStageState::Pending | SourceReadStageState::Deferred { .. })
                            && matches!(after.state, SourceReadStageState::Deferred { .. })
                })
            })
            && next.iter().all(|after| {
                self.get_focus(&after.focus).is_some() || matches!(after.state, SourceReadStageState::Deferred { .. })
            })
    }
    fn continuation_is_valid(&self, stage: &SourceReadStage) -> bool {
        let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } =
            &stage.focus.prerequisite
        else {
            return true;
        };
        self.get(SourceReadQuestion::TransactionMetadata).is_some_and(|initial| {
            initial.focus.observation_ordinal() == ObservationOrdinal::Initial
                && matches!(&initial.state, SourceReadStageState::Answered { answer }
                if matches!(answer.as_ref(), SourceReadAnswer::TransactionMetadata(first)
                    if continuation.matches_first(first)))
        })
    }
    fn insert(&mut self, stage: SourceReadStage) -> Result<()> {
        if !stage.is_valid()
            || self.0.len() >= MAX_SOURCE_READ_STAGES
            || !SourceReadQuestion::ALL.contains(&stage.focus.question)
            || self.iter().any(|before| same_observation(&before.focus, &stage.focus))
        {
            return Err(invalid());
        }
        let wanted_rank = rank(&stage.focus)?;
        let index = self
            .0
            .iter()
            .position(|existing| rank(&existing.focus).is_ok_and(|value| value > wanted_rank))
            .unwrap_or(self.0.len());
        self.0.insert(index, stage);
        Ok(())
    }
}
fn same_observation(left: &SourceReadFocus, right: &SourceReadFocus) -> bool {
    left.question == right.question && left.observation_ordinal() == right.observation_ordinal()
}
fn rank(focus: &SourceReadFocus) -> Result<(usize, ObservationOrdinal)> {
    let question =
        SourceReadQuestion::ALL.iter().position(|question| *question == focus.question).ok_or_else(invalid)?;
    Ok((question, focus.observation_ordinal()))
}
impl<'de> Deserialize<'de> for SourceReadStages {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Stages;
        impl<'de> serde::de::Visitor<'de> for Stages {
            type Value = SourceReadStages;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bounded unique source observations")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut values: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut result = SourceReadStages::new();
                while let Some(stage) = values.next_element::<SourceReadStage>()? {
                    result.insert(stage).map_err(|_| serde::de::Error::custom("invalid source observation stages"))?;
                }
                if !result.is_valid() {
                    return Err(serde::de::Error::custom("invalid source observation stages"));
                }
                Ok(result)
            }
        }
        deserializer.deserialize_seq(Stages)
    }
}
