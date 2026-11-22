//! Bounded recorded question state. This is model validation, never native read admission.
use super::*;

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
        Ok(result)
    }
    pub fn is_valid(&self) -> bool {
        self.0.len() <= SourceReadQuestion::ALL.len()
            && self.0.iter().enumerate().all(|(index, stage)| {
                stage.is_valid()
                    && SourceReadQuestion::ALL.contains(&stage.focus.question)
                    && self.0[..index].iter().all(|earlier| earlier.focus.question != stage.focus.question)
            })
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn get(&self, question: SourceReadQuestion) -> Option<&SourceReadStage> {
        self.0.iter().find(|stage| stage.focus.question == question)
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SourceReadStage> + DoubleEndedIterator {
        self.0.iter()
    }
    pub(super) fn enroll(&mut self, stage: SourceReadStage) -> Result<()> {
        if let Some(existing) = self.get(stage.focus.question) {
            return if existing.focus == stage.focus { Ok(()) } else { Err(invalid()) };
        }
        self.insert(stage)
    }
    pub(super) fn transition(&mut self, focus: &SourceReadFocus, state: SourceReadStageState) -> Result<()> {
        let index = self.0.iter().position(|stage| stage.focus.question == focus.question).ok_or_else(invalid)?;
        if self.0[index].focus != *focus {
            return Err(invalid());
        }
        let next = self.0[index].transition(state)?;
        self.0[index] = next;
        Ok(())
    }
    pub(super) fn accepts_transition(&self, next: &Self) -> bool {
        self.is_valid()
            && next.is_valid()
            && self.0.iter().all(|before| {
                next.get(before.focus.question)
                    .is_some_and(|after| before.focus == after.focus && before.transition(after.state.clone()).is_ok())
            })
            && next.0.iter().all(|after| {
                self.get(after.focus.question).is_some()
                    || matches!(after.state, SourceReadStageState::Pending | SourceReadStageState::Deferred { .. })
            })
    }
    fn insert(&mut self, stage: SourceReadStage) -> Result<()> {
        if !stage.is_valid()
            || self.0.len() >= SourceReadQuestion::ALL.len()
            || self.get(stage.focus.question).is_some()
        {
            return Err(invalid());
        }
        let rank = SourceReadQuestion::ALL
            .iter()
            .position(|question| *question == stage.focus.question)
            .ok_or_else(invalid)?;
        let index = self
            .0
            .iter()
            .position(|existing| {
                SourceReadQuestion::ALL
                    .iter()
                    .position(|question| *question == existing.focus.question)
                    .is_some_and(|value| value > rank)
            })
            .unwrap_or(self.0.len());
        self.0.insert(index, stage);
        Ok(())
    }
}
impl<'de> Deserialize<'de> for SourceReadStages {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Stages;
        impl<'de> serde::de::Visitor<'de> for Stages {
            type Value = SourceReadStages;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bounded unique source questions")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut values: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut result = SourceReadStages::new();
                while let Some(stage) = values.next_element::<SourceReadStage>()? {
                    result.insert(stage).map_err(|_| serde::de::Error::custom("invalid source question stages"))?;
                }
                Ok(result)
            }
        }
        deserializer.deserialize_seq(Stages)
    }
}
