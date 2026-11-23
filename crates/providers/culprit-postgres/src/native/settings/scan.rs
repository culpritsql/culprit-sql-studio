//! The ordinary scan keeps the same native settings rows and paired held-read context as retained analysis.
use super::*;
use crate::native::{
    local_context::{self, Context, SameConnection},
    metadata,
};
use culprit_core::{Error, Setting};

pub(crate) struct CollectedSettings {
    pub read: PostgresSettingsRead,
    pub values: Vec<Setting>,
    pub failure: Option<Error>,
}
impl CollectedSettings {
    fn observed(
        observations: Vec<SettingObservation>,
        settings_context: NativeSettingsContext,
        acquisition: PostgresSettingsAcquisition,
    ) -> Self {
        let values = observations.iter().filter_map(|setting| settings_context.project(setting)).collect();
        let read = PostgresSettingsRead { settings_context, observations, population_complete: true, acquisition };
        let failure = (!read.is_valid()).then(invalid);
        Self { read, values, failure }
    }
    fn unqualified(
        observations: Vec<SettingObservation>,
        settings_context: NativeSettingsContext,
        before: NativeReadContext,
        after: Option<NativeReadContext>,
        reason: PostgresSettingsAcquisitionGap,
        failure: Option<Error>,
    ) -> Self {
        let mut read = Self::observed(
            observations,
            settings_context,
            PostgresSettingsAcquisition::Unqualified {
                before: Some(Box::new(before)),
                after: after.map(Box::new),
                reason,
            },
        );
        read.failure = failure.or(read.failure);
        read
    }
}

/// Four additional ordinary statements: metadata/member before2 and after2. The selected Settings query,
/// its SQL limit/decoder and the caller's existing allowance/held connection remain unchanged.
pub(crate) async fn scan<R: QueryRead>(reader: &mut R, host: &str, port: u16) -> Result<CollectedSettings> {
    let before_metadata = metadata::activity(reader, host, port).await?;
    let settings_context = NativeSettingsContext {
        format: NativeSettingsFormat::PostgresSettings,
        version: before_metadata.version.to_string(),
    };
    // Capture only after the legitimate first successful acquisition. Application readers retire rather than
    // replace an interrupted socket; SameConnection additionally refuses any different reader generation.
    let generation = reader.connection_generation().ok_or_else(|| CollectionStop::SourceChanged.error())?;
    let mut held = SameConnection::new(reader, generation);
    let before = Context::observe(&mut held, &before_metadata).await?;
    let initial = match &before {
        Some(owner) => owner.read_context()?,
        None => local_context::unknown(&before_metadata, &before_metadata.at)?,
    };
    let observations =
        crate::read::query(&mut held, &query(SETTINGS, 1_000)).await?.iter().map(decode).collect::<Result<Vec<_>>>()?;
    let after_metadata = match metadata::activity(&mut held, host, port).await {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(CollectedSettings::unqualified(
                observations,
                settings_context,
                initial,
                None,
                PostgresSettingsAcquisitionGap::ReadFailed,
                Some(error),
            ));
        }
    };
    let completed =
        match local_context::complete_observed(&mut held, &before_metadata, before.as_ref(), &after_metadata).await {
            Ok(completed) => completed,
            Err(error) => {
                return Ok(CollectedSettings::unqualified(
                    observations,
                    settings_context,
                    initial,
                    None,
                    PostgresSettingsAcquisitionGap::ReadFailed,
                    Some(error),
                ));
            }
        };
    match completed {
        Some(completed) if completed.context.has_qualified_local_owner() => Ok(CollectedSettings::observed(
            observations,
            settings_context,
            PostgresSettingsAcquisition::Completed { context: Box::new(completed.context) },
        )),
        Some(completed) => Ok(CollectedSettings::unqualified(
            observations,
            settings_context,
            initial,
            Some(completed.context),
            PostgresSettingsAcquisitionGap::ContextUnavailable,
            None,
        )),
        None => Ok(CollectedSettings::unqualified(
            observations,
            settings_context,
            initial,
            Some(local_context::unknown(&after_metadata, &after_metadata.at)?),
            PostgresSettingsAcquisitionGap::SourceChanged,
            Some(CollectionStop::SourceChanged.error()),
        )),
    }
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
