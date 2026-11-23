//! Durable catalog acquisition shares the existing held database owner and the one snapshot stream.
use super::*;

const CATALOG_READER_QUERIES: u32 = 12;
pub(in crate::telemetry) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        max_queries: requested
            .max_queries
            .min(CATALOG_READER_QUERIES + crate::read::ReadSession::SESSION_SETUP_QUERIES),
        ..requested.clone()
    }
}

impl<D: SnapshotRecipe, R: NativeStreamRead> NativeSnapshotReader<D, R> {
    pub(super) async fn describe_catalog(&mut self) -> Result<(CollectionTarget, SourcePartition, String, bool)> {
        if !D::DATASET.is_mssql_database_catalog() {
            return Err(invalid_data());
        }
        let profile =
            description::observed_profile(&mut self.io, crate::database_identity::log_rates::Detail::Identity).await?;
        if profile.edition == crate::probe::ENGINE_EDITION_AZURE_SQL_DATABASE {
            return self.describe_azure_database().await;
        }
        let owner = DatabaseAcquisition::read_catalog(&mut self.io, &self.host, self.port, profile.edition).await?;
        let context = owner.owner()?;
        let visible = if D::REQUIRES_FULL_METADATA { state::clock(&mut self.io).await?.1 } else { true };
        self.authority = SnapshotAuthority::Catalog(Box::new(owner));
        Ok((context.target, context.partition, context.observed_from, visible))
    }

    pub(super) async fn describe_azure_database(
        &mut self,
    ) -> Result<(CollectionTarget, SourcePartition, String, bool)> {
        let (database, physical) = description::identity(&mut self.io, &self.host, self.port).await?;
        let (at, visible) = state::clock(&mut self.io).await?;
        self.authority = SnapshotAuthority::AzureLogical { physical_incarnation: physical };
        Ok((database.into(), SourcePartition::Target, at, visible))
    }

    pub(super) async fn recheck_catalog(
        &mut self,
        initial: &CollectionDescription,
    ) -> Result<culprit_read::SnapshotRecheck> {
        let SnapshotAuthority::Catalog(before) = &self.authority else { return Err(invalid_data()) };
        let after = before.recheck(&mut self.io, &self.host, self.port).await?;
        let context = before.completed(&after)?;
        if initial.target != context.target || initial.partition != context.partition {
            return Err(CollectionStop::SourceChanged.error());
        }
        let visible = if D::REQUIRES_FULL_METADATA { state::clock(&mut self.io).await?.1 } else { true };
        let scope = if self.layout.is_none() {
            culprit_read::SnapshotScope::NotApplicable { source: D::SOURCE.name.into() }
        } else {
            culprit_read::SnapshotScope::Available {
                source: D::SOURCE.name.into(),
                complete: before.database_wide() && after.database_wide() && visible,
                error_code: (!visible).then_some(culprit_core::ErrorCode::PermissionDenied),
            }
        };
        Ok(culprit_read::SnapshotRecheck {
            unchanged: true,
            observed_until: context.observed_until.clone(),
            execution: Some(Box::new(context)),
            scopes: vec![scope],
        })
    }
}
