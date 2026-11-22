use super::*;
use async_trait::async_trait;
use culprit_evidence::delivery::heads::DiagnosticHeadPage;

#[async_trait]
impl RecoveryStore for ObjectRecoveryStore {
    async fn checkpoint_head(&self) -> Result<Option<String>> {
        Ok(self.root().await?.0.head_id().map(str::to_owned))
    }
    async fn captured_range(
        &self,
        range: &culprit_core::telemetry::CapturedRange,
    ) -> Result<Option<culprit_core::telemetry::CaptureIdentity>> {
        let Some(range_key) = coverage_key(range)? else {
            return Ok(None);
        };
        let key = format!("coverage/{range_key}.json");
        let (root, _) = self.root().await?;
        if let Some(index) = self.index(&key, &root).await? {
            return self.capture_for_index(&index, range, &root).await.map(Some);
        }
        if let Some(head) = &root.head {
            let manifest = self.manifest(head).await?;
            if coverage_key(&manifest.source.captured_range()?)?.as_deref() == Some(range_key.as_str()) {
                return Ok(Some(manifest.capture));
            }
        }
        Ok(None)
    }

    async fn diagnostic_heads(&self, cursor: Option<&str>, limit: u32) -> Result<DiagnosticHeadPage> {
        let (root, _) = self.root().await?;
        root.diagnostic_heads.page(cursor, limit, root.high_sequence)
    }
    async fn diagnostic_original_groups(
        &self,
        keys: &[String],
    ) -> Result<culprit_evidence::delivery::originals::OriginalHistoryLookup> {
        use culprit_evidence::delivery::originals::{
            MAX_POINTER_BYTES, OriginalHistoryLookup, OriginalHistoryPointer, validate_original_group_keys,
        };
        validate_original_group_keys(keys)?;
        let (root, _) = self.root().await?;
        let mut entries = std::collections::BTreeMap::new();
        for (key, hash) in root.original_groups.selected(keys)? {
            let pointer = if let Some(hash) = hash {
                let object = self
                    .transport
                    .get(&format!("diagnostic-originals/{hash}.json"))
                    .await?
                    .ok_or_else(|| DeliveryProblem::NotReady.error())?;
                let bytes = read_bytes(object, MAX_POINTER_BYTES).await?;
                Some(OriginalHistoryPointer::from_bytes(&bytes, &hash)?)
            } else {
                None
            };
            entries.insert(key, pointer);
        }
        OriginalHistoryLookup::new(keys, entries)
    }
    async fn pending_ranges(&self, cursor: Option<&str>, limit: u32, order: RecoveryOrder) -> Result<RecoveryPage> {
        if !(1..=128).contains(&limit) {
            return Err(DeliveryProblem::Capacity.error());
        }
        let (root, _) = self.root().await?;
        let (ids, next_cursor) =
            root.pending.page(cursor, 1, self.reader.id(), root.head.as_ref().map_or(0, |head| head.order), order)?;
        let mut ranges = Vec::new();
        for id in ids {
            let index = self.committed_index(&id, &root).await?.ok_or_else(integrity)?;
            ranges.push(self.manifest(&index).await?);
        }
        Ok(RecoveryPage { ranges, next_cursor, high_sequence: root.high_sequence })
    }
    async fn pending_diagnostic_inputs(&self, cursor: Option<&str>, limit: u32) -> Result<RecoveryPage> {
        let (root, _) = self.root().await?;
        let (ids, next_cursor) = root.pending.diagnostic_page(
            cursor,
            limit,
            self.reader.id(),
            root.head.as_ref().map_or(0, |head| head.order),
        )?;
        let mut ranges = Vec::new();
        for id in ids {
            let index = self.committed_index(&id, &root).await?.ok_or_else(integrity)?;
            ranges.push(self.manifest(&index).await?);
        }
        Ok(RecoveryPage { ranges, next_cursor, high_sequence: root.high_sequence })
    }
    async fn diagnostic_input_pending(
        &self,
        input: &culprit_core::telemetry::DiagnosticCaptureRef,
        controller_version: u32,
    ) -> Result<bool> {
        let (root, _) = self.root().await?;
        let index = self
            .committed_index(&range_id_for(self.reader.contract(), &input.capture)?, &root)
            .await?
            .ok_or_else(|| DeliveryProblem::NotReady.error())?;
        let manifest = self.manifest(&index).await?;
        super::super::diagnostic_recovery::validate_pending_input(&manifest, input, controller_version)?;
        root.pending.diagnostic_input_pending(input)
    }
    async fn retire(&self, fence: &Fence, publication: &super::super::sink::PublishedRange) -> Result<()> {
        let range = publication.committed();
        if range.store_id() != self.reader.id() {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let (mut root, etag) = self.root().await?;
        root.require_owner(fence)?;
        let index = self.committed_index(&range.manifest().range_id, &root).await?.ok_or_else(integrity)?;
        if index.manifest_digest != range.manifest_digest() {
            return Err(integrity());
        }
        root.pending.retire(&range.manifest().range_id);
        self.replace_root(&root, &etag).await
    }
    async fn recover(&self, cursor: Option<&str>, limit: u32) -> Result<RecoveryPage> {
        if limit == 0 || limit > MAX_RECOVERY_PAGE {
            return Err(DeliveryProblem::Capacity.error());
        }
        let (root, _) = self.root().await?;
        let Some(head) = &root.head else {
            if cursor.is_some() {
                return Err(integrity());
            }
            return Ok(RecoveryPage { ranges: vec![], next_cursor: None, high_sequence: 0 });
        };
        let mut cursor = match cursor {
            Some(value) if value.len() <= MAX_INDEX_BYTES => {
                serde_json::from_str::<RecoveryCursor>(value).map_err(|_| integrity())?
            }
            Some(_) => return Err(DeliveryProblem::Capacity.error()),
            None => RecoveryCursor {
                store_id: self.reader.id().to_owned(),
                after: 0,
                head: head.clone(),
                high_sequence: root.high_sequence,
            },
        };
        cursor.validate(&root)?;
        let boundary = self
            .index(&cursor.head.order_key(), &root)
            .await?
            .or_else(|| root.head.as_ref().filter(|head| head.order == cursor.head.order).cloned())
            .ok_or_else(integrity)?;
        if boundary != cursor.head {
            return Err(integrity());
        }
        let mut ranges = Vec::new();
        let mut bytes = 0usize;
        while cursor.after < cursor.head.order && ranges.len() < limit as usize {
            let order = cursor.after + 1;
            let index = self
                .index(&format!("commits/{order:020}.json"), &root)
                .await?
                .or_else(|| (order == cursor.head.order).then(|| cursor.head.clone()))
                .ok_or_else(integrity)?;
            if index.order != order || (order == cursor.head.order && index != cursor.head) {
                return Err(integrity());
            }
            let manifest = self.manifest(&index).await?;
            let length = encode(&manifest, MAX_MANIFEST_BYTES)?.len();
            if !ranges.is_empty() && bytes + length > MAX_MANIFEST_BYTES {
                break;
            }
            bytes += length;
            ranges.push(manifest);
            cursor.after = order;
        }
        let next_cursor = if cursor.after < cursor.head.order {
            Some(String::from_utf8(encode(&cursor, MAX_INDEX_BYTES)?).map_err(|_| integrity())?)
        } else {
            None
        };
        Ok(RecoveryPage { ranges, next_cursor, high_sequence: cursor.high_sequence })
    }
}
#[async_trait::async_trait]
impl culprit_evidence::delivery::recovery::RecoveryRead for ObjectRecoveryStore {
    fn id(&self) -> &str {
        self.reader.id()
    }
    async fn load(&self, id: &str) -> Result<RangeManifest> {
        self.reader.load(id).await
    }
    async fn capture_manifest(&self, capture: &culprit_core::telemetry::CaptureIdentity) -> Result<RangeManifest> {
        self.reader.capture_manifest(capture).await
    }
    async fn open_object(&self, batch: &PayloadObject) -> Result<RecoveryReader> {
        self.reader.open_object(batch).await
    }
}
#[async_trait::async_trait]
impl culprit_evidence::delivery::mutations::RecoveryWrite for ObjectRecoveryStore {
    async fn stage(&self, batch: &PayloadObject, path: &Path) -> Result<StagedBatch> {
        validate_payload(batch)?;
        self.transport.put_file(&batch.object, path, batch.bytes).await?;
        // Content length and object metadata alone cannot establish an immutable payload receipt.
        self.verify_payload(batch).await?;
        Ok(StagedBatch {
            store_id: self.reader.id().to_owned(),
            object: batch.object.clone(),
            sha256: batch.sha256.clone(),
            bytes: batch.bytes,
        })
    }
    async fn commit(
        &self,
        fence: &Fence,
        expected_head: Option<&str>,
        manifest: &RangeManifest,
        staged: &[StagedBatch],
    ) -> Result<CommitReceipt> {
        manifest.validate()?;
        if manifest.contract != *self.reader.contract() {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let (mut root, etag) = self.root().await?;
        root.require_owner(fence)?;
        let manifest_digest = manifest.digest()?;
        if let Some(existing) = self.committed_index(&manifest.range_id, &root).await? {
            if existing.manifest_digest != manifest_digest {
                return Err(integrity());
            }
            self.manifest(&existing).await?;
            // A root CAS may have succeeded although its HTTP acknowledgement was lost.
            return Ok(self.receipt(&existing));
        }
        if root.head_id() != expected_head {
            return Err(DeliveryProblem::Conflict.error());
        }
        self.verify_staged(manifest, staged).await?;
        let original_heads = super::super::diagnostic_recovery::validate_staged(self, manifest).await?;
        for pointer in original_heads {
            let hash = root.original_groups.observe(&pointer)?;
            self.immutable(&format!("diagnostic-originals/{hash}.json"), pointer.bytes()?).await?;
        }
        root.diagnostic_heads.observe(manifest, &manifest_digest)?;
        self.repair_indexes(&root).await?;
        for advance in &manifest.advances {
            if root.checkpoints.get(&advance.stream_key) != advance.previous.as_ref() {
                return Err(DeliveryProblem::Conflict.error());
            }
            root.checkpoints.insert(advance.stream_key.clone(), advance.next.clone());
        }
        let sequence = manifest
            .capture
            .revision
            .as_ref()
            .ok_or_else(integrity)?
            .sequence
            .parse::<u64>()
            .map_err(|_| integrity())?;
        let order = root.head.as_ref().map_or(Ok(1), |head| head.order.checked_add(1).ok_or_else(integrity))?;
        root.pending.observe(
            order,
            manifest,
            crate::outbox::diagnostic_manifest_eligible(
                &manifest.source,
                manifest.investigation.as_ref(),
                &manifest.reference_coverage,
            )?,
        )?;
        let index = CommitIndex {
            store_id: self.reader.id().to_owned(),
            order,
            range_id: manifest.range_id.clone(),
            manifest_digest,
            high_sequence: root.high_sequence.max(sequence),
            previous: root.head_id().map(str::to_owned),
        };
        self.immutable(&index.manifest_key(), encode(manifest, MAX_MANIFEST_BYTES)?).await?;
        root.indexed_order = order - 1;
        root.high_sequence = root.high_sequence.max(sequence);
        root.head = Some(index.clone());
        // This CAS is the sole commit point, including the stream checkpoint advances.
        self.replace_root(&root, &etag).await?;
        // The root already authorizes recovery even if either index write is interrupted.
        self.repair_indexes(&root).await?;
        Ok(self.receipt(&index))
    }
}

impl ObjectRecoveryStore {
    fn receipt(&self, index: &CommitIndex) -> CommitReceipt {
        CommitReceipt {
            store_id: self.reader.id().to_owned(),
            manifest_digest: index.manifest_digest.clone(),
            head: index.range_id.clone(),
        }
    }
    async fn verify_staged(&self, manifest: &RangeManifest, staged: &[StagedBatch]) -> Result<()> {
        if staged.len() != manifest.batches.len() {
            return Err(integrity());
        }
        for (batch, receipt) in manifest.batches.iter().zip(staged) {
            if receipt.store_id != self.reader.id()
                || receipt.object != batch.object
                || receipt.sha256 != batch.sha256
                || receipt.bytes != batch.bytes
            {
                return Err(integrity());
            }
            self.verify_payload(&PayloadObject::from(batch)).await?;
        }
        Ok(())
    }
}
