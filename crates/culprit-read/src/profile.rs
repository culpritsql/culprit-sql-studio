//! An approved profile is checked inside an existing held reader; this guard owns no native work.
use culprit_core::{Result, telemetry::*};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Clone)]
pub struct SourceProfileGuard {
    admission: SourceProfileAdmission,
    binding: Arc<Mutex<Option<NativeBinding>>>,
    refused: Arc<AtomicBool>,
    observed_generation: Arc<AtomicU64>,
}
struct NativeBinding {
    context: NativeReadContext,
    connection_generation: u64,
}
impl SourceProfileGuard {
    pub fn new(request: &CollectionRequest, admission: &SourceProfileAdmission) -> Result<Self> {
        if !admission.matches_request(request) {
            return Err(changed());
        }
        Ok(Self {
            admission: admission.clone(),
            binding: Arc::new(Mutex::new(None)),
            refused: Arc::new(AtomicBool::new(false)),
            observed_generation: Arc::new(AtomicU64::new(0)),
        })
    }
    fn refuse(&self) -> culprit_core::Error {
        self.refused.store(true, Ordering::Release);
        changed()
    }
    pub fn builtin_policy(&self) -> Option<&BuiltinRecipeSafety> {
        self.admission.builtin_policy()
    }
    pub fn observed_generation(&self) -> Option<u64> {
        match self.observed_generation.load(Ordering::Acquire) {
            0 => None,
            generation => Some(generation),
        }
    }
    /// Authority discovery can precede native binding; revocation is still checked before login and SET.
    pub fn ensure_active(&self) -> Result<()> {
        if self.refused.load(Ordering::Acquire) || !self.admission.approval_is_active() {
            Err(changed())
        } else {
            Ok(())
        }
    }
    /// The existing original native authority must bind the held socket before any population query.
    pub fn bind_native(&self, context: &NativeReadContext, connection_generation: Option<u64>) -> Result<()> {
        self.ensure_active()?;
        let generation = connection_generation.filter(|generation| *generation > 0).ok_or_else(|| self.refuse())?;
        if !self.admission.matches_native_context(context) {
            return Err(self.refuse());
        }
        let mut binding = self.binding.lock().map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
        if binding.is_some() {
            return Err(self.refuse());
        }
        self.observed_generation.store(generation, Ordering::Release);
        *binding = Some(NativeBinding { context: context.clone(), connection_generation: generation });
        Ok(())
    }
    /// A settled/retired socket cannot be silently replaced after the original authority was bound.
    pub fn check_connection(&self, connection_generation: Option<u64>) -> Result<()> {
        self.ensure_active()?;
        if let Some(generation) = connection_generation.filter(|generation| *generation > 0) {
            let previous = self.observed_generation.load(Ordering::Acquire);
            if previous == 0 {
                self.observed_generation.store(generation, Ordering::Release);
            } else if previous != generation {
                return Err(self.refuse());
            }
        }
        let binding = self.binding.lock().map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
        if let Some(binding) = binding.as_ref()
            && (connection_generation != Some(binding.connection_generation)
                || !self.admission.matches_native_context(&binding.context))
        {
            return Err(self.refuse());
        }
        Ok(())
    }
    /// The source's existing final witness remains mandatory; matching old profile names is insufficient.
    pub fn verify_final(&self, context: &NativeReadContext, connection_generation: Option<u64>) -> Result<()> {
        self.check_connection(connection_generation)?;
        if !self.admission.matches_native_context(context) {
            return Err(self.refuse());
        }
        let binding = self.binding.lock().map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
        let binding = binding.as_ref().ok_or_else(|| self.refuse())?;
        if binding.context.target != context.target
            || binding.context.partition != context.partition
            || binding.context.database_writable != context.database_writable
            || binding.context.observed_from != context.observed_from
            || ObservationBounds::new(&binding.context.observed_until, &context.observed_until).is_none()
        {
            return Err(self.refuse());
        }
        Ok(())
    }
}

pub struct GuardedTelemetrySource<S> {
    inner: S,
    guard: SourceProfileGuard,
    initial: Option<NativeReadContext>,
}

impl<S: TelemetrySource> GuardedTelemetrySource<S> {
    pub fn new(inner: S, guard: SourceProfileGuard) -> Self {
        Self { inner, guard, initial: None }
    }
}

#[async_trait::async_trait]
impl<S: TelemetrySource> TelemetrySource for GuardedTelemetrySource<S> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.guard.ensure_active()?;
        let description = self.inner.describe().await?;
        let context = NativeReadContext::from_investigation_description(&description)?;
        self.guard.bind_native(&context, self.guard.observed_generation())?;
        self.initial = Some(context);
        Ok(description)
    }

    fn query_count(&self) -> u32 {
        self.inner.query_count()
    }

    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.inner.read_metrics()
    }

    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        self.guard.check_connection(self.guard.observed_generation())?;
        self.inner.read_page(stream, cursor).await
    }

    async fn finish(&mut self) -> Result<SourceCompletion> {
        let completion = self.inner.finish().await?;
        if completion != SourceCompletion::Changed {
            let final_context = match &completion {
                SourceCompletion::SourceRead { execution, .. }
                | SourceCompletion::LocalResource { execution, .. }
                | SourceCompletion::SnapshotAcquisition { execution, .. } => execution.as_ref(),
                _ => self.initial.as_ref().ok_or_else(changed)?,
            };
            self.guard.verify_final(final_context, self.guard.observed_generation())?;
        }
        Ok(completion)
    }

    async fn close(&mut self) -> SourceShutdown {
        self.inner.close().await
    }
}
fn changed() -> culprit_core::Error {
    CollectionStop::SourceChanged.error()
}
