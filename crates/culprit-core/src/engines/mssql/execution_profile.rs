//! Meaning of documented native endpoint waits; no plan/operator or root-cause inference.
use crate::telemetry::{ExecutionTaskWaitKind as Kind, MssqlParallelExchangeWait};
pub(super) fn wait_kind(native: &str) -> Option<Kind> {
    if MssqlParallelExchangeWait::from_native(native).is_some() {
        return Some(Kind::ExchangeSynchronization);
    }
    match native {
        // Shared-mode PAGEIOLATCH is a buffer read from disk; other latch modes retain their own meaning.
        "PAGEIOLATCH_SH" => Some(Kind::BufferRead),
        "LCK_M_S" | "LCK_M_U" | "LCK_M_X" | "LCK_M_IS" | "LCK_M_IX" | "LCK_M_SIX" | "LCK_M_SCH_S" | "LCK_M_SCH_M"
        | "LCK_M_RS_S" | "LCK_M_RS_U" => Some(Kind::NativeLock),
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_engine_retains_its_own_supported_endpoint_semantics() {
        for engine in crate::Engine::ALL {
            assert_eq!(
                engine.spec().execution_task_wait("PAGEIOLATCH_SH"),
                (engine == crate::Engine::Mssql).then_some(Kind::BufferRead)
            );
            assert_eq!(
                engine.spec().execution_task_wait("LCK_M_S"),
                (engine == crate::Engine::Mssql).then_some(Kind::NativeLock)
            );
            for unknown in ["PAGEIOLATCH_EX", "PAGEIOLATCH_NL", "PAGELATCH_SH", "LCK_M_future", ""] {
                assert_eq!(engine.spec().execution_task_wait(unknown), None);
            }
        }
    }
}
