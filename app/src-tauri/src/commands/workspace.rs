//! Desktop workspace capability; native work remains with the existing Workspace owner.
use culprit_core::Result;
use culprit_engine::Workspace;
use std::sync::Arc;

pub(crate) type Ws<'a> = tauri::State<'a, std::sync::Arc<culprit_engine::Workspace>>;
/// Store updates can wait for file locks; report reads decode and replay large artifacts. Keep both off UI/runtime threads.
pub(crate) async fn blocking_workspace<T: Send + 'static>(
    ws: Arc<Workspace>,
    update: impl FnOnce(&Workspace) -> Result<T> + Send + 'static,
) -> Result<T> {
    culprit_engine::run_blocking(move || update(&ws)).await
}
#[cfg(test)]
mod workspace_write_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn a_contended_store_write_leaves_the_command_runtime_responsive() {
        let dir = std::env::temp_dir().join(format!("culprit-app-write-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("queries.toml.lock"))
            .unwrap();
        held.lock().unwrap();
        let ws = Arc::new(Workspace::open_in(&dir));
        let (started, waiting) = tokio::sync::oneshot::channel();
        let write = tokio::spawn(blocking_workspace(ws.clone(), move |ws| {
            let _ = started.send(());
            ws.save_query(culprit_engine::saved::SaveQuery {
                name: "health".into(),
                sql: "SELECT 1".into(),
                ..Default::default()
            })
        }));
        tokio::time::timeout(Duration::from_secs(2), waiting).await.unwrap().unwrap();
        let runtime_progressed_while_waiting = !write.is_finished();
        drop(held);
        let saved = tokio::time::timeout(Duration::from_secs(2), write).await.unwrap().unwrap().unwrap();
        assert!(runtime_progressed_while_waiting);
        assert_eq!(saved.name, "health");
        assert_eq!(ws.saved_queries().unwrap().len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
