use std::sync::Arc;
use std::time::Instant;

use culprit_engine::Workspace;
use tauri::Manager;
use tauri::webview::PageLoadEvent;

mod commands;
mod incident_history;
mod requests;
mod screenshot;
mod sign_in_page;

pub fn run_app() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let trace = std::env::var_os("CULPRIT_TRACE_STARTUP").is_some();
    let workspace = Workspace::open()?;
    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_dialog::init())
        .manage(commands::approvals::Announced::default())
        .manage(commands::authentication::SignIn::default())
        .manage(commands::updates::PendingUpdate::default())
        .manage(commands::discovery::DiscoveryRequests::default())
        .manage(incident_history::HistoryImports::default())
        .manage(commands::native_events::EventImports::default())
        .manage(commands::editor::PreviewRequests::default())
        .manage(commands::connections::ConnectionTests::default())
        .manage(commands::warehouses::WarehouseRequests::default())
        .on_window_event(|window, event| {
            if matches!(event, tauri::WindowEvent::Destroyed) {
                window.state::<commands::discovery::DiscoveryRequests>().close(window.label());
                window.state::<incident_history::HistoryImports>().0.close(window.label());
                window.state::<commands::native_events::EventImports>().0.close(window.label());
                window.state::<commands::editor::PreviewRequests>().close(window.label());
                window.state::<commands::connections::ConnectionTests>().close(window.label());
                window.state::<commands::warehouses::WarehouseRequests>().close(window.label());
            }
        })
        .setup(move |app| {
            let handle = app.handle().clone();
            let workspace = workspace.with_sign_in_handler(Arc::new(move |event| {
                commands::authentication::on_sign_in(&handle, event);
            }));
            app.manage(Arc::new(workspace));
            Ok(())
        })
        .on_page_load(move |webview, payload| {
            if payload.event() != PageLoadEvent::Finished {
                webview.state::<commands::discovery::DiscoveryRequests>().close(webview.label());
                webview.state::<incident_history::HistoryImports>().0.close(webview.label());
                webview.state::<commands::native_events::EventImports>().0.close(webview.label());
                webview.state::<commands::editor::PreviewRequests>().close(webview.label());
                webview.state::<commands::connections::ConnectionTests>().close(webview.label());
                webview.state::<commands::warehouses::WarehouseRequests>().close(webview.label());
                return;
            }
            // The window starts hidden so it never flashes white; show it once the page (and its CSS) is in.
            let window = webview.window();
            let _ = window.show();
            let _ = window.set_focus();
            if trace {
                eprintln!("culprit: window shown {} ms after start", started.elapsed().as_millis());
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::localization::set_language,
            commands::warehouses::begin_warehouse_read,
            commands::warehouses::cancel_warehouse_read,
            commands::warehouses::warehouse_connections,
            commands::warehouses::add_warehouse_connection,
            commands::warehouses::remove_warehouse_connection,
            commands::warehouses::warehouse_resources,
            commands::warehouses::warehouse_incident,
            commands::warehouses::warehouse_window,
            commands::warehouses::warehouse_artifacts,
            commands::connections::connections,
            commands::connections::parse_connection,
            commands::connections::test_connection,
            commands::connections::begin_connection_test,
            commands::connections::cancel_connection_test,
            commands::connections::test_ssh,
            commands::connections::set_ssh,
            commands::connections::add_connection,
            commands::connections::remove_connection,
            commands::connections::rename_connection,
            commands::connections::set_connection_settings,
            commands::connections::test_connection_settings,
            commands::discovery::begin_discovery,
            commands::discovery::cancel_discovery,
            commands::discovery::discover_azure,
            commands::discovery::discover_aws,
            commands::connections::set_password,
            commands::discovery::add_discovered,
            commands::connections::import_candidates,
            commands::connections::import_connections,
            commands::authentication::cancel_sign_in,
            commands::authentication::open_sign_in_page,
            commands::reports::latest_report,
            commands::reports::report_at,
            commands::reports::scan,
            commands::reports::brief,
            commands::plans::explain,
            commands::plans::query_plan,
            commands::plans::last_actual_plan,
            commands::plans::query_progress,
            commands::reports::drift,
            commands::timeline::changes,
            commands::timeline::events,
            commands::timeline::cloud,
            commands::timeline::mark,
            commands::timeline::unmark,
            commands::reports::index_advice,
            commands::reports::dashboard,
            commands::activity::dashboard_live,
            commands::plans::parse_plan,
            commands::plans::diff_plans,
            commands::plans::export_plan,
            commands::plans::export_diff,
            commands::plans::export_query_plan,
            commands::reports::export_finding,
            commands::reports::verify_findings,
            commands::files::save_file,
            commands::files::screenshot,
            commands::activity::live,
            commands::activity::waits,
            commands::activity::wait_sample,
            commands::editor::run_sql,
            commands::native_events::begin_native_events,
            commands::native_events::cancel_native_events,
            commands::native_events::open_native_events,
            incident_history::begin_incident_history,
            incident_history::cancel_incident_history,
            incident_history::open_incident_history,
            commands::editor::begin_preview,
            commands::editor::cancel_preview,
            commands::editor::preview_table,
            commands::activity::deadlocks,
            commands::activity::parse_deadlock,
            commands::editor::check_statement,
            commands::editor::lint_sql,
            commands::editor::checks,
            commands::editor::catalog,
            commands::editor::saved_queries,
            commands::editor::save_query,
            commands::editor::delete_saved_query,
            commands::editor::saved_query_ran,
            commands::approvals::set_agent_access,
            commands::connections::set_query_log,
            commands::files::choose_query_log,
            commands::files::choose_database_file,
            commands::files::choose_ssh_identity,
            commands::files::choose_ca_certificate,
            commands::approvals::pending_approvals,
            commands::approvals::decide_approval,
            commands::activity::interrupt_session,
            commands::updates::check_update,
            commands::updates::install_update,
            commands::updates::restart_app
        ])
        .run(tauri::generate_context!())?;
    Ok(())
}

pub fn run() {
    if let Err(e) = run_app() {
        eprintln!("culprit: {e}");
        std::process::exit(1);
    }
}
