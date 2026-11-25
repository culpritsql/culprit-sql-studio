use clap::Parser;
use culprit_archive_web::{
    ArchiveWebApp, ArchiveWebOptions, HttpResponse, MAX_CONCURRENT_REQUESTS, MAX_HTTP_BODY_BYTES, MAX_HTTP_HEADER_BYTES,
};
use culprit_core::{Error, Result, t};
use culprit_engine::evidence::{EvidenceConfig, EvidenceRuntime};
use culprit_read::cancellation::CollectionCancellation;
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

#[derive(Parser)]
#[command(name = "culprit-archive-web", version, about = t!("archive_web.cli.about"))]
struct Cli {
    /// Path to the read-only EvidenceConfig JSON file (warehouse + recovery credentials only).
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
    /// Display name for the archive connection in the web UI.
    #[arg(long, default_value = "archive")]
    name: String,
    /// Socket address to listen on (loopback by default).
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// Built frontend bundle directory (`app/dist`).
    #[arg(long, value_name = "DIR")]
    ui_dist: Option<PathBuf>,
    /// Required reverse-proxy identity header (e.g. X-MS-CLIENT-PRINCIPAL-ID or X-Forwarded-User).
    #[arg(long, value_name = "HEADER")]
    require_auth_header: Option<String>,
    /// Explicitly allow non-loopback binding without a reverse-proxy auth header.
    #[arg(long)]
    allow_unauthenticated_network: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("culprit-archive-web: {}", err.message);
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let options = ArchiveWebOptions {
        connection_name: cli.name,
        bind: cli.bind,
        ui_dist: cli.ui_dist,
        require_auth_header: cli.require_auth_header,
        allow_unauthenticated_network: cli.allow_unauthenticated_network,
    };
    options.validate()?;
    let config = EvidenceConfig::from_file(&cli.config)?;
    let connection = ArchiveWebApp::connection_info_from_config(&options.connection_name, &config)?;
    let service = Arc::new(EvidenceRuntime::default().evidence(&config)?);
    let app = Arc::new(ArchiveWebApp::new(
        connection,
        service.clone(),
        service,
        options.ui_dist,
        options.require_auth_header,
    ));

    let listener = TcpListener::bind(options.bind).await.map_err(|err| Error::internal(err.to_string()))?;
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let shutdown = CollectionCancellation::default();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                shutdown.cancel();
                break;
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|err| Error::internal(err.to_string()))?;
                let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                    let _ = write_simple_busy(stream).await;
                    continue;
                };
                let app = app.clone();
                let request_cancel = CollectionCancellation::default();
                let shutdown_clone = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    tokio::select! {
                        _ = shutdown_clone.cancelled() => {
                            request_cancel.cancel();
                        }
                        _ = handle_connection(stream, app, request_cancel.clone()) => {}
                    }
                });
            }
        }
    }
    Ok(())
}

async fn write_simple_busy(mut stream: TcpStream) -> std::io::Result<()> {
    stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\nConnection: close\r\n\r\nBusy").await
}

async fn handle_connection(mut stream: TcpStream, app: Arc<ArchiveWebApp>, cancellation: CollectionCancellation) {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        if buffer.len() >= MAX_HTTP_HEADER_BYTES {
            let _ = write_response(
                &mut stream,
                "GET",
                HttpResponse {
                    status: 431,
                    content_type: "text/plain; charset=utf-8",
                    body: b"Headers too large".to_vec(),
                },
            )
            .await;
            return;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => {
                cancellation.cancel();
                return;
            }
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
    };

    let Ok(header_str) = std::str::from_utf8(&buffer[..header_end]) else {
        return;
    };
    let mut lines = header_str.split("\r\n");
    let Some(request_line) = lines.next() else {
        return;
    };
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(path)) = (parts.next(), parts.next()) else {
        return;
    };
    let method = method.to_owned();
    let path = path.to_owned();

    let mut headers = BTreeMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let key = name.trim().to_ascii_lowercase();
            let val = value.trim().to_owned();
            if key == "content-length" {
                let Ok(len) = val.parse::<usize>() else { return };
                if len > MAX_HTTP_BODY_BYTES {
                    let _ = write_response(
                        &mut stream,
                        &method,
                        HttpResponse {
                            status: 413,
                            content_type: "text/plain; charset=utf-8",
                            body: b"Payload too large".to_vec(),
                        },
                    )
                    .await;
                    return;
                }
                content_length = len;
            }
            headers.insert(key, val);
        }
    }

    let body_start = header_end + 4;
    let mut body = buffer[body_start..].to_vec();
    while body.len() < content_length {
        let needed = (content_length - body.len()).min(chunk.len());
        match stream.read(&mut chunk[..needed]).await {
            Ok(0) | Err(_) => {
                cancellation.cancel();
                return;
            }
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(content_length);

    let response = app.dispatch(&method, &path, &headers, &body, &cancellation).await;
    let _ = write_response(&mut stream, &method, response).await;
}

async fn write_response(stream: &mut TcpStream, method: &str, response: HttpResponse) -> std::io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        422 => "Unprocessable Content",
        431 => "Request Header Fields Too Large",
        504 => "Gateway Timeout",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nContent-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'\r\nConnection: close\r\n\r\n",
        response.status,
        response.content_type,
        response.body.len(),
    );
    stream.write_all(head.as_bytes()).await?;
    if method != "HEAD" {
        stream.write_all(&response.body).await?;
    }
    Ok(())
}
