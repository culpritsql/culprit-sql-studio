use super::super::*;
use culprit_core::telemetry::{CollectionStop, SourceShutdown};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const REJECTION: &str = "Client IP denied by the SQL firewall";
const PRELOGIN: u8 = 0x12;
const LOGIN7: u8 = 0x10;
const SQL_BATCH: u8 = 0x01;
const DONE: [u8; 13] = [0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

async fn read_packet(client: &mut TcpStream, expected: u8) {
    let mut header = [0; 8];
    client.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], expected);
    let size = u16::from_be_bytes([header[2], header[3]]) as usize;
    client.read_exact(&mut vec![0; size - header.len()]).await.unwrap();
}

async fn reply(client: &mut TcpStream, body: &[u8]) {
    let size = u16::try_from(8 + body.len()).unwrap().to_be_bytes();
    client.write_all(&[0x04, 1, size[0], size[1], 0, 0, 1, 0]).await.unwrap();
    client.write_all(body).await.unwrap();
}

fn rejection(code: u32) -> Vec<u8> {
    let mut body = code.to_le_bytes().to_vec();
    body.extend([1, 14]); // Error state and severity.
    let message: Vec<u16> = REJECTION.encode_utf16().collect();
    body.extend(u16::try_from(message.len()).unwrap().to_le_bytes());
    body.extend(message.into_iter().flat_map(u16::to_le_bytes));
    body.extend([0; 6]); // Empty server/procedure names and line number.
    let mut tokens = vec![0xaa]; // TDS ERROR, followed by DONE with the error flag.
    tokens.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    tokens.extend(body);
    let mut done = DONE;
    done[1] = 2;
    tokens.extend(done);
    tokens
}

async fn login(listener: &TcpListener) -> TcpStream {
    let (mut client, _) = listener.accept().await.unwrap();
    read_packet(&mut client, PRELOGIN).await;
    // The loopback fixture explicitly negotiates no encryption.
    reply(&mut client, &[1, 0, 6, 0, 1, 0xff, 2]).await;
    read_packet(&mut client, LOGIN7).await;
    client
}

fn pool(listener: &TcpListener) -> Arc<Pool> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(listener.local_addr().unwrap().port());
    config.authentication(AuthMethod::sql_server("fixture", "fixture"));
    config.encryption(EncryptionLevel::NotSupported);
    Arc::new(Pool::new(config, None))
}

async fn read_error(pool: &Arc<Pool>) -> Error {
    pool.observe(|reader| Box::pin(crate::read::one(reader, "SELECT 1"))).await.unwrap_err()
}

#[tokio::test]
async fn firewall_rejections_preserve_the_server_error_and_admit_the_next_read() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pool = pool(&listener);
    let server = tokio::spawn(async move {
        for code in [40615, 40914] {
            reply(&mut login(&listener).await, &rejection(code)).await;
        }
    });
    for _ in 0..2 {
        let error = read_error(&pool).await;
        assert_eq!(error.code, ErrorCode::ConnectionFailed);
        assert_eq!(error.message, REJECTION);
        assert_eq!(error.hint, Some(t!("provider.mssql.error.firewall.hint")));
        assert_eq!(pool.source_shutdown(), SourceShutdown::Quiescent);
    }
    server.await.unwrap();
}

#[tokio::test]
async fn the_same_server_error_during_session_setup_keeps_unconfirmed_work_fenced() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pool = pool(&listener);
    let server = tokio::spawn(async move {
        let mut client = login(&listener).await;
        reply(&mut client, &DONE).await;
        read_packet(&mut client, SQL_BATCH).await;
        reply(&mut client, &rejection(40615)).await;
    });
    let error = read_error(&pool).await;
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::CancellationUnconfirmed);
    assert_eq!(pool.source_shutdown(), SourceShutdown::Unconfirmed);
    server.await.unwrap();
}
