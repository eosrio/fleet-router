//! Upstream `wss://` against a local rustls mock.
//!
//! The mock's certificate is signed by a test CA written to `SSL_CERT_FILE`.
//! rustls-native-certs loads that file instead of the OS store, on every
//! platform, so the router can verify the certificate without a public CA.
//!
//! `config test`, the upstream monitor, and the client proxy dial all go
//! through `Server::ws_url`. Failover uses the same dial as the proxy
//! (`establish_upstream`).

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mock_ship::{serve_websocket, MockShipConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

struct TlsShip {
    endpoint: String,
    ca_file: tempfile::NamedTempFile,
    join: tokio::task::JoinHandle<()>,
}

impl Drop for TlsShip {
    fn drop(&mut self) {
        self.join.abort();
    }
}

fn install_tls_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();
}

fn test_server_config(ca_pem: &mut impl Write) -> Arc<ServerConfig> {
    install_tls_provider();

    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "fleet-router test CA");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    write!(ca_pem, "{}", ca_cert.pem()).expect("write ca pem");

    let server_key = KeyPair::generate().expect("server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    server_params
        .distinguished_name
        .push(DnType::CommonName, "127.0.0.1");
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server cert");

    let certs = vec![CertificateDer::from(server_cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der()));
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("rustls server config");
    Arc::new(config)
}

async fn start_tls_ship() -> TlsShip {
    let mut ca_file = tempfile::NamedTempFile::new().expect("ca file");
    let server_config = test_server_config(ca_file.as_file_mut());
    ca_file.flush().expect("flush ca");
    let acceptor = TlsAcceptor::from(server_config);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let ship = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        ..MockShipConfig::default()
    };

    let join = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            let ship = ship.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(stream).await else {
                    return;
                };
                let Ok(ws) = tokio_tungstenite::accept_async(tls).await else {
                    return;
                };
                serve_websocket(ship, ws).await;
            });
        }
    });

    TlsShip {
        endpoint: format!("wss://{addr}"),
        ca_file,
        join,
    }
}

fn router_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fleet-router")
}

fn write_config(
    endpoint: &str,
    listen_port: u16,
    metrics_port: Option<u16>,
) -> tempfile::NamedTempFile {
    let mut config = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": listen_port,
        "upstream_reconnect_ms": 200,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 200,
        "handshake_timeout_ms": 2000,
        "servers": [{
            "name": "tls-mock",
            "endpoint": endpoint,
            "enabled": true
        }]
    });
    if let Some(port) = metrics_port {
        config["metrics_address"] = serde_json::json!("127.0.0.1");
        config["metrics_port"] = serde_json::json!(port);
    }
    let mut file = tempfile::NamedTempFile::new().expect("config file");
    file.write_all(config.to_string().as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// `config test` is a child process. Run it off the async worker so the mock's
/// accept loop can complete the TLS handshake.
async fn run_config_test(
    cfg: &std::path::Path,
    ca: Option<&std::path::Path>,
) -> std::process::Output {
    let bin = router_bin().to_string();
    let cfg = cfg.to_path_buf();
    let ca = ca.map(|path| path.to_path_buf());
    tokio::task::yield_now().await;
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(bin);
        cmd.args(["config", "test"]).arg(cfg);
        match &ca {
            Some(path) => {
                cmd.env("SSL_CERT_FILE", path);
            }
            None => {
                cmd.env_remove("SSL_CERT_FILE");
            }
        }
        cmd.env_remove("SSL_CERT_DIR")
            .output()
            .expect("spawn config test")
    })
    .await
    .expect("config test task")
}

#[tokio::test]
async fn config_test_accepts_a_trusted_wss_upstream() {
    let ship = start_tls_ship().await;
    let cfg = write_config(&ship.endpoint, free_port(), None);
    let output = run_config_test(cfg.path(), Some(ship.ca_file.path())).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "config test exited {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("ABI Received"),
        "trusted wss upstream was not accepted\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[tokio::test]
async fn config_test_rejects_an_untrusted_wss_certificate() {
    let ship = start_tls_ship().await;
    let cfg = write_config(&ship.endpoint, free_port(), None);
    let output = run_config_test(cfg.path(), None).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "config test should still exit 0 when the probe fails: {stdout}"
    );
    assert!(
        stdout.contains("configuration is valid"),
        "wss endpoint should pass validation\n{stdout}"
    );
    assert!(
        !stdout.contains("ABI Received"),
        "untrusted certificate must not be accepted\n{stdout}"
    );
    assert!(
        stdout.contains("Connection failed"),
        "expected a TLS verification failure\n{stdout}"
    );
}

#[tokio::test]
async fn monitor_and_proxy_dial_a_wss_upstream() {
    let ship = start_tls_ship().await;
    let listen_port = free_port();
    let metrics_port = free_port();
    let cfg = write_config(&ship.endpoint, listen_port, Some(metrics_port));

    let child = Command::new(router_bin())
        .arg("run")
        .arg("--config")
        .arg(cfg.path())
        .env("SSL_CERT_FILE", ship.ca_file.path())
        .env_remove("SSL_CERT_DIR")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn fleet-router");
    let _child = KillChild(child);

    let needle = format!(
        "fleet_router_upstream_up{{endpoint=\"{}\",server=\"tls-mock\"}} 1",
        ship.endpoint
    );
    let mut online = false;
    for _ in 0..50 {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", metrics_port)).await {
            let req = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
            if stream.write_all(req).await.is_ok() {
                let mut buf = Vec::new();
                if stream.read_to_end(&mut buf).await.is_ok()
                    && String::from_utf8_lossy(&buf).contains(&needle)
                {
                    online = true;
                    break;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(online, "monitor did not mark the wss upstream online");

    let url = format!("ws://127.0.0.1:{listen_port}");
    let (ws, _) = connect_async(&url)
        .await
        .expect("client websocket through the router");
    let (_writer, mut reader) = ws.split();
    let msg = tokio::time::timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timed out waiting for proxied ABI")
        .expect("client stream ended")
        .expect("client read");
    match msg {
        Message::Text(text) => {
            let abi: serde_json::Value = serde_json::from_str(&text).expect("abi json");
            assert_eq!(abi["version"], "eosio::abi/1.1");
        }
        other => panic!("expected the upstream ABI, got {other:?}"),
    }
}

struct KillChild(std::process::Child);

impl Drop for KillChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
