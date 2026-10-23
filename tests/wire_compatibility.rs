//! Wire SHiP handshake and envelope compatibility through the running router.
//! The payloads are opaque mock data; this does not validate Wire trace codecs.

mod common;

use std::time::Duration;

use common::{base_config, http_get, read_msg, server, wait_metric, ws_connect, RouterProcess};
use futures::SinkExt;
use mock_ship::protocol::generate_fake_data;
use mock_ship::{MockShipConfig, MockShipServer, Observed};
use rs_abieos::{AbiHandle, Abieos};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

const WIRE_ABI: &str = include_str!("fixtures/wire_ship_abi.json");
const WAIT: Duration = Duration::from_secs(5);

/// Exercise the actual CLI handshake probe against the supplied first frame.
async fn config_test_abi(text: &str, strict: bool) -> (bool, String) {
    let mock = MockShipServer::new(MockShipConfig {
        abi_text: Some(text.into()),
        ..MockShipConfig::live(1, 1000)
    })
    .await;
    let endpoint = mock.endpoint();
    let _mock = mock.start();
    let mut cfg = base_config(&[server("wire", &endpoint)]);
    // The CLI validates fixed ports without starting a listener.
    cfg["listen_port"] = json!(17000);
    cfg.as_object_mut().unwrap().remove("metrics_port");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(file.as_file_mut(), &cfg).unwrap();
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fleet-router"));
    command.args(["config", "test"]).arg(file.path());
    if strict {
        command.arg("--strict");
    }
    let output = timeout(WAIT, command.kill_on_drop(true).output())
        .await
        .expect("config test timed out")
        .expect("spawn config test");
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[tokio::test]
async fn config_test_accepts_wire_abi_in_both_modes() {
    for strict in [false, true] {
        let (ok, output) = config_test_abi(WIRE_ABI, strict).await;
        assert!(ok, "strict={strict}: {output}");
        assert!(
            output.contains("ABI Received") && output.contains("Valid servers:"),
            "the runtime accepts this Wire ABI; the CLI must also accept it (strict={strict}): {output}"
        );
        assert!(!output.contains("Invalid ABI"), "{output}");
    }
}

#[tokio::test]
async fn config_test_rejects_invalid_abis_containing_the_eosio_marker() {
    for abi in [
        "not JSON: eosio::abi/1.1",
        r#"{"version":"unsupported::abi/1.1","comment":"eosio::abi"}"#,
        r#"{"version":"eosio::abi/1.1","structs":false}"#,
    ] {
        let (_, output) = config_test_abi(abi, false).await;
        assert!(output.contains("Invalid ABI!"), "{abi}: {output}");
        assert!(output.contains("Error:"), "missing parser error: {output}");
        assert!(!output.contains("ABI Received"), "{abi}: {output}");
    }
}

fn decode(abi: &mut AbiHandle, frame: Message) -> Value {
    let Message::Binary(bytes) = frame else {
        panic!("expected a binary SHiP result, got {frame:?}");
    };
    let text = abi
        .bin_to_json("result", &bytes)
        .expect("Wire result layout");
    serde_json::from_str(&text).expect("result JSON")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

async fn wire_roundtrip(v1: bool) {
    let mock = MockShipServer::new(MockShipConfig {
        abi_text: Some(WIRE_ABI.into()),
        chain_id: [0xAB; 32],
        block_data_size: 32,
        finality_data_begin_block: 1,
        finality_data_end_block: 1001,
        ..MockShipConfig::live(1, 1000)
    })
    .await;
    let endpoint = mock.endpoint();
    let mock = mock.start();
    let router = RouterProcess::start(base_config(&[server("wire", &endpoint)])).await;
    wait_metric(
        router.metrics(),
        &format!("fleet_router_upstream_up{{endpoint=\"{endpoint}\",server=\"wire\"}} 1"),
        WAIT,
    )
    .await;
    assert_eq!(http_get(router.metrics(), "/ready").await.unwrap().0, 200);

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
    let Message::Text(text) = read_msg(&mut reader, WAIT).await else {
        panic!("expected the upstream ABI as the first client frame");
    };
    assert_eq!(text.as_str(), WIRE_ABI, "forward the exact Wire ABI");
    let mut abi = AbiHandle::from_json(&text).expect("Wire ABI");
    let encoder = Abieos::new();
    encoder
        .set_abi_json("ship", &text)
        .expect("Wire ABI encoder");
    let status_type = if v1 {
        "get_status_request_v1"
    } else {
        "get_status_request_v0"
    };
    let request = encoder
        .json_to_bin("ship", "request", &json!([status_type, {}]).to_string())
        .unwrap();
    writer.send(Message::Binary(request.into())).await.unwrap();
    let status = decode(&mut abi, read_msg(&mut reader, WAIT).await);
    assert_eq!(
        status[0],
        if v1 {
            "get_status_result_v1"
        } else {
            "get_status_result_v0"
        }
    );
    assert_eq!(status[1]["head"]["block_num"], 1000);
    assert_eq!(status[1]["last_irreversible"]["block_num"], 990);
    assert_eq!(status[1]["trace_begin_block"], 1);
    assert_eq!(status[1]["trace_end_block"], 1001);
    assert_eq!(
        status[1]["chain_id"].as_str().unwrap().to_ascii_uppercase(),
        "AB".repeat(32)
    );
    if v1 {
        assert_eq!(status[1]["finality_data_begin_block"], 1);
        assert_eq!(status[1]["finality_data_end_block"], 1001);
    }

    let mut fields = json!({
        "start_block_num": 100,
        "end_block_num": 102,
        "max_messages_in_flight": 1,
        "have_positions": [],
        "irreversible_only": false,
        "fetch_block": true,
        "fetch_traces": true,
        "fetch_deltas": true,
    });
    if v1 {
        fields["fetch_finality_data"] = json!(true);
    }
    let request_type = if v1 {
        "get_blocks_request_v1"
    } else {
        "get_blocks_request_v0"
    };
    let request = encoder
        .json_to_bin(
            "ship",
            "request",
            &json!([request_type, fields]).to_string(),
        )
        .unwrap();
    writer.send(Message::Binary(request.into())).await.unwrap();
    for num in 100..102 {
        let result = decode(&mut abi, read_msg(&mut reader, WAIT).await);
        assert_eq!(
            result[0],
            if v1 {
                "get_blocks_result_v1"
            } else {
                "get_blocks_result_v0"
            }
        );
        assert_eq!(result[1]["this_block"]["block_num"], num);
        assert_eq!(result[1]["prev_block"]["block_num"], num - 1);
        for (name, offset) in [("block", 0), ("traces", 1000), ("deltas", 2000)] {
            assert_eq!(
                result[1][name].as_str().unwrap().to_ascii_uppercase(),
                hex(&generate_fake_data(num + offset, 32))
            );
        }
        if v1 {
            assert_eq!(
                result[1]["finality_data"]
                    .as_str()
                    .unwrap()
                    .to_ascii_uppercase(),
                hex(&generate_fake_data(num + 3000, 32))
            );
        }
        if num == 100 {
            assert!(
                timeout(Duration::from_millis(100), read_msg(&mut reader, WAIT))
                    .await
                    .is_err(),
                "the second block requires ACK credit"
            );
            let ack = encoder
                .json_to_bin(
                    "ship",
                    "request",
                    &json!(["get_blocks_ack_request_v0", {"num_messages": 1}]).to_string(),
                )
                .unwrap();
            writer.send(Message::Binary(ack.into())).await.unwrap();
        }
    }
    assert!(mock.requests().iter().any(|request| matches!(request,
        Observed::Blocks { v1: actual_v1, start: 100, fetch_block: true, fetch_traces: true, fetch_deltas: true, fetch_finality_data, .. }
        if *actual_v1 == v1 && *fetch_finality_data == v1
    )));
}

#[tokio::test]
async fn wire_v0_handshake_status_and_blocks() {
    wire_roundtrip(false).await;
}

#[tokio::test]
async fn wire_v1_handshake_status_blocks_and_finality() {
    wire_roundtrip(true).await;
}

#[tokio::test]
async fn rejected_abi_logs_the_parser_error() {
    let mock = MockShipServer::new(MockShipConfig {
        abi_text: Some(r#"{"version":"unsupported::abi/1.1"}"#.into()),
        ..MockShipConfig::live(1, 1000)
    })
    .await;
    let endpoint = mock.endpoint();
    let _mock = mock.start();
    let logs = tempfile::tempdir().unwrap();
    let router = RouterProcess::start_recording(
        base_config(&[server("invalid", &endpoint)]),
        Some(logs.path()),
    )
    .await;
    let mut output = String::new();
    let found = timeout(WAIT, async {
        loop {
            output =
                std::fs::read_to_string(logs.path().join("router.stdout.log")).unwrap_or_default();
            output.push_str(
                &std::fs::read_to_string(logs.path().join("router.stderr.log")).unwrap_or_default(),
            );
            if output.contains("error setting ABI for upstream")
                && output.contains("unsupported abi version")
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "missing parser error in monitor warning: {output}"
    );
    assert_eq!(http_get(router.metrics(), "/ready").await.unwrap().0, 503);
}
