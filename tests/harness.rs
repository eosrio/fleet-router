//! Smoke tests for the shared e2e harness in `tests/common`.

mod common;

use std::time::Duration;

use common::{
    base_config, frozen, http_get, metric_value, read_blocks, read_msg, server, wait_metric,
    ws_connect, RouterProcess,
};
use futures::SinkExt;
use mock_ship::client::{blocks, status_v0, BlocksReq, StatusView};
use mock_ship::{MockShipConfig, MockShipServer, Observed};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

#[test]
fn config_builders_shape_servers() {
    assert_eq!(
        frozen("archive-a", "127.0.0.1:9000", Some((2, 1001))),
        json!({
            "name": "archive-a",
            "endpoint": "127.0.0.1:9000",
            "enabled": true,
            "role": "frozen",
            "range": { "begin": 2, "end": 1001 },
        })
    );
    assert!(frozen("archive-b", "127.0.0.1:9001", None)
        .get("range")
        .is_none());
    let cfg = base_config(&[server("live-1", "127.0.0.1:9002")]);
    assert_eq!(cfg["upstream_monitoring_ms"], 200);
    assert_eq!(cfg["servers"][0]["enabled"], true);
}

#[tokio::test]
async fn router_process_streams_from_a_live_mock() {
    let mock = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let endpoint = mock.endpoint();
    let mock = mock.start();
    let router = RouterProcess::start(base_config(&[server("live-1", &endpoint)])).await;
    let metrics = router.metrics();
    let up = format!("fleet_router_upstream_up{{endpoint=\"{endpoint}\",server=\"live-1\"}}");
    wait_metric(metrics, &format!("{up} 1"), Duration::from_secs(5)).await;
    assert_eq!(metric_value(metrics, &up).await, Some(1.0));
    assert_eq!(
        http_get(metrics, "/ready").await.map(|(code, _)| code),
        Some(200)
    );

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
    assert!(matches!(
        read_msg(&mut reader, Duration::from_secs(5)).await,
        Message::Text(_)
    ));
    writer
        .send(Message::Binary(status_v0().into()))
        .await
        .unwrap();
    let Message::Binary(reply) = read_msg(&mut reader, Duration::from_secs(5)).await else {
        panic!("expected a status result");
    };
    let status = StatusView::parse(&reply).expect("status result");
    assert_eq!((status.variant, status.head.0), (0, 1000));

    let request = BlocksReq {
        start: 100,
        end: 110,
        max_in_flight: 10,
        ..BlocksReq::default()
    };
    writer
        .send(Message::Binary(blocks(&request).into()))
        .await
        .unwrap();
    let (got, close) = read_blocks(&mut reader, 10, Duration::from_secs(5)).await;
    assert!(close.is_none(), "unexpected close: {close:?}");
    let nums: Vec<Option<u32>> = got.iter().map(|b| b.num()).collect();
    assert_eq!(nums, (100..110).map(Some).collect::<Vec<_>>());
    assert!(mock.requests().iter().any(|o| matches!(
        o,
        Observed::Blocks {
            start: 100,
            end: 110,
            max_in_flight: 10,
            ..
        }
    )));
}
