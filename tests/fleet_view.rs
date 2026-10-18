//! End to end: the upstream monitors feed each listener's fleet view, which
//! `/coverage`, `/ready?scope=history` and the per-listener series report.

mod common;

use std::time::Duration;

use common::{base_config, frozen, http_get, server, wait_coverage, wait_metric, RouterProcess};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::{json, Value};

const WAIT: Duration = Duration::from_secs(5);

fn first(v: &Value) -> &Value {
    &v["listeners"][0]
}

#[tokio::test]
async fn stitched_coverage_and_history_readiness_follow_the_monitors() {
    let a = MockShipServer::new(MockShipConfig::frozen(2, 1001)).await;
    let b = MockShipServer::new(MockShipConfig::frozen(1001, 2001)).await;
    let c = MockShipServer::new(MockShipConfig::live(1900, 2300)).await;
    let (ea, eb, ec) = (a.endpoint(), b.endpoint(), c.endpoint());
    let (a, _b, _c) = (a.start(), b.start(), c.start());
    let mut cfg = base_config(&[
        frozen("archive-a", &ea, Some((2, 1001))),
        frozen("archive-b", &eb, None),
        server("live-1", &ec),
    ]);
    cfg["chain_id"] = json!("00".repeat(32));
    let router = RouterProcess::start(cfg).await;
    let port = router.metrics();

    wait_coverage(
        port,
        |v| {
            let l = first(v);
            l["topology"] == "stitched"
                && l["status_ready"] == true
                && l["head"]["num"] == 2300
                && l["lib"]["num"] == 2290
                && l["logs"]["trace"]["reported"] == json!({ "begin": 2, "end": 2301 })
                && l["logs"]["trace"]["offline_only"] == json!([])
        },
        WAIT,
    )
    .await;
    assert_eq!(http_get(port, "/ready").await.map(|r| r.0), Some(200));
    assert_eq!(
        http_get(port, "/ready?scope=history").await.map(|r| r.0),
        Some(200)
    );
    wait_metric(port, "fleet_router_fleet_head_block 2300\n", WAIT).await;
    wait_metric(
        port,
        "fleet_router_fleet_coverage_begin_block{log=\"trace\"} 2\n",
        WAIT,
    )
    .await;

    // Archive A goes down: its range stays in the status coverage, the tip
    // stays servable, and history readiness names the segment.
    a.stall_and_drop_sessions();
    wait_coverage(
        port,
        |v| first(v)["logs"]["trace"]["offline_only"] == json!([{ "begin": 2, "end": 1001 }]),
        WAIT,
    )
    .await;
    let (_, body) = http_get(port, "/coverage").await.unwrap();
    let doc: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        first(&doc)["logs"]["trace"]["reported"],
        json!({ "begin": 2, "end": 2301 })
    );
    assert_eq!(http_get(port, "/ready").await.map(|r| r.0), Some(200));
    let (code, body) = http_get(port, "/ready?scope=history").await.unwrap();
    assert_eq!(code, 503);
    assert!(
        body.contains("segment [2,1001) has no online upstream"),
        "unexpected body: {body}"
    );
    wait_metric(
        port,
        "fleet_router_fleet_offline_coverage_blocks{log=\"trace\"} 999\n",
        WAIT,
    )
    .await;
}

#[tokio::test]
async fn pool_listeners_publish_their_fleet_tip() {
    let mock = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let endpoint = mock.endpoint();
    let _mock = mock.start();
    let router = RouterProcess::start(base_config(&[server("live-1", &endpoint)])).await;
    let port = router.metrics();

    wait_coverage(
        port,
        |v| first(v)["topology"] == "pool" && first(v)["head"]["num"] == 1000,
        WAIT,
    )
    .await;
    wait_metric(port, "fleet_router_fleet_head_block 1000\n", WAIT).await;
    assert_eq!(
        http_get(port, "/ready?scope=history").await.map(|r| r.0),
        Some(200)
    );
}
