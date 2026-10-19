//! A node head and the end of a history log are independent SHiP fields.
mod common;

use std::time::Duration;

use common::{base_config, server, RouterProcess};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};

async fn status(router: &RouterProcess) -> Value {
    reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/api/v1/status",
            router.metrics()
        ))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_status(router: &RouterProcess, check: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    let result = timeout(Duration::from_secs(3), async {
        loop {
            last = status(router).await;
            if check(&last) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "dashboard did not converge: {last}");
    last
}

#[tokio::test]
async fn equal_chain_heads_with_different_history_ends_are_not_lagged() {
    let config = |end| MockShipConfig {
        trace_end_block: end,
        ..MockShipConfig::live(1, 1000)
    };
    let a = MockShipServer::new(config(998)).await;
    let b = MockShipServer::new(config(994)).await;
    let mut cfg = base_config(&[server("a", &a.endpoint()), server("b", &b.endpoint())]);
    cfg["dashboard"] = json!({"enabled": true});
    let (_a, _b) = (a.start(), b.start());
    let router = RouterProcess::start(cfg).await;
    wait_status(&router, |s| s["totals"]["upstreams_up"] == 2).await;
    sleep(Duration::from_millis(450)).await;
    let s = status(&router).await;
    for u in s["listeners"][0]["upstreams"].as_array().unwrap() {
        assert_eq!(
            u["head_lag_blocks"], 0,
            "equal node heads classified using log coverage: {s}"
        );
    }
    for u in s["listeners"][0]["upstreams"].as_array().unwrap() {
        assert_eq!(u["chain_head_block"], 1000);
        assert!(u["head_sample_age_ms"].as_u64().unwrap() < 1000);
    }
}

#[tokio::test]
async fn equal_history_ends_do_not_hide_real_chain_head_lag_or_catchup() {
    let config = MockShipConfig {
        trace_end_block: 401,
        ..MockShipConfig::live(1, 1000)
    };
    let a = MockShipServer::new(config.clone()).await;
    let b = MockShipServer::new(config).await;
    let mut cfg = base_config(&[server("a", &a.endpoint()), server("b", &b.endpoint())]);
    cfg["dashboard"] = json!({"enabled": true});
    let (_a, b) = (a.start(), b.start());
    let router = RouterProcess::start(cfg).await;
    wait_status(&router, |s| s["totals"]["upstreams_up"] == 2).await;

    for behind in [1, 7, 200, 0] {
        b.set_head(1000 - behind, 790);
        let s = wait_status(&router, |s| {
            s["listeners"][0]["upstreams"][1]["head_lag_blocks"] == behind
                && s["listeners"][0]["upstreams"][1]["chain_head_block"] == 1000 - behind
        })
        .await;
        assert_eq!(s["listeners"][0]["upstreams"][0]["head_lag_blocks"], 0);
        assert_eq!(s["listeners"][0]["upstreams"][0]["head_block"], 400);
        assert_eq!(s["listeners"][0]["upstreams"][1]["head_block"], 400);
    }
}
