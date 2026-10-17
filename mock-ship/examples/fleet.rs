//! Run a small fleet of mock SHiP nodes for local dashboard work.
//!
//! ```bash
//! cargo run -p mock-ship --example fleet
//! ```
//!
//! Prints a fleet-router config with the dashboard enabled (control token
//! `local-dev-control-token`) and keeps the mocks running until Ctrl+C. Save
//! the config and run `fleet-router run --config <file>`, then open
//! `http://127.0.0.1:9090/ui/`.
//!
//! The mocks report a fixed head, so the router marks them stale after a few
//! monitoring intervals.

use std::time::Duration;

use mock_ship::{MockShipConfig, MockShipServer};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let nodes = [
        ("node-a", 1, 1_000_000, 0, 1),
        ("node-b", 1, 999_990, 0, 2),
        ("archive", 1, 1_000_000, 100, 1),
        ("backup", 500_000, 1_000_000, 0, 1),
    ];
    let mut servers = Vec::new();
    let mut handles = Vec::new();
    for (name, trace_begin, head, priority, weight) in nodes {
        let server = MockShipServer::new(MockShipConfig {
            head_block: head,
            lib_block: head - 10,
            trace_begin_block: trace_begin,
            blocks_to_stream: 0,
            block_delay: Some(Duration::from_millis(50)),
            block_data_size: 512,
            ..Default::default()
        })
        .await;
        servers.push(format!(
            r#"    {{ "name": "{name}", "endpoint": "{}", "enabled": true, "priority": {priority}, "weight": {weight} }}"#,
            server.endpoint()
        ));
        handles.push(server.start());
    }
    println!(
        r#"{{
  "listen_address": "127.0.0.1",
  "listen_port": 17000,
  "upstream_reconnect_ms": 2000,
  "upstream_monitoring_ms": 5000,
  "upstream_status_ms": 2000,
  "metrics_address": "127.0.0.1",
  "metrics_port": 9090,
  "dashboard": {{ "enabled": true, "control_token": "local-dev-control-token" }},
  "servers": [
{}
  ]
}}"#,
        servers.join(",\n")
    );
    eprintln!("mock fleet running; press Ctrl+C to stop");
    let _ = tokio::signal::ctrl_c().await;
    for handle in handles {
        handle.shutdown().await;
    }
}
