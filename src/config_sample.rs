pub const CONFIG_SAMPLE: &str = r#"
{
  "listen_address": "0.0.0.0",
  "listen_port": 17000,
  "upstream_reconnect_ms": 3000,
  "upstream_monitoring_ms": 5000,
  "upstream_status_ms": 5000,
  "servers": [
    {
      "name": "SHiP Node 1",
      "endpoint": "127.0.0.1:18080",
      "enabled": true
    },
    {
      "name": "SHiP Node 2",
      "endpoint": "127.0.0.1:28080",
      "enabled": true
    }
  ]
}
"#;
