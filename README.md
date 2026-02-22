# fleet-router

![CI](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/fleet-router.svg)](https://crates.io/crates/fleet-router)

The `fleet-router` is a reverse proxy and load balancer dedicated to the Antelope SHiP protocol. The Fleet SHiP Router is built with Rust using [rs_abieos](https://github.com/eosrio/rs-abieos) for maximum efficiency.

Major Features include:

- **Resilient Connections:** the Fleet SHiP Router maintains persistent client-side connections even when backend servers go offline while there are other backend servers available. This eliminates the need for developers to manually handle reconnections, simplifying application logic.
- **Intelligent Upstream Selection:** The router dynamically routes requests to the most appropriate SHiP server based on factors like data availability and server load. If a server lacks the requested data range, the router seamlessly redirects to a suitable alternative, ensuring a successful response for the user.


### Fleet Router - Antelope SHiP Reverse Proxy &amp; Load Balancer

Installing Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Installing  Fleet Router from `crates.io`

```bash
cargo install fleet-router
```

Installing Fleet Router from GitHub

```bash
git clone https://github.com/eosrio/fleet-router.git
cd fleet-router
cargo install --path .
```

Create the configuration file

```bash
fleet-router config init /path/to/config.json
```

Configuration Reference

```json5
{
  // Address to Listen for client connections
  "listen_address": "0.0.0.0",
  
  // Port to listen for client connections
  "listen_port": 17000,
  
  // Interval to attempt reconnection to the backend servers
  "upstream_reconnect_ms": 3000,
  
  // Interval to log upstream status
  "upstream_monitoring_ms": 5000,
  
  // Interval to send status requests to upstream servers
  "upstream_status_ms": 5000,
  
  // Array of upstream SHiP nodes
  "servers": [
    {
      "name": "SHIP Node 1",          // Server name for logging
      "endpoint": "127.0.0.1:18080",  // Websocket endpoint
      "enabled": true                 // Allow fleet to use this upstream
    },
    {
      "name": "SHIP Node 2",
      "endpoint": "127.0.0.1:28080",
      "enabled": true
    }
  ]
}
```

Usage

```
fleet-router run --config /path/to/config.json
```

