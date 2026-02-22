use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

use crate::abi::SHIP_ABI_JSON;
use crate::protocol::{
    decode_request, encode_blocks_result_v0_with_data, encode_status_result_v0, generate_fake_data,
    ShipRequest,
};

/// Configuration for a mock SHiP server instance.
#[derive(Debug, Clone)]
pub struct MockShipConfig {
    /// TCP port to bind to. Use 0 for a random available port.
    pub port: u16,
    /// Simulated head block number.
    pub head_block: u32,
    /// Simulated last irreversible block number.
    pub lib_block: u32,
    /// Number of blocks to stream before stopping (0 = unlimited until end_block).
    pub blocks_to_stream: u32,
    /// Optional delay between sending each block result (simulates slow upstream).
    pub block_delay: Option<Duration>,
    /// Optional: disconnect after sending N blocks (simulates upstream failure).
    pub disconnect_after: Option<u32>,
    /// 32-byte chain ID.
    pub chain_id: [u8; 32],
    /// Size of fake data payloads per block (bytes). 0 = no data even if requested.
    pub block_data_size: usize,
    /// First block with trace data available. Default: 1.
    pub trace_begin_block: u32,
    /// One past last block with trace data. Default: 0 = use head_block + 1.
    pub trace_end_block: u32,
}

impl Default for MockShipConfig {
    fn default() -> Self {
        Self {
            port: 0,
            head_block: 1000,
            lib_block: 990,
            blocks_to_stream: 100,
            block_delay: None,
            disconnect_after: None,
            chain_id: [0u8; 32],
            block_data_size: 0,
            trace_begin_block: 1,
            trace_end_block: 0, // 0 = use head_block + 1
        }
    }
}

/// A mock SHiP WebSocket server for testing fleet-router.
///
/// Implements the Spring v1.2.2 SHiP protocol:
/// 1. Accepts WebSocket connection
/// 2. Sends ABI JSON as Text frame
/// 3. Switches to binary and handles requests
pub struct MockShipServer {
    listener: TcpListener,
    config: MockShipConfig,
    /// The actual bound address (useful when port=0)
    addr: SocketAddr,
}

impl MockShipServer {
    /// Create and bind a new mock server. Does NOT start accepting connections.
    pub async fn new(config: MockShipConfig) -> Self {
        let bind_addr = format!("127.0.0.1:{}", config.port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind mock SHiP server to {}: {}", bind_addr, e));
        let addr = listener.local_addr().unwrap();
        Self {
            listener,
            config,
            addr,
        }
    }

    /// Get the WebSocket URL for this server (e.g., "ws://127.0.0.1:12345").
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Get the bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get the endpoint string (e.g., "127.0.0.1:12345").
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Accept and handle a single connection, then return.
    /// This is the simplest mode for unit tests.
    pub async fn handle_one_connection(&self) {
        let (stream, _peer) = self.listener.accept().await.expect("Failed to accept");
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("Failed WebSocket handshake");

        let config = self.config.clone();
        Self::handle_session(ws_stream, config).await;
    }

    /// Start accepting connections in a loop. Returns a handle to stop it.
    /// Each connection is handled in a spawned task.
    pub fn start(self) -> MockShipHandle {
        let config = self.config.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let connection_count = Arc::new(Mutex::new(0u32));
        let count_clone = connection_count.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = self.listener.accept() => {
                        match result {
                            Ok((stream, _peer)) => {
                                let cfg = config.clone();
                                let count = count_clone.clone();
                                tokio::spawn(async move {
                                    {
                                        let mut c = count.lock().await;
                                        *c += 1;
                                    }
                                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                                        Self::handle_session(ws, cfg).await;
                                    }
                                    {
                                        let mut c = count.lock().await;
                                        *c -= 1;
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!("[mock-ship] Accept error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        MockShipHandle {
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(handle),
            connection_count,
        }
    }

    /// Handle a single WebSocket session according to the SHiP protocol.
    async fn handle_session(
        ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        config: MockShipConfig,
    ) {
        let (mut writer, mut reader) = ws_stream.split();

        // Step 1: Send ABI as Text frame
        if writer
            .send(Message::Text(SHIP_ABI_JSON.into()))
            .await
            .is_err()
        {
            return;
        }

        // Step 2: Handle binary requests
        let mut send_credits: u32 = 0;
        let mut current_block: Option<u32> = None;
        let mut end_block: u32 = 0;
        let mut blocks_sent: u32 = 0;
        let mut fetch_block = false;
        let mut fetch_traces = false;
        let mut fetch_deltas = false;

        loop {
            // If we have credits and an active block request, send blocks
            if send_credits > 0 {
                if let Some(block_num) = current_block {
                    if block_num < end_block
                        && (config.blocks_to_stream == 0 || blocks_sent < config.blocks_to_stream)
                    {
                        // Check disconnect_after
                        if let Some(limit) = config.disconnect_after {
                            if blocks_sent >= limit {
                                return; // Simulate upstream failure
                            }
                        }

                        // Optional delay
                        if let Some(delay) = config.block_delay {
                            sleep(delay).await;
                        }

                        let block_data = if fetch_block && config.block_data_size > 0 {
                            Some(generate_fake_data(block_num, config.block_data_size))
                        } else {
                            None
                        };
                        let traces_data = if fetch_traces && config.block_data_size > 0 {
                            Some(generate_fake_data(block_num.wrapping_add(1000), config.block_data_size))
                        } else {
                            None
                        };
                        let deltas_data = if fetch_deltas && config.block_data_size > 0 {
                            Some(generate_fake_data(block_num.wrapping_add(2000), config.block_data_size))
                        } else {
                            None
                        };

                        let result = encode_blocks_result_v0_with_data(
                            config.head_block,
                            config.lib_block,
                            block_num,
                            block_data.as_deref(),
                            traces_data.as_deref(),
                            deltas_data.as_deref(),
                        );
                        if writer.send(Message::Binary(result.into())).await.is_err() {
                            return;
                        }

                        current_block = Some(block_num + 1);
                        send_credits -= 1;
                        blocks_sent += 1;
                        continue; // Try to send more before reading
                    }
                }
            }

            // Read next request from client
            let msg = match reader.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(_)) | None => return,
            };

            match msg {
                Message::Binary(data) => {
                    let request = decode_request(&data);
                    match request {
                        ShipRequest::GetStatusV0 | ShipRequest::GetStatusV1 => {
                            let trace_end = if config.trace_end_block > 0 {
                                config.trace_end_block
                            } else {
                                config.head_block + 1
                            };
                            let result = encode_status_result_v0(
                                config.head_block,
                                config.lib_block,
                                &config.chain_id,
                                config.trace_begin_block,
                                trace_end,
                            );
                            if writer.send(Message::Binary(result.into())).await.is_err() {
                                return;
                            }
                        }
                        ShipRequest::GetBlocksV0 {
                            start_block_num,
                            end_block_num,
                            max_messages_in_flight,
                            fetch_block: fb,
                            fetch_traces: ft,
                            fetch_deltas: fd,
                        }
                        | ShipRequest::GetBlocksV1 {
                            start_block_num,
                            end_block_num,
                            max_messages_in_flight,
                            fetch_block: fb,
                            fetch_traces: ft,
                            fetch_deltas: fd,
                        } => {
                            current_block = Some(start_block_num);
                            end_block = end_block_num;
                            send_credits = max_messages_in_flight;
                            blocks_sent = 0;
                            fetch_block = fb;
                            fetch_traces = ft;
                            fetch_deltas = fd;
                        }
                        ShipRequest::GetBlocksAckV0 { num_messages } => {
                            send_credits += num_messages;
                        }
                        ShipRequest::Unknown(idx) => {
                            eprintln!("[mock-ship] Unknown request variant: {}", idx);
                        }
                    }
                }
                Message::Close(_) => return,
                _ => {} // Ignore other message types
            }
        }
    }
}

/// Handle for a running mock server. Drop to stop.
pub struct MockShipHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
    connection_count: Arc<Mutex<u32>>,
}

impl MockShipHandle {
    /// Get the current number of active connections.
    pub async fn connection_count(&self) -> u32 {
        *self.connection_count.lock().await
    }

    /// Gracefully shut down the server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for MockShipHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}
