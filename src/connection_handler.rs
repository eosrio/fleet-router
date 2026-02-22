use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::sync::Notify;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{
    accept_async, connect_async_with_config, tungstenite, MaybeTlsStream, WebSocketStream,
};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::functions::select_backend_server;
use crate::models::{ServerConfigDb, ServerStateDb};


pub struct ConnectionGuard {
    pub endpoint: String,
    pub backend_servers: ServerStateDb,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let db = self.backend_servers.clone();
        // Only spawn if the Tokio runtime is still alive (prevents panic during shutdown)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut lock = db.lock().await;
                if let Some(server) = lock.get_mut(&endpoint) {
                    if server.connections > 0 {
                        server.connections -= 1;
                    }
                }
            });
        }
    }
}

async fn get_socket(
    backend_servers: &ServerStateDb,
    server_config_db: &ServerConfigDb,
    max_attempts: u32,
    requested_block: Option<u32>,
) -> Option<(ConnectionGuard, WebSocketStream<MaybeTlsStream<TcpStream>>)> {
    for _ in 0..max_attempts {
        // Select a backend server and optimistically increment counter
        let server = {
            let backend_servers_lock = &mut backend_servers.lock().await;
            let selected_backend = match select_backend_server(backend_servers_lock, requested_block) {
                Ok(endpoint) => endpoint,
                Err(_) => {
                    continue;
                }
            };
            // Optimistic increment — prevents thundering herd
            if let Some(s) = backend_servers_lock.get_mut(&selected_backend) {
                s.connections += 1;
            }
            let server_config = server_config_db.lock().await;
            let Some(cfg) = server_config.get(&selected_backend) else {
                // Rollback optimistic increment
                if let Some(s) = backend_servers_lock.get_mut(&selected_backend) {
                    s.connections -= 1;
                }
                continue;
            };
            cfg.clone()
        };

        let mut config = WebSocketConfig::default();

        // Set the maximum message size to 1 GB
        config.max_message_size = Some(1_073_741_824);

        // Connect to the selected server
        match connect_async_with_config(server.ws_url(), Some(config), true).await {
            Ok((ws_stream, _)) => {
                println!("[client_handler] Connected to the server {}", server.name);
                // Counter was already incremented optimistically
                return Some((
                    ConnectionGuard {
                        endpoint: server.endpoint.clone(),
                        backend_servers: backend_servers.clone(),
                    },
                    ws_stream,
                ));
            }
            Err(e) => {
                eprintln!("[client_handler] Error connecting to the server: {}", e);
                // Rollback optimistic increment and mark offline
                {
                    let backend_server_lock = &mut backend_servers.lock().await;
                    if let Some(s) = backend_server_lock.get_mut(&server.endpoint) {
                        s.connections -= 1;
                        s.online = false;
                    }
                }
                continue;
            }
        }
    }
    eprintln!(
        "[client_handler] Unable to connect to any server after {} attempts",
        max_attempts
    );
    None
}


pub async fn handle_client(
    client_stream: TcpStream,
    client_address: std::net::SocketAddr,
    backend_servers: ServerStateDb,
    server_config_db: ServerConfigDb,
    shared_abieos: Arc<Mutex<Abieos>>,
) {
    // Start the WebSocket protocol on the accepted connection stream, gracefully close the connection if it fails
    let mut client_websocket = match accept_async(client_stream).await {
        Ok(ws_stream) => ws_stream,
        Err(e) => {
            eprintln!("Error during WebSocket handshake: {}", e);
            return;
        }
    };

    println!("New incoming WebSocket connection: {}", client_address);

    let Some((mut _active_conn, server_socket)) =
        get_socket(&backend_servers, &server_config_db, 3, None).await
    else {
        eprintln!("[client_handler] No upstream server available! Closing connection.");
        // Gracefully close the client connection
        let _ = client_websocket
            .close(Some(CloseFrame {
                code: CloseCode::Error,
                reason: "No upstream server available!".into(),
            }))
            .await;
        return;
    };

    // Split the client websocket
    let (client_writer, mut client_reader) = client_websocket.split();

    // Add the client writer to a Mutex
    let client_writer = Arc::new(Mutex::new(client_writer));
    let server_ship_abi: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    // Store raw binary request bytes (V0 or V1) for failover replay
    let last_request: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

    // Get the server WebSocket reader and writer
    let (server_writer, server_reader_stream) = server_socket.split();
    // Add the server reader and writer to a Mutex
    let server_reader = Arc::new(Mutex::new(server_reader_stream));
    let server_writer = Arc::new(Mutex::new(server_writer));

    // Store the server's ABI
    let shared_abieos_arc = shared_abieos.clone();
    {
        let ship_abi_arc = server_ship_abi.clone();
        let server_reader_arc = server_reader.clone();
        let mut server_reader_guard = server_reader_arc.lock().await;
        if let Some(Ok(Message::Text(text))) = server_reader_guard.next().await {
            let abieos = shared_abieos_arc.lock().await;
            if let Err(e) = abieos.set_abi_json("0", &text) {
                eprintln!("[client_handler] Error setting ABI: {}", e);
                return;
            }
            let mut server_ship_abi = ship_abi_arc.lock().await;
            *server_ship_abi = Some(text.to_string());
        } else {
            eprintln!("[client_handler] Error reading first message from server");
            return;
        }
    }

    // Client loop on a sub-task
    let last_request_arc = last_request.clone();
    let server_writer_arc = server_writer.clone();
    let client_disconnected = Arc::new(AtomicBool::new(false));
    let client_disconnect_notify = Arc::new(Notify::new());

    let client_disconnected_tx = client_disconnected.clone();
    let client_disconnect_notify_tx = client_disconnect_notify.clone();
    // Forward messages from the client to the server, with request inspection
    spawn(async move {
        while let Some(Ok(msg)) = client_reader.next().await {
            // Intercept V0 (variant 1) and V1 (variant 3) block requests — store raw bytes
            if let Message::Binary(ref bin_msg) = msg {
                if bin_msg.len() >= 5 && (bin_msg[0] == 1 || bin_msg[0] == 3) {
                    *last_request_arc.lock().await = Some(bin_msg.to_vec());
                }
            }

            let mut server_writer = server_writer_arc.lock().await;
            if let Err(e) = server_writer.send(msg).await {
                eprintln!("[client_handler] Error sending to server: {}", e);
                continue; // Don't break — failover may swap the socket
            }
        }

        println!("Client stream ended");
        client_disconnected_tx.store(true, Ordering::Release);
        client_disconnect_notify_tx.notify_one();
    });

    // Send the server's ABI to the client
    let ship_abi_arc = server_ship_abi.clone();
    {
        let server_ship_abi = ship_abi_arc.lock().await;
        let client_writer_clone = client_writer.clone();
        if let Some(abi) = &*server_ship_abi {
            println!("[client_handler] Sending ABI to client...");
            let mut client_writer = client_writer_clone.lock().await;
            if let Err(e) = client_writer
                .send(Message::Text(abi.as_str().into()))
                .await
            {
                eprintln!("[client_handler] Error sending ABI to client: {}", e);
                return;
            }
        }
    }

    // Server loop — inline block tracking (zero-copy, no background task)
    let client_writer_arc = client_writer.clone();
    let server_reader_arc = server_reader.clone();
    let ship_abi_arc = server_ship_abi.clone();
    let shared_abieos_arc = shared_abieos.clone();

    let mut client_writer = client_writer_arc.lock().await;
    let mut server_reader = server_reader_arc.lock().await;

    let mut restarting = false;
    let mut last_seen_block: Option<u32> = None;

    let client_disconnected_rx = client_disconnected.clone();
    let client_disconnect_notify_rx = client_disconnect_notify.clone();
    loop {
        // Keep forwarding messages from the server to the client
        let mut client_down = false;
        loop {
            // Check if client already disconnected before blocking on server
            if client_disconnected_rx.load(Ordering::Acquire) {
                println!("[client_handler] Client already disconnected");
                client_down = true;
                break;
            }
            let msg = tokio::select! {
                msg = server_reader.next() => msg,
                _ = client_disconnect_notify_rx.notified() => {
                    println!("[client_handler] Client disconnected while waiting for server data");
                    client_down = true;
                    break;
                }
            };
            let Some(Ok(msg)) = msg else { break; };

            // Extract block_num from binary layout (zero-copy):
            // get_blocks_result_v0 (variant 1) or v1 (variant 2)
            // variant(1) + head(4+32) + lib(4+32) + this_block_flag(1) = offset 74
            let mut current_block: Option<u32> = None;
            if let Message::Binary(ref bin) = msg {
                if bin.len() >= 78 && (bin[0] == 1 || bin[0] == 2) && bin[73] == 1 {
                    if let Ok(bytes) = bin[74..78].try_into() {
                        current_block = Some(u32::from_le_bytes(bytes));
                    }
                }
            }

            // Inline dedup during failover (no clones, no channels)
            if restarting {
                if let Some(b_num) = current_block {
                    let expected = last_seen_block.map_or(0, |b| b + 1);
                    if b_num < expected {
                        continue; // drop duplicate block
                    }
                    restarting = false;
                } else {
                    continue; // skip non-block frames until we sync up
                }
            }

            if let Some(b_num) = current_block {
                last_seen_block = Some(b_num);
            }

            if let Err(e) = client_writer.send(msg).await {
                eprintln!("[client_handler] Error sending message to client: {}", e);
                client_down = true;
                break;
            }
        } // end of server reader loop

        // if the client closed the connection, break the final loop
        if client_down {
            break;
        }

        // Update last_request with the actual next block for failover replay
        {
            let mut lr = last_request.lock().await;
            if let Some(ref mut req_bytes) = *lr {
                if let Some(lb) = last_seen_block {
                    let next_block = lb + 1;
                    if req_bytes.len() >= 5 {
                        req_bytes[1..5].copy_from_slice(&next_block.to_le_bytes());
                    }
                }
                // If last_seen_block is None, keep original start_block (no blocks were sent)
            }
        }

        println!("[client_handler] Server stream ended");

        // now we must select another server to reconnect
        // Extract requested start_block from last_request for range-aware routing
        let failover_block = {
            let lr = last_request.lock().await;
            lr.as_ref().and_then(|req| {
                if req.len() >= 5 {
                    Some(u32::from_le_bytes(req[1..5].try_into().unwrap()))
                } else {
                    None
                }
            })
        };

        let Some((new_conn, new_stream)) = get_socket(&backend_servers, &server_config_db, 3, failover_block).await
        else {
            eprintln!("[client_handler] No upstream server available! Closing connection.");
            break;
        };
        _active_conn = new_conn; // drops the old guard, decrementing its counter

        // Re-split and update mutexes
        let mut server_writer_lock = server_writer.lock().await;
        let (writer, reader) = new_stream.split();
        *server_reader = reader;
        *server_writer_lock = writer;
        drop(server_writer_lock);

        // Get the first message from the server
        let first_message = server_reader.next().await;
        if let Some(Ok(Message::Text(text))) = first_message {
            // Use abieos to set the ABI and confirm that the server response is valid
            let abieos = shared_abieos_arc.lock().await;
            match abieos.set_abi_json("0", &text) {
                Ok(_) => {
                    let mut server_ship_abi = ship_abi_arc.lock().await;
                    *server_ship_abi = Some(text.to_string());
                    println!("[client_handler] ABI set successfully");
                }
                Err(e) => {
                    eprintln!("[client_handler] Error setting ABI: {}", e);
                    break;
                }
            }
        } else {
            eprintln!("[client_handler] Error reading first message from server");
            break;
        }

        // Sleep briefly before resuming
        sleep(Duration::from_millis(100)).await;

        // Replay the last request (raw bytes, already patched with next block)
        let lr = last_request.lock().await;
        if let Some(req_bytes) = &*lr {
            let msg = Message::Binary(req_bytes.clone().into());
            let mut server_writer_lock = server_writer.lock().await;
            if let Err(e) = server_writer_lock.send(msg).await {
                eprintln!("[client_handler] Error replaying request: {}", e);
                break;
            }
        }
        drop(lr);

        println!("Reconnected to the server");
        restarting = true;
    }
}
