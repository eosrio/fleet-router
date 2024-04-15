use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{accept_async, connect_async_with_config, MaybeTlsStream, tungstenite, WebSocketStream};
use tungstenite::Message;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::protocol::frame::coding::CloseCode;

use crate::functions::select_backend_server;
use crate::models::{GetBlocksRequest, ServerConfigDb, ServerStateDb};
use crate::zcd::{deserialize_result, ZCDValues};

async fn get_socket(
    backend_servers: &ServerStateDb,
    server_config_db: &ServerConfigDb,
    max_attempts: u32,
) -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    for _ in 0..max_attempts {
        // Select a backend server
        let server = {
            let backend_servers_lock = &mut backend_servers.lock().await;
            let selected_backend = match select_backend_server(backend_servers_lock) {
                Ok(endpoint) => endpoint,
                Err(_) => {
                    continue;
                }
            };
            let server_config = server_config_db.lock().await;
            server_config.get(&selected_backend).unwrap().clone()
        };

        let mut config = WebSocketConfig::default();

        // Set the maximum message size to 1 GB
        config.max_message_size = Some(1_073_741_824);

        // Connect to the selected server
        match connect_async_with_config(server.ws_url(), Some(config), true).await {
            Ok((ws_stream, _)) => {
                println!("[client_handler] Connected to the server {}", server.name);
                {
                    // Increment the connection counter
                    let backend_server_lock = &mut backend_servers.lock().await;
                    backend_server_lock
                        .get_mut(&server.endpoint)
                        .unwrap()
                        .connections += 1;
                }
                return Some(ws_stream);
            }
            Err(e) => {
                eprintln!("[client_handler] Error connecting to the server: {}", e);
                // mark this server as unavailable
                {
                    let backend_server_lock = &mut backend_servers.lock().await;
                    backend_server_lock
                        .get_mut(&server.endpoint)
                        .unwrap()
                        .online = false;
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

enum DSChannelEvent {
    Binary(Vec<u8>),
    Text(String),
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

    let server_socket = get_socket(&backend_servers, &server_config_db, 3).await;

    if server_socket.is_none() {
        eprintln!("[client_handler] No upstream server available! Closing connection.");
        // Gracefully close the client connection
        let _ = client_websocket
            .close(Some(CloseFrame {
                code: CloseCode::Error,
                reason: "No upstream server available!".into(),
            }))
            .await;
        return;
    }

    // Split the client websocket
    let (client_writer, mut client_reader) = client_websocket.split();

    // Add the client writer to a Mutex
    let client_writer = Arc::new(Mutex::new(client_writer));
    let server_ship_abi: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let last_request: Arc<Mutex<Option<GetBlocksRequest>>> = Arc::new(Mutex::new(None));

    // Get the server WebSocket reader and writer
    let (server_writer, server_reader) = server_socket.unwrap().split();
    // Add the server reader and writer to a Mutex
    let server_reader = Arc::new(Mutex::new(server_reader));
    let server_writer = Arc::new(Mutex::new(server_writer));

    // Store the server's ABI
    let shared_abieos_arc = shared_abieos.clone();
    {
        let ship_abi_arc = server_ship_abi.clone();
        let server_reader_arc = server_reader.clone();
        let mut server_reader = server_reader_arc.lock().await;
        if let Some(Ok(Message::Text(text))) = server_reader.next().await {
            let abieos = shared_abieos_arc.lock().await;
            abieos
                .set_abi_json("0", text.clone())
                .expect("Error setting ABI");
            let mut server_ship_abi = ship_abi_arc.lock().await;
            *server_ship_abi = Some(text);
        } else {
            eprintln!("[client_handler] Error reading first message from server");
            return;
        }
    }

    // Client loop on a sub-task
    let last_request_arc = last_request.clone();
    let server_writer_arc = server_writer.clone();
    let shared_abieos_arc = shared_abieos.clone();

    // Forward messages from the client to the server, with request inspection
    spawn(async move {
        while let Some(Ok(msg)) = client_reader.next().await {
            // println!("[client_handler] Received message from client: {:?}", msg);

            // decode the message
            if let Message::Binary(bin_msg) = &msg {
                let result_message = deserialize_result(&bin_msg);

                let variant: Option<u8> = match result_message.get("variant") {
                    Some(ZCDValues::U8(v)) => Some(v),
                    _ => None,
                };

                if let Some(v) = variant {
                    if let Some(ZCDValues::Bytes(data)) = result_message.get("data") {
                        if v == 1 {
                            // deserialize the request with abieos + serde
                            let abieos = shared_abieos_arc.lock().await;
                            let request = abieos
                                .bin_to_json("0", "get_blocks_request_v0", data)
                                .expect("[ABIEOS] Error deserializing request");

                            let request: GetBlocksRequest = serde_json::from_str(request.as_str())
                                .expect("[SERDE] Error deserializing request");

                            let mut last_request = last_request_arc.lock().await;
                            *last_request = Some(request);
                        }
                    }
                }
            }

            let mut server_writer = server_writer_arc.lock().await;
            server_writer
                .send(msg)
                .await
                .expect("Error sending message to server");
        }

        println!("Client stream ended");
    });

    // Send the server's ABI to the client
    let ship_abi_arc = server_ship_abi.clone();
    {
        let server_ship_abi = ship_abi_arc.lock().await;
        let client_writer_clone = client_writer.clone();
        match &*server_ship_abi {
            Some(abi) => {
                println!("[client_handler] Sending ABI to client...");
                let mut client_writer = client_writer_clone.lock().await;
                client_writer
                    .send(Message::Text(abi.clone()))
                    .await
                    .expect("Error sending ABI to client");
            }
            None => {}
        }
    }

    // create channel to async handle deserialization for forwarded blocks from server to client
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DSChannelEvent>(32);
    let (forward_tx, mut forward_rx) = tokio::sync::mpsc::channel::<Option<Vec<u8>>>(32);

    let shared_abieos_arc = shared_abieos.clone();
    let last_request_arc = last_request.clone();
    spawn(async move {
        let mut last_block: u32 = 0;
        let mut saved_message: Box<Vec<u8>> = Box::new(vec![]);
        let mut hold_message = false;
        loop {
            match rx.recv().await {
                Some(DSChannelEvent::Text(txt)) => {
                    if txt == "update_last_request".to_string() {
                        let mut last_request = last_request_arc.lock().await;
                        if let Some(req) = &*last_request {
                            let mut updated_req = req.clone();
                            updated_req.start_block_num = last_block + 1;
                            *last_request = Some(updated_req);
                            hold_message = true;
                        }
                    } else if txt == "forward_limited".to_string() {
                        // send message to client
                        let last_request = last_request_arc.lock().await;
                        if let Some(req) = &*last_request {
                            println!(
                                "last_block: {} | req.start_block_num: {}",
                                last_block, req.start_block_num
                            );

                            // send None to client if the last block is less than the start block
                            if last_block >= req.start_block_num {
                                forward_tx
                                    .send(Some(*saved_message.clone()))
                                    .await
                                    .expect("Error sending message to channel");
                            } else {
                                forward_tx
                                    .send(None)
                                    .await
                                    .expect("Error sending message to channel");
                                println!("Ignored block number: {}", last_block);
                            }
                        }
                    }
                }
                Some(DSChannelEvent::Binary(msg)) => {
                    if hold_message {
                        saved_message = Box::new(msg.clone());
                    }

                    // use zero copy deserialization
                    let result = deserialize_result(&msg);
                    match result.get("variant") {
                        Some(ZCDValues::U8(v)) => {
                            if v == 1 {
                                let data = result.get("data").unwrap();
                                if let ZCDValues::Bytes(data) = data {
                                    let abieos = shared_abieos_arc.lock().await;

                                    // decode block result
                                    let result = abieos
                                        .bin_to_json("0", "get_blocks_result_v0", data)
                                        .expect("[ABIEOS] Error deserializing result");

                                    // parse to object
                                    let block_result: serde_json::Value =
                                        serde_json::from_str(&result.as_str())
                                            .expect("[SERDE] Error deserializing result");

                                    // inspect the result
                                    last_block = block_result["this_block"]["block_num"]
                                        .as_u64()
                                        .take()
                                        .unwrap()
                                        as u32;

                                    // println!("Last Received Block: {}", &last_block);
                                }
                            }
                        }
                        _ => {}
                    };
                }
                None => {}
            }
        }
    });

    // Server loop
    let client_writer_arc = client_writer.clone();
    let server_reader_arc = server_reader.clone();
    let ship_abi_arc = server_ship_abi.clone();
    let shared_abieos_arc = shared_abieos.clone();

    let mut client_writer = client_writer_arc.lock().await;
    let mut server_reader = server_reader_arc.lock().await;

    let mut restarting = false;

    let tx1 = tx.clone();
    loop {
        // Keep forwarding messages from the server to the client
        let mut client_down = false;
        while let Some(Ok(msg)) = server_reader.next().await {
            match msg.clone() {
                Message::Binary(bin_data) => {
                    tx1.send(DSChannelEvent::Binary(bin_data))
                        .await
                        .expect("Error sending message to channel");
                }
                _ => {}
            }

            let mut final_msg = msg;

            if restarting {
                tx1.send(DSChannelEvent::Text(String::from("forward_limited")))
                    .await
                    .expect("Error sending message to channel");
                let processed_data = forward_rx.recv().await;
                if let Some(Some(data)) = processed_data {
                    final_msg = Message::Binary(data);
                    restarting = false;
                }
            }

            match client_writer.send(final_msg).await {
                Err(e) => {
                    eprintln!("[client_handler] Error sending message to client: {}", e);
                    client_down = true;
                    break;
                }
                _ => {}
            }
        } // end of server reader loop

        // if the client closed the connection, break the final loop
        if client_down {
            break;
        }

        tx1.send(DSChannelEvent::Text("update_last_request".to_string()))
            .await
            .expect("Error sending update_last_request to channel");

        println!("[client_handler] Server stream ended");

        // now we must select another server to reconnect
        let server_socket = get_socket(&backend_servers, &server_config_db, 3).await;
        if server_socket.is_none() {
            eprintln!("[client_handler] No upstream server available! Closing connection.");
            break;
        }

        // Re-split and update mutexes
        let mut server_writer = server_writer.lock().await;
        let (writer, reader) = server_socket.unwrap().split();
        *server_reader = reader;
        *server_writer = writer;

        // Get the first message from the server
        let first_message = server_reader.next().await;
        if let Some(Ok(Message::Text(text))) = first_message {
            // Use abieos to set the ABI and confirm that the server response is valid
            let abieos = shared_abieos_arc.lock().await;
            match abieos.set_abi_json("0", text.clone()) {
                Ok(_) => {
                    let mut server_ship_abi = ship_abi_arc.lock().await;
                    *server_ship_abi = Some(text);
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

        // Sleep for 1 second before resuming
        sleep(Duration::from_millis(100)).await;

        // Replay the last request
        let last_request = last_request.lock().await;
        if let Some(req) = &*last_request {
            // serialize the request
            let request_payload = ("get_blocks_request_v0", req);
            let json = serde_json::to_string(&request_payload).unwrap();
            // encode to ship format
            let abieos = shared_abieos_arc.lock().await;
            match abieos.json_to_bin("0", "request", json) {
                Ok(serialized) => {
                    let msg = Message::Binary(serialized);
                    // send the message to the server
                    server_writer
                        .send(msg)
                        .await
                        .expect("Error sending message to server");
                }
                Err(e) => {
                    eprintln!("[ABIEOS] Error serializing request: {}", e);
                }
            }
        }

        println!("Reconnected to the server");
        restarting = true;
    }
}
