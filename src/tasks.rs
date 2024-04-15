use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::models::{Server, ServerState, StaticConfig};
use crate::zcd;
use crate::zcd::ZCDValues;

pub async fn state_monitoring_loop(
    server_state_db_clone: Arc<Mutex<HashMap<String, ServerState>>>,
    interval_ms: u64,
) -> () {
    let mut last_block: HashMap<String, u32> = HashMap::new();
    let mut stale_counter: HashMap<String, u32> = HashMap::new();

    loop {
        sleep(Duration::from_millis(interval_ms)).await;
        let mut server_state_db = server_state_db_clone.lock().await;
        let mut updated = 0;
        for (server, state) in server_state_db.iter_mut() {
            if !last_block.contains_key(server) {
                last_block.insert(server.clone(), 0);
                stale_counter.insert(server.clone(), 0);
            }

            let last_block_number = last_block.get_mut(server).unwrap();
            let stale_counter = stale_counter.get_mut(server).unwrap();

            if state.chain_state_end_block > *last_block_number {
                *last_block_number = state.chain_state_end_block;
                *stale_counter = 0;
                updated += 1;
                // println!("Server {}: {} - Last State Block {}", index, server, *last_block_number);
            } else {
                *stale_counter += 1;
                if *stale_counter > 10 {
                    eprintln!("Server: {} - No New Blocks", server);
                }
            }
        }

        if updated > 0 {
            println!("[{}] - Last Blocks: {:?}", Utc::now(), last_block);
        }
    }
}

pub async fn send_status_loop(
    sender: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    interval_ms: u64,
) -> () {
    loop {
        sleep(Duration::from_millis(interval_ms)).await;
        let message = Message::Binary(vec![0]);
        let mut s = sender.lock().await;
        match s.send(message).await {
            Ok(_) => (),
            Err(e) => {
                println!("Error sending status request: {}", e);
                break;
            }
        }
    }
}

pub async fn monitoring_connection(
    server: Server,
    static_config: StaticConfig,
    server_state_db_clone: Arc<Mutex<HashMap<String, ServerState>>>,
    abieos_arc_clone: Arc<Mutex<Abieos>>,
) -> () {
    // println!("{:?}", static_config);

    let mut ship_abi: Option<String> = None;
    let mut server_closed = false;

    loop {
        // Get websocket connection with the upstream backend
        let websocket = match connect_async(server.ws_url()).await {
            Ok((websocket, _)) => {
                // println!("{:?}", resp);
                websocket
            }
            Err(_) => {
                eprintln!("Error connecting to server");
                println!(
                    "Retrying connection in {} ms...",
                    static_config.upstream_reconnect_ms
                );
                sleep(Duration::from_millis(static_config.upstream_reconnect_ms)).await;
                continue;
            }
        };

        println!("Connected to server: {}", server.endpoint);
        let (sender, mut receiver) = websocket.split();
        let sender = Arc::new(Mutex::new(sender));

        let s1 = sender.clone();
        tokio::spawn(send_status_loop(s1, static_config.upstream_status_ms));

        let s2 = sender.clone();
        loop {
            let msg = match receiver.next().await {
                Some(Ok(msg)) => msg,
                None => {
                    // if the server closed the connection, break the inner loop but keep trying to reconnect
                    println!("{} disconnected!", server.endpoint);
                    ship_abi = None;

                    // mark the server as offline
                    let server_state_db = &mut server_state_db_clone.lock().await;
                    let state = server_state_db.get_mut(&server.endpoint).unwrap();
                    state.online = false;
                    break;
                }
                _ => {
                    println!("Error reading next message!");
                    break;
                }
            };
            // println!("{:?}",msg);
            handle_monitoring_msg(
                msg,
                &mut server_closed,
                &mut ship_abi,
                &abieos_arc_clone,
                &s2,
                &server_state_db_clone,
                &server,
            )
                .await;

            if server_closed {
                break;
            }
        } // end of receiver loop

        // if the server closed the connection, break the loop
        if server_closed {
            break;
        }

        println!(
            "Retrying connection in {} ms...",
            static_config.upstream_reconnect_ms
        );
        sleep(Duration::from_millis(static_config.upstream_reconnect_ms)).await;
    }
}

async fn handle_monitoring_msg(
    message: Message,
    server_closed: &mut bool,
    ship_abi: &mut Option<String>,
    abieos_arc_clone: &Arc<Mutex<Abieos>>,
    sender: &Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    server_state_db_clone: &Arc<Mutex<HashMap<String, ServerState>>>,
    server: &Server,
) -> () {
    match message {
        Message::Text(msg) => {
            if ship_abi.is_none() {
                *ship_abi = Some(msg);
                let abieos = abieos_arc_clone.lock().await;
                println!("Abieos Context: {:?}", abieos.context.unwrap());
                match abieos.set_abi_json_native(0u64, ship_abi.clone().unwrap()) {
                    Ok(x) => {
                        if x {
                            let message = Message::Binary(vec![0]);
                            let mut s = sender.lock().await;
                            s.send(message).await.unwrap_or_else(|e| {
                                eprintln!("Error sending message: {}", e);
                            });
                        }
                    }
                    Err(_) => {
                        println!("Error setting ABI");
                    }
                };
            } else {
                println!("Received unexpected text message from server");
            }
        }
        Message::Binary(bin_msg) => {
            let result_message = zcd::deserialize_result(&bin_msg);
            let variant = result_message.get("variant").unwrap();
            match variant {
                ZCDValues::U8(v) => {
                    let data = result_message.get("data").unwrap();
                    match data {
                        ZCDValues::Bytes(bytes) => {
                            if v == 0 {
                                let result = zcd::deserialize_status_result(&bytes);
                                let head_block_num: u32 =
                                    result.get("head_block_num").unwrap().into();
                                if head_block_num > 0 {
                                    let server_state_db = &mut server_state_db_clone.lock().await;
                                    let state = server_state_db.get_mut(&server.endpoint).unwrap();
                                    state.enabled = true;
                                    state.online = true;
                                    state.trace_begin_block =
                                        result.get("trace_begin_block").unwrap().into();
                                    state.trace_end_block =
                                        result.get("trace_end_block").unwrap().into();
                                    state.chain_state_begin_block =
                                        result.get("chain_state_begin_block").unwrap().into();
                                    state.chain_state_end_block =
                                        result.get("chain_state_end_block").unwrap().into();
                                }
                            }
                        }
                        _ => {
                            eprintln!("Received unexpected type for variant");
                        }
                    }
                }
                _ => {
                    eprintln!("Received unexpected binary message");
                }
            }
        }
        Message::Close(_) => {
            eprintln!("Received close message from server");
            *server_closed = true;
        }
        _ => {}
    }
}
