import WebSocket from "ws";
import {Abieos} from "@eosrio/node-abieos";

let ws: WebSocket;

let shipAbi: any;

const abieos = Abieos.getInstance();

function send(ws: WebSocket, message: object) {
    const hexString = abieos.jsonToHex('SHIP_ABI', 'request', message);
    const buffer = Buffer.from(hexString, "hex");
    ws.send(buffer);
}

let live = false;

let lastBlockNum = 0;

function connect() {

    // Connect to the load balancer
    ws = new WebSocket('ws://localhost:17000');

    ws.addEventListener('open', (event) => {
        console.log('Connected to server.. Waiting for ABI');
    });

    ws.addEventListener('message', (message) => {
        if (message.type === 'message') {
            if (!shipAbi) {
                shipAbi = JSON.parse(message.data.toString());
                abieos.loadAbi('SHIP_ABI', shipAbi);
                console.log('Received ABI');
                send(ws, ['get_status_request_v0', {}]);
            } else {
                if (message.data) {

                    let decoded;

                    try {
                        decoded = abieos.binToJson('SHIP_ABI', 'result', message.data as Buffer);
                    } catch (e) {
                        console.log('Error decoding message:', e);
                        console.log(message.data);
                        return;
                    }

                    if (decoded && decoded.length == 2) {
                        const msgType = decoded[0];
                        switch (msgType) {
                            case 'get_status_result_v0':
                                console.log('Received status', decoded[1]);
                                break;
                            case 'get_blocks_result_v0':
                                // process block data

                                // deserialize block data
                                const result = decoded[1] as {
                                    head: { block_num: number; block_id: string; };
                                    block: string;
                                };

                                if (result.block && result.head) {
                                    const blockData = abieos.hexToJson('SHIP_ABI', 'signed_block', result.block);
                                    const current_block = result.head.block_num;
                                    console.log(`Block time: ${blockData['timestamp']} | Block Number: ${current_block} | Block ID: ${result.head.block_id}`);
                                    if (lastBlockNum == current_block) {
                                        console.log('Duplicate block received, ignoring..');
                                    }
                                    lastBlockNum = current_block;
                                }

                                // ACK block
                                send(ws, ['get_blocks_ack_request_v0', {num_messages: 1}]);
                                break;
                        }

                        if (!live) {
                            live = true;
                            send(ws, ['get_blocks_request_v0', {
                                start_block_num: decoded[1].head.block_num,
                                end_block_num: 0xffffffff,
                                max_messages_in_flight: 1,
                                have_positions: [],
                                irreversible_only: false,
                                fetch_block: true,
                                fetch_traces: false,
                                fetch_deltas: false
                            }]);
                        }
                    }
                }
            }
        }
    });

    ws.addEventListener('error', (error) => {
        console.log('Error:', error);
    });

    ws.addEventListener('close', (event) => {
        console.log("Server closed the connection:", event.code, event.reason);
        shipAbi = undefined;
        live = false;
        console.log('Reconnecting in 3 seconds..');
        setTimeout(() => {
            connect();
        }, 3000);
    });
}

connect();