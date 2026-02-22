// Fleet Router Test Chain — High-Throughput Load Generator
// Bun HTTP server that pushes eosio.token transfers using @wharfkit/antelope
// REST API: GET /status, POST /start, POST /stop
// Supports 15k+ TPS burst via concurrent async HTTP requests

import { APIClient, PrivateKey, Action, Transaction, SignedTransaction, Serializer, ABI } from "@wharfkit/antelope"

const NODEOS_URL = process.env.NODEOS_URL || "http://nodeos-1:8888"
const PEER_URLS = (process.env.PEER_URLS || "http://nodeos-2:8888,http://nodeos-3:8888").split(",")
const PORT = parseInt(process.env.PORT || "3000")

// Default eosio development key
const SIGNING_KEY = PrivateKey.from("5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3")

const ACCOUNTS = [
    "testaccountb", "testaccountc", "testaccountd", "testaccounte",
    "testaccountf", "testaccountg", "testaccounth", "testaccounti",
    "testaccountj",
]

// Cached ABI for eosio.token — avoid fetching every time
let tokenABI: ABI | null = null

// State
let running = false
let txCount = 0
let txErrors = 0
let targetTps = 5
let concurrency = 50
let abortController: AbortController | null = null

// --- Chain interaction ---

async function getChainInfo(url: string): Promise<any | null> {
    try {
        const res = await fetch(`${url}/v1/chain/get_info`, { signal: AbortSignal.timeout(2000) })
        if (!res.ok) return null
        return await res.json()
    } catch {
        return null
    }
}

async function checkSync(): Promise<{ synced: boolean; producer: number; peers: number[] }> {
    const producerInfo = await getChainInfo(NODEOS_URL)
    if (!producerInfo) return { synced: false, producer: 0, peers: [] }

    const peerHeads: number[] = []
    for (const url of PEER_URLS) {
        const info = await getChainInfo(url)
        peerHeads.push(info?.head_block_num || 0)
    }

    const synced = peerHeads.every(h => Math.abs(h - producerInfo.head_block_num) <= 2)
    return { synced, producer: producerInfo.head_block_num, peers: peerHeads }
}

async function fetchTokenABI(): Promise<ABI> {
    if (tokenABI) return tokenABI
    const res = await fetch(`${NODEOS_URL}/v1/chain/get_abi`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ account_name: "eosio.token" })
    })
    const data = await res.json() as any
    tokenABI = ABI.from(data.abi)
    return tokenABI
}

async function pushTransaction(from: string, to: string, quantity: string, memo: string): Promise<boolean> {
    try {
        // Get chain info for transaction header
        const info = await getChainInfo(NODEOS_URL)
        if (!info) return false

        const abi = await fetchTokenABI()

        // Build action data
        const actionData = Serializer.encode({
            abi,
            type: "transfer",
            object: { from, to, quantity, memo }
        })

        const action = Action.from({
            account: "eosio.token",
            name: "transfer",
            authorization: [{ actor: from, permission: "active" }],
            data: actionData
        })

        // Build transaction
        const expiration = new Date(new Date(info.head_block_time + "Z").getTime() + 30000)
            .toISOString().slice(0, -1)
        const refBlockNum = info.last_irreversible_block_num & 0xFFFF
        const refBlockPrefix = parseInt(
            info.last_irreversible_block_id.substr(16, 8).match(/../g)!.reverse().join(""),
            16
        )

        const transaction = Transaction.from({
            expiration,
            ref_block_num: refBlockNum,
            ref_block_prefix: refBlockPrefix,
            actions: [action],
        })

        // Sign
        const signature = SIGNING_KEY.signDigest(
            transaction.signingDigest(info.chain_id)
        )

        const signed = SignedTransaction.from({
            ...transaction,
            signatures: [signature],
        })

        // Push
        const res = await fetch(`${NODEOS_URL}/v1/chain/push_transaction`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                signatures: signed.signatures.map(s => String(s)),
                compression: 0,
                packed_context_free_data: "",
                packed_trx: Serializer.encode({ object: transaction, type: Transaction }).hexString,
            }),
            signal: AbortSignal.timeout(5000),
        })

        return res.ok
    } catch {
        return false
    }
}

// --- Load generation with concurrency ---

async function generateLoad(signal: AbortSignal) {
    const intervalMs = Math.max(1, Math.floor(1000 / targetTps))
    let idx = 0
    const batchSize = Math.min(concurrency, targetTps)
    const batchInterval = Math.max(1, Math.floor(1000 / (targetTps / batchSize)))

    console.log(`Load generation started: target=${targetTps} TPS, concurrency=${concurrency}, batchSize=${batchSize}, batchInterval=${batchInterval}ms`)

    while (!signal.aborted) {
        // Fire a batch of concurrent requests
        const promises: Promise<boolean>[] = []
        for (let i = 0; i < batchSize && !signal.aborted; i++) {
            const fromIdx = idx % ACCOUNTS.length
            const toIdx = (idx + 1) % ACCOUNTS.length
            const from = ACCOUNTS[fromIdx]
            const to = ACCOUNTS[toIdx]
            const amount = `${(idx % 99) + 1}.0000 SYS`
            const memo = `load-${txCount + i}`
            idx++

            promises.push(pushTransaction(from, to, amount, memo))
        }

        // Await batch results
        const results = await Promise.allSettled(promises)
        for (const result of results) {
            if (result.status === "fulfilled" && result.value) {
                txCount++
            } else {
                txErrors++
            }
        }

        // Wait for next batch
        await new Promise(r => setTimeout(r, batchInterval))
    }

    running = false
    console.log(`Load generation stopped. Total tx: ${txCount}, errors: ${txErrors}`)
}

// --- HTTP Server ---

const server = Bun.serve({
    port: PORT,
    async fetch(req) {
        const url = new URL(req.url)

        if (req.method === "GET" && url.pathname === "/status") {
            const sync = await checkSync()
            return Response.json({
                running,
                txCount,
                txErrors,
                targetTps,
                concurrency,
                accounts: ACCOUNTS.length,
                ...sync,
            })
        }

        if (req.method === "POST" && url.pathname === "/start") {
            if (running) {
                return Response.json({ error: "Already running" }, { status: 409 })
            }

            // Parse options from body
            try {
                const body = await req.json() as any
                if (body.tps && typeof body.tps === "number" && body.tps > 0) {
                    targetTps = Math.floor(body.tps)
                }
                if (body.concurrency && typeof body.concurrency === "number" && body.concurrency > 0) {
                    concurrency = Math.floor(body.concurrency)
                }
            } catch {
                // Use defaults
            }

            // Check sync
            const sync = await checkSync()
            if (!sync.synced) {
                return Response.json({ error: "Nodes not synced", ...sync }, { status: 503 })
            }

            // Pre-cache ABI
            try {
                await fetchTokenABI()
            } catch (e) {
                return Response.json({ error: "Failed to fetch token ABI — is bootstrap done?" }, { status: 503 })
            }

            running = true
            txErrors = 0
            abortController = new AbortController()
            generateLoad(abortController.signal)

            return Response.json({ started: true, targetTps, concurrency, txCount })
        }

        if (req.method === "POST" && url.pathname === "/stop") {
            if (!running) {
                return Response.json({ error: "Not running" }, { status: 409 })
            }

            abortController?.abort()
            await new Promise(r => setTimeout(r, 500))

            return Response.json({ stopped: true, txCount, txErrors })
        }

        return Response.json({ error: "Not found" }, { status: 404 })
    }
})

console.log(`Load generator listening on :${PORT}`)
console.log(`Producer: ${NODEOS_URL}`)
console.log(`Peers: ${PEER_URLS.join(", ")}`)

// Wait for sync on startup
async function waitForSync() {
    console.log("Waiting for all nodes to sync...")
    for (let i = 0; i < 120; i++) {
        const sync = await checkSync()
        if (sync.synced) {
            console.log(`All nodes synced! Producer: ${sync.producer}, Peers: ${sync.peers.join(", ")}`)
            return
        }
        console.log(`  Not synced. Producer: ${sync.producer}, Peers: ${sync.peers.join(", ")}`)
        await new Promise(r => setTimeout(r, 2000))
    }
    console.error("WARNING: Nodes did not sync within 240s")
}

waitForSync()
