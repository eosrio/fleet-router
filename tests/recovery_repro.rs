//! Opt-in reproduction of the v0.7.0 multi-listener recovery report.
//! `cargo test --test recovery_repro -- --ignored --nocapture`
//! A failure of the final head-lag assertion is the expected v0.7.0 result.
mod common;

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use mock_ship::client::{ack, blocks, BlocksReq};
use mock_ship::{MockShipConfig, MockShipHandle, MockShipServer};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const LISTENERS: usize = 14;
const TESTNET: usize = 1;
const POLL_MS: u64 = 500;
const RECONNECT_MS: u64 = 300;

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

struct Reader {
    blocks: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    error: Arc<Mutex<Option<String>>>,
    task: JoinHandle<()>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Reader {
    async fn start(port: u16, head: u32, backfill: bool, mib_per_second: f64) -> Self {
        let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{port}"))
            .await
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(5), ws.next()).await.unwrap(),
            Some(Ok(Message::Text(_)))
        ));
        let start = if backfill { 100 } else { head };
        ws.send(Message::Binary(
            blocks(&BlocksReq {
                start,
                end: u32::MAX,
                max_in_flight: 64,
                fetch_block: backfill,
                ..Default::default()
            })
            .into(),
        ))
        .await
        .unwrap();
        let mut result = Self {
            blocks: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
            error: Arc::new(Mutex::new(None)),
            task: tokio::spawn(async {}),
        };
        let (count, bytes, error) = (
            result.blocks.clone(),
            result.bytes.clone(),
            result.error.clone(),
        );
        let task = tokio::spawn(async move {
            let mut expected = start;
            let mut credit = 0;
            let mut received_bytes = 0_u64;
            let pace_epoch = tokio::time::Instant::now();
            let outcome: Result<(), String> = async {
                loop {
                    let message = timeout(Duration::from_secs(10), ws.next())
                        .await
                        .map_err(|_| "reader stalled for 10 seconds".to_string())?
                        .ok_or_else(|| "client socket ended".to_string())?
                        .map_err(|e| e.to_string())?;
                    match message {
                        Message::Binary(frame)
                            if frame.first() == Some(&1) && frame.len() >= 78 && frame[73] == 1 =>
                        {
                            let block = u32::from_le_bytes(frame[74..78].try_into().unwrap());
                            if block != expected {
                                return Err(format!("expected block {expected}, got {block}"));
                            }
                            expected += 1;
                            count.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(frame.len() as u64, Ordering::Relaxed);
                            received_bytes += frame.len() as u64;
                            credit += 1;
                            if credit == 32 {
                                if backfill && mib_per_second > 0.0 {
                                    let due = pace_epoch
                                        + Duration::from_secs_f64(
                                            received_bytes as f64 / (mib_per_second * 1048576.0),
                                        );
                                    tokio::time::sleep_until(due).await;
                                }
                                ws.send(Message::Binary(ack(credit).into()))
                                    .await
                                    .map_err(|e| e.to_string())?;
                                credit = 0;
                            }
                        }
                        Message::Close(frame) => return Err(format!("client closed: {frame:?}")),
                        _ => {}
                    }
                }
            }
            .await;
            if let Err(reason) = outcome {
                *error.lock().unwrap() = Some(reason);
            }
        });
        result.task = task;
        result
    }

    fn sample(&self) -> Value {
        json!({"blocks":self.blocks.load(Ordering::Relaxed), "bytes":self.bytes.load(Ordering::Relaxed),
            "error":self.error.lock().unwrap().clone(), "reader_finished":self.task.is_finished()})
    }
}

struct Pool {
    a: Option<MockShipHandle>,
    b: MockShipHandle,
    config: MockShipConfig,
    port_a: u16,
    endpoints: [String; 2],
    requests: [Arc<AtomicUsize>; 2],
    head: u32,
}

impl Pool {
    async fn new(index: usize) -> Self {
        let head = if index == TESTNET { 10_000 } else { 1_000_000 };
        let requests = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
        let config = MockShipConfig {
            chain_id: [index as u8 + 1; 32],
            block_data_size: if index == 0 { 65536 } else { 4 },
            ..MockShipConfig::live(1, head)
        };
        let a = MockShipServer::new(MockShipConfig {
            status_requests: Some(requests[0].clone()),
            ..config.clone()
        })
        .await;
        let b = MockShipServer::new(MockShipConfig {
            status_requests: Some(requests[1].clone()),
            ..config.clone()
        })
        .await;
        let endpoints = [a.endpoint(), b.endpoint()];
        Self {
            port_a: a.addr().port(),
            a: Some(a.start()),
            b: b.start(),
            config,
            endpoints,
            requests,
            head,
        }
    }

    fn advance(&mut self) {
        self.head += 2;
        if let Some(a) = &self.a {
            a.set_head(self.head, self.head - 10);
        }
        self.b.set_head(self.head, self.head - 10);
    }
}

struct Capture {
    dir: PathBuf,
    samples: File,
    epoch: Instant,
}

/// Keep the router's CPU separate from the mock servers and reader tasks.
#[cfg(target_os = "linux")]
struct ProcessCpu {
    pid: u32,
    ticks: f64,
    page_size: u64,
    file: File,
}

#[cfg(target_os = "linux")]
impl ProcessCpu {
    fn new(pid: u32, dir: &std::path::Path) -> Self {
        let getconf = |key: &str| {
            let output = std::process::Command::new("getconf")
                .arg(key)
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        Self {
            pid,
            ticks: getconf("CLK_TCK") as f64,
            page_size: getconf("PAGESIZE"),
            file: File::create(dir.join("process-cpu.jsonl")).unwrap(),
        }
    }

    fn sample(&mut self) {
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.pid)).unwrap();
        let (_, fields) = stat
            .rsplit_once(") ")
            .expect("process stat command boundary");
        let fields: Vec<&str> = fields.split_whitespace().collect();
        let field = |index: usize| fields[index].parse::<u64>().unwrap();
        let sample = json!({"t_ms":unix_ms(), "cpu_seconds":(field(11)+field(12)) as f64/self.ticks,
            "working_set_bytes":field(21)*self.page_size,"threads":field(17)});
        writeln!(self.file, "{sample}").unwrap();
        self.file.flush().unwrap();
    }
}

impl Capture {
    fn new() -> Self {
        let dir = std::env::var_os("FLEET_ROUTER_REPRO_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("target/recovery-repro/{}", unix_ms())));
        fs::create_dir_all(&dir).unwrap();
        Self {
            samples: File::create(dir.join("samples.jsonl")).unwrap(),
            dir,
            epoch: Instant::now(),
        }
    }

    fn write(&mut self, mut sample: Value) {
        sample["t_ms"] = json!(unix_ms());
        sample["elapsed_ms"] = json!(self.epoch.elapsed().as_millis() as u64);
        writeln!(self.samples, "{sample}").unwrap();
        self.samples.flush().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multi-listener recovery and throughput capture; run explicitly"]
async fn recovery_with_fourteen_listeners_and_eighteen_readers() {
    let mut capture = Capture::new();
    let timing_scale: u64 = std::env::var("FLEET_ROUTER_REPRO_TIMING_SCALE")
        .ok()
        .map(|v| v.parse().expect("timing scale must be an integer"))
        .unwrap_or(1);
    assert!((1..=10).contains(&timing_scale));
    let poll_ms = POLL_MS * timing_scale;
    let restart = std::env::var_os("FLEET_ROUTER_REPRO_NO_RESTART").is_none();
    let offered_mib_s: f64 = std::env::var("FLEET_ROUTER_REPRO_MIB_S")
        .ok()
        .map(|v| {
            v.parse()
                .expect("FLEET_ROUTER_REPRO_MIB_S must be a number")
        })
        .unwrap_or(60.0);
    assert!(offered_mib_s.is_finite() && offered_mib_s >= 0.0);
    let mut pools = Vec::new();
    for i in 0..LISTENERS {
        pools.push(Pool::new(i).await);
    }
    let listeners: Vec<Value> = pools
        .iter()
        .enumerate()
        .map(|(i, p)| {
            json!({
                "name":format!("network-{i}"),"listen_port":0,
                "servers":[{"name":"primary","endpoint":p.endpoints[0],"enabled":true,"priority":0},
                           {"name":"backup","endpoint":p.endpoints[1],"enabled":true,"priority":1}]
            })
        })
        .collect();
    let router = common::RouterProcess::start_recording(json!({
        "listen_address":"127.0.0.1", "listeners":listeners,
        "metrics_address":"127.0.0.1","metrics_port":0,"dashboard":{"enabled":true},
        "upstream_status_ms":poll_ms,"upstream_monitoring_ms":poll_ms,"upstream_reconnect_ms":RECONNECT_MS*timing_scale,
    }), Some(&capture.dir)).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let status_url = format!("{base}/api/v1/status");
    for _ in 0..100 {
        let status: Value = http
            .get(&status_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["totals"]["upstreams_up"] == 28 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let metadata = json!({"router_pid":router.pid(),"dashboard":format!("{base}/ui/"),
        "listeners":14,"upstreams":28,"readers":18,"poll_ms":poll_ms,"reconnect_ms":RECONNECT_MS*timing_scale,"timing_scale":timing_scale,"started_ms":unix_ms(),
        "restart_injected":restart,"offered_mainnet_mib_s":offered_mib_s,
        "workload":"two 64 KiB backfill readers on network-0; sixteen header readers on other networks; restart network-1 primary"});
    fs::write(
        capture.dir.join("metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    println!("REPRO {metadata}\nEvidence: {}", capture.dir.display());
    #[cfg(target_os = "linux")]
    let mut process_cpu = ProcessCpu::new(router.pid(), &capture.dir);
    let mut readers = Vec::new();
    for (i, pool) in pools.iter().enumerate() {
        let count = if i < 4 { 2 } else { 1 };
        for _ in 0..count {
            readers.push(
                Reader::start(
                    router.listener_ports[i],
                    pool.head,
                    i == 0,
                    offered_mib_s / 2.0,
                )
                .await,
            );
        }
    }
    assert_eq!(readers.len(), 18);
    let mut lagged = [0_usize; 2];
    let mut raw_lagged = [0_usize; 2];
    let mut baseline_lag = 0;
    let mut phase_traffic = Vec::new();
    for (phase, ticks) in [("baseline", 20), ("down", 20), ("returned", 60)] {
        if phase == "down" {
            // Reconnect deadlines start when the monitor loses its socket.
            // Offset the outage, rather than just the server's return, from
            // the healthy monitor's polling boundary. Controls wait too.
            let count = pools[TESTNET].requests[1].load(Ordering::Relaxed);
            timeout(Duration::from_millis(poll_ms * 2), async {
                while pools[TESTNET].requests[1].load(Ordering::Relaxed) == count {
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("backup monitor stopped polling");
            sleep(Duration::from_millis(130 * timing_scale)).await;
            if restart {
                let primary = pools[TESTNET].a.take().unwrap();
                primary.close_sessions();
                primary.shutdown().await;
            }
        } else if phase == "returned" && restart {
            let p = &mut pools[TESTNET];
            p.a = Some(
                MockShipServer::new(MockShipConfig {
                    port: p.port_a,
                    head_block: p.head,
                    lib_block: p.head - 10,
                    status_requests: Some(p.requests[0].clone()),
                    ..p.config.clone()
                })
                .await
                .start(),
            );
        }
        capture.write(json!({"kind":"phase","phase":phase}));
        let started = Instant::now();
        let bytes_before: u64 = readers
            .iter()
            .take(2)
            .map(|r| r.bytes.load(Ordering::Relaxed))
            .sum();
        for _ in 0..ticks {
            for pool in &mut pools {
                pool.advance();
            }
            sleep(Duration::from_millis(100 * timing_scale)).await;
            let status: Value = http
                .get(&status_url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            #[cfg(target_os = "linux")]
            process_cpu.sample();
            let testnet = &status["listeners"][TESTNET];
            let upstreams = testnet["upstreams"].as_array().unwrap();
            let best = upstreams
                .iter()
                .filter_map(|u| u["head_block"].as_u64())
                .max()
                .unwrap();
            let raw_sample_gap: Vec<u64> = upstreams
                .iter()
                .map(|u| best - u["head_block"].as_u64().unwrap())
                .collect();
            // Old tagged binaries omit the new field. Explicit null means
            // the fixed UI shows an unavailable comparison, not a raw delta.
            let display_lag: Vec<Option<u64>> = upstreams
                .iter()
                .enumerate()
                .map(|(i, u)| {
                    u.get("head_lag_blocks")
                        .map_or(Some(raw_sample_gap[i]), Value::as_u64)
                })
                .collect();
            let both_up = upstreams.iter().all(|u| u["online"] == true);
            if both_up {
                // Do not let an absent comparison masquerade as a fix.
                if upstreams[0].get("head_lag_blocks").is_some() {
                    assert!(
                        display_lag.iter().all(Option::is_some),
                        "online nodes must have measured lag: {testnet}"
                    );
                }
                if phase == "baseline" && display_lag.iter().any(|lag| lag.is_some_and(|n| n > 0)) {
                    baseline_lag += 1;
                }
                if phase == "returned" {
                    for i in 0..2 {
                        if display_lag[i].is_some_and(|n| n > 0) {
                            lagged[i] += 1;
                        }
                        if raw_sample_gap[i] > 0 {
                            raw_lagged[i] += 1;
                        }
                    }
                }
            }
            capture.write(json!({"kind":"sample","phase":phase,"status":status,"testnet_display_lag":display_lag,"testnet_raw_sample_gap":raw_sample_gap,
                "testnet_actual_heads":[pools[TESTNET].a.as_ref().map(|a|a.clock().head()),pools[TESTNET].b.clock().head()],
                "testnet_poll_counts":pools[TESTNET].requests.iter().map(|r|r.load(Ordering::Relaxed)).collect::<Vec<_>>(),
                "readers":readers.iter().map(Reader::sample).collect::<Vec<_>>() }));
            assert_eq!(
                status["totals"]["sessions"],
                18,
                "client connection count changed during {phase}; inspect {}",
                capture.dir.display()
            );
            for reader in &readers {
                assert!(
                    reader.error.lock().unwrap().is_none(),
                    "reader failed: {}",
                    reader.sample()
                );
            }
        }
        let bytes_after: u64 = readers
            .iter()
            .take(2)
            .map(|r| r.bytes.load(Ordering::Relaxed))
            .sum();
        phase_traffic.push(json!({"phase":phase,"mainnet_mib_s":(bytes_after-bytes_before) as f64/started.elapsed().as_secs_f64()/1048576.0}));
    }
    fs::write(
        capture.dir.join("metrics.txt"),
        http.get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    fs::write(
        capture.dir.join("sessions.json"),
        http.get(format!("{base}/api/v1/sessions"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    let summary = json!({"restart_injected":restart,"offered_mainnet_mib_s":offered_mib_s,"baseline_false_lag_samples":baseline_lag,"returned_false_lag_samples":lagged,"returned_raw_sample_gap_samples":raw_lagged,
        "phase_traffic":phase_traffic,"reader_errors":readers.iter().map(|r|r.error.lock().unwrap().clone()).collect::<Vec<_>>(),
        "cpu_issue":"not asserted; inspect process-cpu.jsonl separately"});
    fs::write(
        capture.dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    println!("REPRO SUMMARY {summary}");
    if restart {
        assert!(
            raw_lagged.iter().any(|n| *n > 0),
            "polling offset was not exercised"
        );
        assert_eq!(
            lagged,
            [0, 0],
            "dashboard false lag reproduced on synchronized nodes after recovery; evidence: {}",
            capture.dir.display()
        );
    }
}
