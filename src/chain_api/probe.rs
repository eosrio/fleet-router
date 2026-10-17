//! One probe task per upstream chain API.
//!
//! A live API is asked for `get_info` every `probe_ms` (head, LIB, earliest
//! available block, chain_id), and for `get_supported_apis` when it comes up
//! and every minute. A frozen range server's API is asked for
//! `/v1/archive/describe`, and its identity is verified against its own
//! SHiP endpoint ([`Verification`]) before any request goes to it.
//!
//! An API comes up after one good probe and goes down after two failed ones
//! in a row, or at once when it reports another chain. A request that fails
//! in transport also takes it down until its next good probe.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::header::HeaderMap;
use hyper::Method;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::time::{timeout_at, MissedTickBehavior};

use crate::fleet::FleetHub;
use crate::models::Role;
use crate::ship::{BlockPos, Range};

use super::directory::{ApiDirectory, ApiSlot, Verify};
use super::upstream::{Attempt, Call, UpstreamClient};

/// How often a live API's advertised endpoints are asked for again.
const APIS_EVERY: Duration = Duration::from_secs(60);

/// Largest probe answer read.
const MAX_PROBE_BODY: usize = 1024 * 1024;

const GET_INFO: &str = "/v1/chain/get_info";
const SUPPORTED_APIS: &str = "/v1/node/get_supported_apis";
const DESCRIBE: &str = "/v1/archive/describe";
const GET_BLOCK_INFO: &str = "/v1/chain/get_block_info";

/// The parts of `get_info` the router reads.
#[derive(Deserialize)]
struct Info {
    chain_id: String,
    head_block_num: u32,
    last_irreversible_block_num: u32,
    /// Absent on EOSIO 2.0 and FIO nodes.
    #[serde(default)]
    earliest_available_block_num: Option<u32>,
    #[serde(default)]
    server_version_string: Option<String>,
}

/// A frozen range server's `/v1/archive/describe`.
#[derive(Deserialize)]
struct Describe {
    chain_id: String,
    last: Position,
    blocks: Blocks,
    #[serde(default)]
    apis: Option<Vec<String>>,
    #[serde(default)]
    get_block: Option<String>,
    #[serde(default)]
    max_in_flight: Option<u64>,
}

#[derive(Deserialize)]
struct Position {
    num: u32,
    id: String,
}

#[derive(Deserialize)]
struct Blocks {
    begin: u32,
    end: u32,
}

#[derive(Deserialize)]
struct SupportedApis {
    apis: Vec<String>,
}

#[derive(Deserialize)]
struct BlockId {
    id: String,
}

/// 64 hex digits as 32 bytes.
fn hex32(text: &str) -> Option<[u8; 32]> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (byte, pair) in out.iter_mut().zip(bytes.chunks(2)) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        *byte = (hi << 4 | lo) as u8;
    }
    Some(out)
}

/// What every probe task of one listener shares.
pub struct Prober {
    pub listener: Option<Arc<str>>,
    pub client: UpstreamClient,
    pub hub: Arc<FleetHub>,
    pub directory: Arc<ApiDirectory>,
    pub every: Duration,
    /// Bounds each probe request.
    pub timeout: Duration,
    /// `max_in_flight_per_upstream`.
    pub per_upstream: usize,
}

impl Prober {
    /// Probe `slot` every [`Self::every`] until shutdown, starting now.
    pub async fn run(self: Arc<Self>, slot: Arc<ApiSlot>, mut shutdown: broadcast::Receiver<()>) {
        let mut tick = tokio::time::interval(self.every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.recv() => return,
            }
            let probe = async {
                match slot.role {
                    Role::Live => self.probe_live(&slot).await,
                    Role::Frozen => self.probe_frozen(&slot).await,
                }
            };
            tokio::select! {
                _ = probe => {}
                _ = shutdown.recv() => return,
            }
        }
    }

    /// One request; the status and the whole body, or `None` on a transport
    /// failure or when it takes longer than [`Self::timeout`].
    async fn fetch(
        &self,
        slot: &ApiSlot,
        method: Method,
        target: &str,
        body: Bytes,
    ) -> Option<(u16, Bytes)> {
        let call = Call::new(method, target, &HeaderMap::new(), body);
        let request = call.request_for(slot).ok()?;
        let deadline = tokio::time::Instant::now() + self.timeout;
        let Attempt::Response(response) = self.client.send(request, self.timeout).await else {
            return None;
        };
        let status = response.status().as_u16();
        let body = Limited::new(response.into_body(), MAX_PROBE_BODY);
        let body = timeout_at(deadline, body.collect()).await.ok()?.ok()?;
        Some((status, body.to_bytes()))
    }

    /// A 200 answer parsed as `T`.
    async fn fetch_json<T: for<'de> Deserialize<'de>>(
        &self,
        slot: &ApiSlot,
        method: Method,
        target: &str,
        body: Bytes,
    ) -> Option<T> {
        match self.fetch(slot, method, target, body).await? {
            (200, body) => serde_json::from_slice(&body).ok(),
            _ => None,
        }
    }

    /// The chain_id an API must report: the listener's pinned one, else
    /// the one the server's own SHiP endpoint (or the listener's SHiP view)
    /// reported, else the first one any API reported.
    async fn expected_chain_id(&self, slot: &ApiSlot, reported: [u8; 32]) -> [u8; 32] {
        if let Some(pinned) = self.hub.pinned_chain_id {
            return pinned;
        }
        let ship = self
            .hub
            .state
            .lock()
            .await
            .get(&slot.ship_endpoint)
            .and_then(|s| s.chain_id);
        ship.or(self.hub.view().chain_id)
            .unwrap_or_else(|| self.directory.first_chain_id(reported))
    }

    fn failed(&self, slot: &ApiSlot) {
        let went_down = ApiSlot::probe_failed(&mut slot.state());
        if went_down {
            tracing::warn!(
                listener = self.listener.as_deref().unwrap_or_default(),
                server = %slot.name,
                api = %slot.base,
                "chain API down: it does not answer its probes"
            );
        }
    }

    fn came_up(&self, slot: &ApiSlot) {
        tracing::info!(
            listener = self.listener.as_deref().unwrap_or_default(),
            server = %slot.name,
            api = %slot.base,
            role = slot.role.as_str(),
            "chain API up"
        );
    }

    /// Record a chain_id other than the listener's: the API is down until
    /// it reports the right one. Logged once per change.
    fn wrong_chain(&self, slot: &ApiSlot, reported: [u8; 32], expected: [u8; 32]) {
        let first = {
            let mut state = slot.state();
            state.up = false;
            state.chain_id = Some(reported);
            !std::mem::replace(&mut state.chain_mismatch, true)
        };
        if first {
            tracing::warn!(
                listener = self.listener.as_deref().unwrap_or_default(),
                server = %slot.name,
                api = %slot.base,
                reported = %hex(&reported),
                expected = %hex(&expected),
                "chain API reports another chain_id; not used until it matches"
            );
        }
    }

    async fn probe_live(&self, slot: &ApiSlot) {
        let info: Option<Info> = self
            .fetch_json(slot, Method::POST, GET_INFO, Bytes::new())
            .await;
        let Some((info, chain_id)) = info.and_then(|i| hex32(&i.chain_id).map(|c| (i, c))) else {
            self.failed(slot);
            return;
        };
        let expected = self.expected_chain_id(slot, chain_id).await;
        if chain_id != expected {
            self.wrong_chain(slot, chain_id, expected);
            return;
        }
        let (came_up, refresh) = {
            let mut state = slot.state();
            state.chain_id = Some(chain_id);
            state.chain_mismatch = false;
            state.head = Some(info.head_block_num);
            state.lib = Some(info.last_irreversible_block_num);
            state.earliest = info.earliest_available_block_num;
            state.version = info.server_version_string;
            let came_up = ApiSlot::probe_ok(&mut state);
            let refresh = came_up || state.apis_at.is_none_or(|at| at.elapsed() >= APIS_EVERY);
            (came_up, refresh)
        };
        if came_up {
            self.came_up(slot);
        }
        if refresh {
            self.refresh_apis(slot).await;
        }
    }

    /// Ask a live API which endpoints it serves. A 404 or an answer that
    /// does not parse leaves the set unknown; a transport failure keeps the
    /// last answer and asks again on the next probe.
    async fn refresh_apis(&self, slot: &ApiSlot) {
        let Some((status, body)) = self
            .fetch(slot, Method::POST, SUPPORTED_APIS, Bytes::new())
            .await
        else {
            return;
        };
        let apis = match status {
            200 => serde_json::from_slice::<SupportedApis>(&body)
                .ok()
                .map(|s| Arc::new(s.apis.into_iter().collect::<BTreeSet<String>>())),
            _ => None,
        };
        let mut state = slot.state();
        state.apis = apis;
        state.apis_at = Some(Instant::now());
    }

    async fn probe_frozen(&self, slot: &ApiSlot) {
        let ship_head = self.hub.view().up(slot.index).and_then(|u| u.head);
        let lost = slot.state().verification.ship_head(ship_head);
        if lost == Verify::Mismatch {
            self.verification_lost(slot);
        }

        let describe: Option<Describe> = self
            .fetch_json(slot, Method::GET, DESCRIBE, Bytes::new())
            .await;
        let parsed = describe.and_then(|d| {
            let chain_id = hex32(&d.chain_id)?;
            let last = BlockPos {
                num: d.last.num,
                id: hex32(&d.last.id)?,
            };
            Some((d, chain_id, last))
        });
        let Some((describe, chain_id, last)) = parsed else {
            self.failed(slot);
            return;
        };
        let expected = self.expected_chain_id(slot, chain_id).await;
        let chain_ok = chain_id == expected;
        let step = slot
            .state()
            .verification
            .described(last, chain_ok, ship_head);
        if !chain_ok {
            // Verification is cleared; the chain_id warning says why.
            self.wrong_chain(slot, chain_id, expected);
            return;
        }
        let cap = describe
            .max_in_flight
            .map_or(self.per_upstream, |n| {
                usize::try_from(n).unwrap_or(usize::MAX).max(1)
            })
            .min(self.per_upstream);
        slot.set_cap(cap);
        let came_up = {
            let mut state = slot.state();
            state.chain_id = Some(chain_id);
            state.chain_mismatch = false;
            state.blocks = Some(Range {
                begin: describe.blocks.begin,
                end: describe.blocks.end,
            });
            state.last = Some(last);
            state.apis = describe
                .apis
                .map(|apis| Arc::new(apis.into_iter().collect::<BTreeSet<String>>()));
            state.get_block = describe.get_block;
            ApiSlot::probe_ok(&mut state)
        };
        if came_up {
            self.came_up(slot);
        }
        match step {
            Verify::Keep => {}
            Verify::Mismatch => self.verification_lost(slot),
            Verify::Check(pos) => self.check(slot, pos).await,
        }
    }

    /// Ask the API for the id of `pos.num`; it must be `pos.id`.
    async fn check(&self, slot: &ApiSlot, pos: BlockPos) {
        let body = Bytes::from(format!(r#"{{"block_num":{}}}"#, pos.num));
        let answer: Option<BlockId> = self
            .fetch_json(slot, Method::POST, GET_BLOCK_INFO, body)
            .await;
        // No answer: ask again on the next probe.
        let Some(id) = answer.and_then(|a| hex32(&a.id)) else {
            return;
        };
        let (result, was_logged) = {
            let mut state = slot.state();
            let result = state.verification.checked(pos, id);
            let was_logged = state.mismatch_logged;
            if result == Verify::Keep {
                state.mismatch_logged = false;
            }
            (result, was_logged)
        };
        if result == Verify::Mismatch {
            self.verification_lost(slot);
        } else if was_logged {
            tracing::info!(
                listener = self.listener.as_deref().unwrap_or_default(),
                server = %slot.name,
                api = %slot.base,
                block = pos.num,
                "chain API verified against its SHiP endpoint again"
            );
        } else {
            tracing::debug!(
                server = %slot.name,
                block = pos.num,
                "chain API verified against its SHiP endpoint"
            );
        }
    }

    /// Log a verification mismatch once until the API is verified again.
    fn verification_lost(&self, slot: &ApiSlot) {
        let first = !std::mem::replace(&mut slot.state().mismatch_logged, true);
        if first {
            tracing::warn!(
                listener = self.listener.as_deref().unwrap_or_default(),
                server = %slot.name,
                api = %slot.base,
                "chain API serves different blocks than its SHiP endpoint; not used until they match"
            );
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_64_hex_digits() {
        let text = "0a".repeat(32);
        assert_eq!(hex32(&text), Some([0x0a; 32]));
        assert_eq!(hex32(&text.to_ascii_uppercase()), Some([0x0a; 32]));
        assert_eq!(hex32(&"0a".repeat(31)), None);
        assert_eq!(hex32(&format!("{}zz", "0a".repeat(31))), None);
        assert_eq!(hex(&[0xab, 0x01]), "ab01");
    }

    #[test]
    fn probe_answers_parse_like_nodeos_writes_them() {
        let info: Info = serde_json::from_str(&format!(
            r#"{{"server_version":"0","chain_id":"{}","head_block_num":200,"last_irreversible_block_num":190,"head_block_id":"x","server_version_string":"v1.2.2","earliest_available_block_num":100}}"#,
            "aa".repeat(32)
        ))
        .unwrap();
        assert_eq!(
            (
                info.head_block_num,
                info.last_irreversible_block_num,
                info.earliest_available_block_num
            ),
            (200, 190, Some(100))
        );
        // EOSIO 2.0 omits earliest_available_block_num.
        let legacy: Info = serde_json::from_str(&format!(
            r#"{{"chain_id":"{}","head_block_num":5,"last_irreversible_block_num":4}}"#,
            "aa".repeat(32)
        ))
        .unwrap();
        assert_eq!(legacy.earliest_available_block_num, None);
        let describe: Describe = serde_json::from_str(&format!(
            r#"{{"server":"s","chain_id":"{0}","last":{{"num":1000,"id":"{0}"}},"blocks":{{"begin":1,"end":1001}},"apis":["/v1/chain/get_raw_block"],"get_block":"off","max_in_flight":4,"in_flight":0}}"#,
            "bb".repeat(32)
        ))
        .unwrap();
        assert_eq!(
            (
                describe.last.num,
                describe.blocks.begin,
                describe.blocks.end
            ),
            (1000, 1, 1001)
        );
        assert_eq!(describe.max_in_flight, Some(4));
        assert_eq!(describe.get_block.as_deref(), Some("off"));
    }
}
