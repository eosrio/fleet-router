//! Counters and gauges of one chain API listener, and the per-upstream
//! request results. `/metrics` and the dashboard read them; requests record
//! an attempt, probes do not.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::fleet::{Histogram, HistogramSnapshot, LabeledCounter, MetricLabel};
use crate::telemetry;

use super::pick::RetryReason;
use super::route::RequestClass;

/// Implements [`MetricLabel`] for a fieldless enum: every value in
/// rendering order, each with its label text.
macro_rules! labels {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        impl MetricLabel for $name {
            const ALL: &'static [$name] = &[$($name::$variant),+];
            fn as_str(self) -> &'static str {
                match self { $($name::$variant => $label),+ }
            }
            fn index(self) -> usize {
                self as usize
            }
        }
    };
}

/// `code` label of `fleet_router_api_requests_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatusClass {
    Success,
    Redirect,
    ClientError,
    ServerError,
}

labels!(StatusClass {
    Success => "2xx",
    Redirect => "3xx",
    ClientError => "4xx",
    ServerError => "5xx",
});

impl StatusClass {
    /// The class of `status`. Informational codes never reach a client as a
    /// final answer; they count with 2xx.
    pub fn of(status: u16) -> StatusClass {
        match status {
            300..=399 => StatusClass::Redirect,
            400..=499 => StatusClass::ClientError,
            500.. => StatusClass::ServerError,
            _ => StatusClass::Success,
        }
    }
}

/// `reason` label of `fleet_router_api_rejected_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rejection {
    /// A connection over `max_connections`, closed at accept.
    Connections,
    /// A request over `max_in_flight`, answered Busy.
    InFlight,
    /// A body over `max_body_bytes`, answered 413.
    BodyTooLarge,
    /// A body that did not arrive in time, or a client that took none of
    /// its answer for the read timeout; the connection was closed.
    Timeout,
}

labels!(Rejection {
    Connections => "connections",
    InFlight => "in_flight",
    BodyTooLarge => "body_too_large",
    Timeout => "timeout",
});

/// `region` label of `fleet_router_api_block_requests_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockRegion {
    Irreversible,
    Reversible,
    /// No routing hint: answered by one live node.
    Unhinted,
}

labels!(BlockRegion {
    Irreversible => "irreversible",
    Reversible => "reversible",
    Unhinted => "unhinted",
});

/// `role` label of `fleet_router_api_block_requests_total`: who wrote the
/// answer the client got.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AnswerRole {
    Live,
    Frozen,
    /// The router answered Busy.
    None,
}

labels!(AnswerRole {
    Live => "live",
    Frozen => "frozen",
    None => "none",
});

/// `result` label of `fleet_router_api_pushes_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushResult {
    /// A 2xx from the node.
    Ok,
    /// Any other answer from the node, or no node could be reached.
    Error,
    /// The connection failed after the push was written: it may or may not
    /// have been accepted, and it is not resent.
    Ambiguous,
}

labels!(PushResult {
    Ok => "ok",
    Error => "error",
    Ambiguous => "ambiguous",
});

/// `result` label of `fleet_router_upstream_api_requests_total`: what one
/// attempt on one upstream API gave.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UpstreamResult {
    Ok,
    UnknownBlock,
    NotFound,
    Busy,
    /// Any other non-2xx answer.
    Error,
    Connect,
    Timeout,
}

labels!(UpstreamResult {
    Ok => "ok",
    UnknownBlock => "unknown_block",
    NotFound => "not_found",
    Busy => "busy",
    Error => "error",
    Connect => "connect",
    Timeout => "timeout",
});

impl UpstreamResult {
    /// The backend failed the attempt. Unknown Block and 404 are answers:
    /// the node responded, and the router can ask another.
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            Self::Busy | Self::Error | Self::Connect | Self::Timeout
        )
    }
}

/// No success or error has been recorded. A real timestamp is never this.
const NEVER_MS: u64 = u64::MAX;

/// Latency and the last success or failure of one upstream API.
///
/// Fixed histogram buckets, four atomics. [`Self::record`] only stores; it
/// does not allocate or take a lock. Probes do not record.
#[derive(Debug)]
pub struct AttemptStats {
    latency: Histogram,
    /// Microseconds of the latest [`Self::record`]. Meaningful once
    /// [`Histogram::count`] is non-zero.
    last_us: AtomicU64,
    last_success_ms: AtomicU64,
    last_error_ms: AtomicU64,
}

/// What the dashboard reads from [`AttemptStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptSnapshot {
    pub count: u64,
    pub last_us: Option<u64>,
    pub avg_us: Option<u64>,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_error_ms: Option<u64>,
}

impl Default for AttemptStats {
    fn default() -> Self {
        AttemptStats {
            latency: Histogram::default(),
            last_us: AtomicU64::new(0),
            last_success_ms: AtomicU64::new(NEVER_MS),
            last_error_ms: AtomicU64::new(NEVER_MS),
        }
    }
}

impl AttemptStats {
    /// One finished attempt. `elapsed` is the time until the response head
    /// arrived, or until the attempt failed. The body of a relayed answer
    /// is not included.
    pub fn record(&self, result: UpstreamResult, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        // Relaxed, like the histogram. A concurrent reader can be one
        // sample behind; it never allocates and never takes a lock.
        self.last_us.store(us, Ordering::Relaxed);
        self.latency.observe(elapsed);
        // Unknown Block and 404 do not move either timestamp, so they skip
        // the clock read.
        if result == UpstreamResult::Ok {
            self.last_success_ms
                .store(telemetry::now_ms(), Ordering::Relaxed);
        } else if result.is_failure() {
            self.last_error_ms
                .store(telemetry::now_ms(), Ordering::Relaxed);
        }
    }

    pub fn latency(&self) -> HistogramSnapshot {
        self.latency.snapshot()
    }

    pub fn snapshot(&self) -> AttemptSnapshot {
        let count = self.latency.count();
        AttemptSnapshot {
            count,
            last_us: (count > 0).then(|| self.last_us.load(Ordering::Relaxed)),
            avg_us: self.latency.average_us(),
            p50_us: self.latency.quantile_us(0.50),
            p95_us: self.latency.quantile_us(0.95),
            last_success_ms: stamp(self.last_success_ms.load(Ordering::Relaxed)),
            last_error_ms: stamp(self.last_error_ms.load(Ordering::Relaxed)),
        }
    }
}

fn stamp(ms: u64) -> Option<u64> {
    (ms != NEVER_MS).then_some(ms)
}

/// The chain API counters of one listener.
#[derive(Debug)]
pub struct ApiStats {
    /// `[class][code]`.
    requests: Box<[[AtomicU64; 4]]>,
    /// By [`RequestClass`], from the request head to the response head.
    durations: Box<[Histogram]>,
    in_flight: AtomicUsize,
    pub rejected: LabeledCounter<Rejection>,
    pub retries: LabeledCounter<RetryReason>,
    /// `[region][role]`.
    block_requests: [[AtomicU64; 3]; 3],
    pub pushes: LabeledCounter<PushResult>,
    /// The listener is bound and accepting.
    bound: AtomicBool,
}

impl Default for ApiStats {
    fn default() -> Self {
        ApiStats {
            requests: RequestClass::ALL
                .iter()
                .map(|_| Default::default())
                .collect(),
            durations: RequestClass::ALL
                .iter()
                .map(|_| Histogram::default())
                .collect(),
            in_flight: AtomicUsize::new(0),
            rejected: LabeledCounter::new(),
            retries: LabeledCounter::new(),
            block_requests: Default::default(),
            pushes: LabeledCounter::new(),
            bound: AtomicBool::new(false),
        }
    }
}

impl ApiStats {
    /// Count one answered request.
    pub fn request(&self, class: RequestClass, status: u16, elapsed: Duration) {
        self.requests[class.index()][StatusClass::of(status).index()]
            .fetch_add(1, Ordering::Relaxed);
        self.durations[class.index()].observe(elapsed);
    }

    pub fn block_request(&self, region: BlockRegion, role: AnswerRole) {
        self.block_requests[region.index()][role.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn enter(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn set_bound(&self, bound: bool) {
        self.bound.store(bound, Ordering::Relaxed);
    }
}

// Read by the `/metrics` renderer and readiness.
impl ApiStats {
    pub fn requests(&self, class: RequestClass, code: StatusClass) -> u64 {
        self.requests[class.index()][code.index()].load(Ordering::Relaxed)
    }

    pub fn duration(&self, class: RequestClass) -> HistogramSnapshot {
        self.durations[class.index()].snapshot()
    }

    pub fn block_requests(&self, region: BlockRegion, role: AnswerRole) -> u64 {
        self.block_requests[region.index()][role.index()].load(Ordering::Relaxed)
    }

    /// Requests holding a `max_in_flight` slot now.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// The listener is bound and accepting.
    pub fn bound(&self) -> bool {
        self.bound.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_count_by_class_and_status_class() {
        let stats = ApiStats::default();
        stats.request(RequestClass::Block, 200, Duration::from_millis(3));
        stats.request(RequestClass::Block, 400, Duration::from_millis(3));
        stats.request(RequestClass::Block, 503, Duration::from_millis(3));
        stats.request(RequestClass::Denied, 404, Duration::from_micros(10));
        assert_eq!(stats.requests(RequestClass::Block, StatusClass::Success), 1);
        assert_eq!(
            stats.requests(RequestClass::Block, StatusClass::ClientError),
            1
        );
        assert_eq!(
            stats.requests(RequestClass::Block, StatusClass::ServerError),
            1
        );
        assert_eq!(
            stats.requests(RequestClass::Denied, StatusClass::ClientError),
            1
        );
        assert_eq!(stats.duration(RequestClass::Block).count, 3);
        assert_eq!(StatusClass::of(101), StatusClass::Success);
        assert_eq!(StatusClass::of(302), StatusClass::Redirect);

        stats.block_request(BlockRegion::Unhinted, AnswerRole::Live);
        assert_eq!(
            stats.block_requests(BlockRegion::Unhinted, AnswerRole::Live),
            1
        );
        assert_eq!(
            stats.block_requests(BlockRegion::Irreversible, AnswerRole::None),
            0
        );
        stats.enter();
        assert_eq!(stats.in_flight(), 1);
        stats.leave();
        assert_eq!(stats.in_flight(), 0);
        for labels in [
            StatusClass::ALL
                .iter()
                .map(|l| l.as_str())
                .collect::<Vec<_>>(),
            Rejection::ALL.iter().map(|l| l.as_str()).collect(),
            BlockRegion::ALL.iter().map(|l| l.as_str()).collect(),
            AnswerRole::ALL.iter().map(|l| l.as_str()).collect(),
            PushResult::ALL.iter().map(|l| l.as_str()).collect(),
            UpstreamResult::ALL.iter().map(|l| l.as_str()).collect(),
        ] {
            let mut unique = labels.clone();
            unique.dedup();
            assert_eq!(unique, labels);
        }
        assert_eq!(UpstreamResult::UnknownBlock.as_str(), "unknown_block");
        assert_eq!(Rejection::BodyTooLarge.index(), 2);
    }

    #[test]
    fn failures_are_transport_and_unusable_answers() {
        assert!(!UpstreamResult::Ok.is_failure());
        assert!(!UpstreamResult::UnknownBlock.is_failure());
        assert!(!UpstreamResult::NotFound.is_failure());
        assert!(UpstreamResult::Busy.is_failure());
        assert!(UpstreamResult::Error.is_failure());
        assert!(UpstreamResult::Connect.is_failure());
        assert!(UpstreamResult::Timeout.is_failure());
    }

    #[test]
    fn attempts_record_latency_and_the_last_success_or_failure() {
        let stats = AttemptStats::default();
        let before = crate::telemetry::now_ms();
        assert_eq!(stats.snapshot().count, 0);
        assert_eq!(stats.snapshot().last_us, None);
        assert_eq!(stats.snapshot().last_success_ms, None);
        assert_eq!(stats.snapshot().last_error_ms, None);
        assert_eq!(stats.latency().count, 0);

        stats.record(UpstreamResult::Ok, Duration::from_micros(100));
        stats.record(UpstreamResult::Error, Duration::from_millis(3));
        stats.record(UpstreamResult::UnknownBlock, Duration::from_micros(100));
        stats.record(UpstreamResult::NotFound, Duration::from_micros(100));
        stats.record(UpstreamResult::Connect, Duration::from_millis(30));
        let after = crate::telemetry::now_ms();

        let snap = stats.snapshot();
        assert_eq!(snap.count, 5);
        assert_eq!(snap.last_us, Some(30_000));
        // 100+3000+100+100+30000 = 33300 µs over 5 attempts.
        assert_eq!(snap.avg_us, Some(6_660));
        assert!(snap.p50_us.is_some());
        assert!(snap.p95_us.is_some());
        let success = snap.last_success_ms.expect("a 2xx was recorded");
        let error = snap.last_error_ms.expect("a failure was recorded");
        assert!(success >= before && success <= after);
        assert!(
            error >= success && error <= after,
            "{error} not after {success}"
        );

        // An Unknown Block is an answer: it must not move the timestamps.
        let again = stats.snapshot();
        stats.record(UpstreamResult::UnknownBlock, Duration::from_secs(30));
        let later = stats.snapshot();
        assert_eq!(later.last_success_ms, again.last_success_ms);
        assert_eq!(later.last_error_ms, again.last_error_ms);
        assert_eq!(later.last_us, Some(30_000_000));
        assert_eq!(later.count, 6);
        // 30 s sits above the last finite bucket, so the high percentile does not.
        assert_eq!(later.p95_us, None);
        assert!(later.avg_us.unwrap() > 1_000_000);
    }

    #[test]
    fn quantiles_interpolate_inside_fixed_buckets() {
        let stats = AttemptStats::default();
        for _ in 0..100 {
            stats.record(UpstreamResult::Ok, Duration::from_micros(100));
        }
        let snap = stats.snapshot();
        // Every sample is in the 1 ms bucket. The rank is interpolated
        // from 0 up to that bucket's bound.
        assert_eq!(snap.p50_us, Some(500));
        assert_eq!(snap.p95_us, Some(950));
        assert_eq!(snap.avg_us, Some(100));
        assert_eq!(snap.last_us, Some(100));

        let one = AttemptStats::default();
        one.record(UpstreamResult::Ok, Duration::from_millis(3));
        // A single sample lands on its bucket's upper bound.
        assert_eq!(one.snapshot().p50_us, Some(5_000));
        assert_eq!(Histogram::default().quantile_us(1.5), None);
        assert_eq!(Histogram::default().quantile_us(0.5), None);
        assert_eq!(Histogram::default().average_us(), None);
    }
}
