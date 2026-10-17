//! Counters and gauges of one chain API listener, and the per-upstream
//! request results. `/metrics` renders them; requests and probes only add.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::fleet::{Histogram, HistogramSnapshot, LabeledCounter, MetricLabel};

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
    /// A body that did not arrive in time; the connection was closed.
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

    pub(super) fn set_bound(&self, bound: bool) {
        self.bound.store(bound, Ordering::Relaxed);
    }
}

// Read by the `/metrics` renderer and readiness.
#[allow(dead_code)]
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
}
