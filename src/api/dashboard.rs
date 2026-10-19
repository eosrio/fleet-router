//! A single revisioned publication for every dashboard panel. Lifecycle changes
//! wake the publisher; high-volume counters stay on a one-second cadence.
//! Subscribers share encoded updates and resync from a bounded cache on a gap.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use futures::Stream;
use serde::Serialize;
use tokio::sync::{broadcast, broadcast::error::RecvError};
use tokio::time::{sleep_until, Instant};

use super::{
    session_views, status_with_session_counts, transition_event, ApiError, SessionView,
    SharedState, StatusView,
};
use crate::telemetry::{self, Event, Sample, UpstreamSample, HISTORY_SAMPLES, MAX_EVENTS};

const UPDATE_CAPACITY: usize = 8;
const MIN_PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

struct Snapshot {
    revision: u64,
    status: StatusView,
    sessions: Vec<SessionView>,
}

#[derive(Default)]
struct Cache {
    latest: Option<Arc<Snapshot>>,
    history: VecDeque<Sample>,
    events: VecDeque<Event>,
}

struct Update {
    revision: u64,
    data: String,
}

#[derive(Serialize)]
struct History<'a> {
    interval_ms: u64,
    samples: &'a [Sample],
}

#[derive(Serialize)]
struct Frame<'a> {
    revision: u64,
    bootstrap: bool,
    status: &'a StatusView,
    sessions: &'a [SessionView],
    events: &'a [Event],
    #[serde(skip_serializing_if = "Option::is_none")]
    sample: Option<&'a Sample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<History<'a>>,
}

/// Shared, bounded state. Only the publisher captures and encodes live views;
/// connections serialize retained history once on bootstrap or resync.
pub struct Feed {
    cache: Mutex<Cache>,
    updates: broadcast::Sender<Arc<Update>>,
}

impl Default for Feed {
    fn default() -> Self {
        Self {
            cache: Mutex::new(Cache::default()),
            updates: broadcast::channel(UPDATE_CAPACITY).0,
        }
    }
}

impl Feed {
    fn cache(&self) -> MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn publish(
        &self,
        status: StatusView,
        sessions: Vec<SessionView>,
        events: Vec<Event>,
        sample: Option<Sample>,
    ) -> Result<(), serde_json::Error> {
        let mut cache = self.cache();
        let revision = cache.latest.as_ref().map_or(1, |s| s.revision + 1);
        let data = serde_json::to_string(&Frame {
            revision,
            bootstrap: false,
            status: &status,
            sessions: &sessions,
            events: &events,
            sample: sample.as_ref(),
            history: None,
        })?;
        if let Some(sample) = sample {
            cache.history.push_back(sample);
            if cache.history.len() > HISTORY_SAMPLES {
                cache.history.pop_front();
            }
        }
        cache.events.extend(events);
        let excess = cache.events.len().saturating_sub(MAX_EVENTS);
        cache.events.drain(..excess);
        cache.latest = Some(Arc::new(Snapshot {
            revision,
            status,
            sessions,
        }));
        drop(cache);
        let _ = self.updates.send(Arc::new(Update { revision, data }));
        Ok(())
    }

    fn bootstrap(&self) -> Result<Option<Arc<Update>>, serde_json::Error> {
        let cache = self.cache();
        let Some(latest) = cache.latest.clone() else {
            return Ok(None);
        };
        let history: Vec<_> = cache.history.iter().cloned().collect();
        let events: Vec<_> = cache.events.iter().cloned().collect();
        drop(cache);
        let data = serde_json::to_string(&Frame {
            revision: latest.revision,
            bootstrap: true,
            status: &latest.status,
            sessions: &latest.sessions,
            events: &events,
            sample: None,
            history: Some(History {
                interval_ms: 1000,
                samples: &history,
            }),
        })?;
        Ok(Some(Arc::new(Update {
            revision: latest.revision,
            data,
        })))
    }

    fn subscribe(self: &Arc<Self>) -> Subscription {
        // Subscribe before reading the cache so no publication can fall between
        // bootstrap and live updates. Queued duplicates are skipped by revision.
        Subscription {
            feed: self.clone(),
            updates: self.updates.subscribe(),
            revision: 0,
            resync: true,
        }
    }
}

struct Subscription {
    feed: Arc<Feed>,
    updates: broadcast::Receiver<Arc<Update>>,
    revision: u64,
    resync: bool,
}

impl Subscription {
    async fn next(&mut self) -> Option<Arc<Update>> {
        loop {
            if self.resync {
                match self.feed.bootstrap() {
                    Ok(Some(update)) => {
                        self.revision = update.revision;
                        self.resync = false;
                        return Some(update);
                    }
                    Ok(None) => {} // Publisher has not made its first frame yet.
                    Err(error) => {
                        tracing::warn!(%error, "failed to encode dashboard bootstrap");
                        return None;
                    }
                }
            }
            match self.updates.recv().await {
                Ok(update) if self.resync || update.revision <= self.revision => continue,
                Ok(update) => {
                    self.revision = update.revision;
                    return Some(update);
                }
                Err(RecvError::Lagged(_)) => {
                    // Discard the old queue before taking a fresh cache cut.
                    // Otherwise a slow cursor could immediately lag again on
                    // frames already included in the resync.
                    self.updates = self.feed.updates.subscribe();
                    self.resync = true;
                }
                Err(RecvError::Closed) => return None,
            }
        }
    }
}

pub(super) async fn get_stream(
    State(app): State<SharedState>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let permit = app.streams.clone().try_acquire_owned().map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many open dashboard streams",
        )
    })?;
    let stream = futures::stream::unfold(
        (app.dashboard_feed.subscribe(), permit),
        |(mut sub, permit)| async move {
            let update = sub.next().await?;
            let event = SseEvent::default()
                .event("dashboard")
                .id(update.revision.to_string())
                .data(&update.data);
            Some((Ok(event), (sub, permit)))
        },
    );
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

/// Runs once per process when the dashboard is enabled. The counter hot path
/// never builds JSON or wakes viewers. Lifecycle bursts coalesce at four Hz.
pub async fn run(app: SharedState, mut shutdown: broadcast::Receiver<()>) {
    let telemetry = telemetry::global();
    let mut previous = HashMap::new();
    let mut last_event = 0;
    let mut next_sample = Instant::now();
    let mut next_publish = next_sample;
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep_until(next_sample) => {},
            _ = telemetry.changed.notified() => {},
        }
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep_until(next_publish) => {},
        }
        let sessions = session_views(&app).await;
        let mut counts = HashMap::new();
        for session in &sessions {
            *counts.entry(session.listener.clone()).or_default() += 1;
        }
        // Session badges, listener totals and table rows use one membership cut.
        let status = status_with_session_counts(&app, counts).await;
        let sample_due = Instant::now() >= next_sample;
        let mut upstreams = BTreeMap::new();
        for listener in &status.listeners {
            for u in &listener.upstreams {
                let key = format!("{}|{}", listener.name, u.endpoint);
                if let Some(before) = previous.insert(key.clone(), u.status) {
                    if let Some(event) = transition_event(&u.name, before, u.status) {
                        telemetry.emit(event.listener(&listener.name).upstream(&u.endpoint));
                    }
                }
                if sample_due {
                    upstreams.insert(
                        key,
                        UpstreamSample {
                            sessions: u.sessions,
                            head_block: u.head_block,
                            blocks_total: u.blocks_total,
                            bytes_total: u.bytes_total,
                            status: u.status,
                        },
                    );
                }
            }
        }
        let sample = sample_due.then_some(Sample {
            t_ms: status.now_ms,
            upstreams,
        });
        if let Some(sample) = &sample {
            telemetry.record_sample(sample.clone());
        }
        let events = telemetry.events_after(last_event);
        let through = events.last().map_or(last_event, |e| e.id);
        match app.dashboard_feed.publish(status, sessions, events, sample) {
            Ok(()) => last_event = through,
            Err(error) => tracing::warn!(%error, "failed to encode dashboard update"),
        }
        let now = Instant::now();
        if sample_due {
            next_sample = now + SAMPLE_INTERVAL;
        }
        next_publish = now + MIN_PUBLISH_INTERVAL;
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ControlView, TotalsView};
    use super::*;
    use crate::telemetry::{EventKind, Level};

    fn publish(feed: &Feed, n: u64) {
        let status = StatusView {
            version: "test",
            now_ms: n * 1000,
            started_ms: 1,
            uptime_secs: n,
            control: ControlView { enabled: false },
            totals: TotalsView {
                sessions: 0,
                upstreams: 0,
                upstreams_up: 0,
            },
            listeners: vec![],
        };
        let mut event = Event::new(Level::Info, EventKind::Control, format!("change {n}"));
        event.id = n;
        feed.publish(
            status,
            vec![],
            vec![event],
            Some(Sample {
                t_ms: n * 1000,
                upstreams: BTreeMap::new(),
            }),
        )
        .unwrap();
    }

    fn decode(update: &Update) -> serde_json::Value {
        serde_json::from_str(&update.data).unwrap()
    }

    #[tokio::test]
    async fn slow_subscriber_gets_a_complete_resync_then_ordered_updates() {
        let feed = Arc::new(Feed::default());
        publish(&feed, 1);
        let mut slow = feed.subscribe();
        let mut fast = feed.subscribe();
        assert_eq!(decode(&slow.next().await.unwrap())["bootstrap"], true);
        fast.next().await.unwrap();
        for n in 2..=20 {
            publish(&feed, n);
            let frame = decode(&fast.next().await.unwrap());
            assert_eq!(frame["revision"], n);
            assert_eq!(frame["bootstrap"], false);
        }
        let reset = decode(&slow.next().await.unwrap());
        assert_eq!(reset["bootstrap"], true);
        assert_eq!(reset["revision"], 20);
        assert_eq!(reset["events"].as_array().unwrap().len(), 20);
        assert_eq!(reset["history"]["samples"].as_array().unwrap().len(), 20);
        publish(&feed, 21);
        let next = slow.next().await.unwrap();
        assert_eq!(
            next.revision, 21,
            "queued pre-resync frames must not roll state backwards"
        );
        assert!(
            Arc::ptr_eq(&next, &fast.next().await.unwrap()),
            "viewers share one encoded live frame"
        );
    }

    #[tokio::test]
    async fn subscribe_before_first_publication_bootstraps_without_losing_an_update() {
        let feed = Arc::new(Feed::default());
        let mut sub = feed.subscribe();
        // Waiting before the first publication is also a normal startup path.
        let consumer = tokio::spawn(async move { sub.next().await.unwrap() });
        tokio::task::yield_now().await;
        publish(&feed, 1);
        let update = tokio::time::timeout(Duration::from_secs(1), consumer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode(&update)["bootstrap"], true);
        assert_eq!(update.revision, 1);
    }

    #[tokio::test]
    async fn resync_history_and_events_have_bounded_retention() {
        let feed = Arc::new(Feed::default());
        for n in 1..=1000 {
            publish(&feed, n);
        }
        let reset = decode(&feed.subscribe().next().await.unwrap());
        assert_eq!(
            reset["history"]["samples"].as_array().unwrap().len(),
            HISTORY_SAMPLES
        );
        assert_eq!(reset["events"].as_array().unwrap().len(), MAX_EVENTS);
        assert_eq!(reset["events"][0]["id"], 501);
        assert_eq!(reset["revision"], 1000);
    }
}
