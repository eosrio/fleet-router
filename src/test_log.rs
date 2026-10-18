//! Log capture for tests: the lines logged on the test's own thread.
//!
//! One global subscriber formats every event at info and above and hands
//! the line to the capture of the thread that logged it, if that thread has
//! one. A `#[tokio::test]` runs on a current-thread runtime, so the tasks it
//! spawns log on its thread too. (A subscriber set per thread is not enough:
//! a log line first reached on another test's thread, with no subscriber
//! there, can stay disabled for every thread.)

use std::cell::RefCell;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, Once};

thread_local! {
    static SINK: RefCell<Option<Captured>> = const { RefCell::new(None) };
}

/// The text logged on a thread while its [`Capturing`] guard lives,
/// without colours.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn buf(&self) -> MutexGuard<'_, Vec<u8>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The captured lines that contain `needle`.
    pub fn lines_with(&self, needle: &str) -> Vec<String> {
        String::from_utf8_lossy(&self.buf())
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_string)
            .collect()
    }
}

/// Ends the capture on its thread when dropped.
pub struct Capturing(());

impl Drop for Capturing {
    fn drop(&mut self) {
        SINK.with(|sink| sink.borrow_mut().take());
    }
}

/// Writes to the capture of the thread that logs, if it has one.
struct ThreadSink;

impl io::Write for ThreadSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        SINK.with(|sink| {
            if let Some(captured) = sink.borrow().as_ref() {
                captured.buf().extend_from_slice(data);
            }
        });
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Capture this thread's log lines at info and above until the guard drops.
pub fn capture() -> (Captured, Capturing) {
    static GLOBAL: Once = Once::new();
    GLOBAL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(|| ThreadSink)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other global subscriber in the unit tests");
    });
    let captured = Captured::default();
    SINK.with(|sink| *sink.borrow_mut() = Some(captured.clone()));
    (captured, Capturing(()))
}
