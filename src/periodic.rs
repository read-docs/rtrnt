//! A job that runs in the background on a deadline, again and again: the
//! tracker announces and the DHT lookups of one torrent.

use std::future::Future;
use std::time::Duration;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::Instrument;

pub struct Periodic<T> {
    /// When the last run started.
    pub last: Instant,
    /// When the next run is due.
    pub next: Instant,
    in_flight: Option<JoinHandle<T>>,
}

impl<T: Send + 'static> Periodic<T> {
    /// A job last run at `last` and next due at `next`.
    pub fn new(last: Instant, next: Instant) -> Self {
        Self {
            last,
            next,
            in_flight: None,
        }
    }

    /// Brings the next run forward, to `min` after the last one started at
    /// the soonest. Never pushes a run back.
    pub fn hurry(&mut self, min: Duration) {
        self.next = self.next.min(self.last + min);
    }

    /// Waits until the next run is due, runs the future `start` makes in the
    /// background and returns its output. With a `period`, the run after
    /// this one is due that long after this one starts, so that a `hurry`
    /// while it runs holds.
    ///
    /// Cancel-safe: a run already started is kept, and the next call waits
    /// for it, ignoring `start`.
    pub async fn poll<F>(
        &mut self,
        period: Option<Duration>,
        start: impl FnOnce() -> F,
    ) -> Result<T, JoinError>
    where
        F: Future<Output = T> + Send + 'static,
    {
        if self.in_flight.is_none() {
            tokio::time::sleep_until(self.next).await;
            self.last = Instant::now();
            if let Some(period) = period {
                self.next = self.last + period;
            }
        }
        let handle = self
            .in_flight
            .get_or_insert_with(|| tokio::spawn(start().in_current_span()));
        let output = handle.await;
        self.in_flight = None;
        output
    }

    /// Stops the run in the background, if any.
    pub fn cancel(&mut self) {
        if let Some(handle) = self.in_flight.take() {
            handle.abort();
        }
    }
}

impl<T> Drop for Periodic<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.in_flight {
            handle.abort();
        }
    }
}
