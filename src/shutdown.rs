use crate::EXIT_INTERRUPTED;
use rtrnt::tui;
use std::sync::{Arc, Once};
use tokio::sync::watch;
use tracing::{info, warn};

/// Ctrl-C, for every torrent of the session.
pub(crate) struct Shutdown {
    /// Set by the first Ctrl-C.
    stop: watch::Sender<bool>,
    handler: Once,
}

impl Shutdown {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            stop: watch::Sender::new(false),
            handler: Once::new(),
        })
    }

    /// Resolves on the first Ctrl-C, or at once if it has come already. A
    /// second one, or one that comes while no torrent waits for this, exits
    /// at once.
    ///
    /// A torrent waits from the moment it calls this, not from the first
    /// poll, so a Ctrl-C while it starts is not lost. Until a torrent first
    /// polls the result, Ctrl-C keeps its default behavior of killing the
    /// process.
    pub(crate) fn signal(self: Arc<Self>) -> impl Future<Output = ()> + Send + 'static {
        let mut stop = self.stop.subscribe();
        async move {
            let handler = Arc::clone(&self);
            self.handler.call_once(move || {
                tokio::spawn(handler.handle_ctrl_c());
            });
            // Fails only once the sender is gone, but `self` holds it.
            let _ = stop.wait_for(|&stop| stop).await;
        }
    }

    async fn handle_ctrl_c(self: Arc<Self>) {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("cannot listen for Ctrl-C: {e}");
            return;
        }
        if !self.request_stop() {
            exit_now();
        }
        if tokio::signal::ctrl_c().await.is_ok() {
            exit_now();
        }
    }

    /// Does what the first Ctrl-C does: asks every torrent to stop. Returns
    /// false, asking nothing, if a stop was asked for already or no torrent
    /// waits for one yet; the caller should then exit at once.
    pub(crate) fn request_stop(&self) -> bool {
        if *self.stop.borrow() || self.stop.receiver_count() == 0 {
            return false;
        }
        self.stop.send_replace(true);
        info!("stopping, press Ctrl-C again to quit at once");
        true
    }
}

/// Exits at once, as a second Ctrl-C does, with the terminal put back as it
/// was if the UI had it.
fn exit_now() -> ! {
    tui::restore_terminal();
    std::process::exit(EXIT_INTERRUPTED.into());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_stop_asked_for_before_the_first_poll_is_not_lost() {
        let shutdown = Shutdown::new();
        let signal = Arc::clone(&shutdown).signal();
        assert!(shutdown.request_stop());
        signal.await;
    }
}
