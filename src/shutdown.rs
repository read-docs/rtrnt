use crate::EXIT_INTERRUPTED;
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
    /// Until a torrent first polls this, Ctrl-C keeps its default behavior
    /// of killing the process.
    pub(crate) async fn signal(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let handler = Arc::clone(&self);
        self.handler.call_once(move || {
            tokio::spawn(handler.handle_ctrl_c());
        });
        // Fails only once the sender is gone, but `self` holds it.
        let _ = stop.wait_for(|&stop| stop).await;
    }

    async fn handle_ctrl_c(self: Arc<Self>) {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("cannot listen for Ctrl-C: {e}");
            return;
        }
        if self.stop.receiver_count() == 0 {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
        self.stop.send_replace(true);
        info!("stopping, press Ctrl-C again to quit at once");
        if tokio::signal::ctrl_c().await.is_ok() {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
    }
}
