//! Graceful shutdown signal listener.
//!
//! Returns a future that completes when SIGTERM or SIGINT is received. Passed
//! to `ServerV18::serve_with_dirs(...)` and the tokio admin-sidecar router
//! (the latter binds via `BoundAdminServer::bind` internally on the tokio
//! admin port). The mio data plane reads the same future to gate its accept
//! loop.

use std::future::Future;

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

/// Future that completes on the first SIGTERM or SIGINT received by the process.
pub async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!(target: "beava.shutdown", signal = "SIGTERM", "shutdown initiated");
        }
        _ = sigint.recv() => {
            tracing::info!(target: "beava.shutdown", signal = "SIGINT", "shutdown initiated");
        }
    }
}

/// Listens for the server shutdown signal.
///
/// Only a single shutdown signal is ever sent, after which the server
/// should shutdown. This struct can be queried to check whether signal
/// has been received.
pub struct Shutdown {
    // Channel's receiver for the shutdown signal
    notify: watch::Receiver<bool>,
}

impl Shutdown {
    /// Returns a new [`Shutdown`] with the given [`broadcast::Receiver`].
    ///
    /// [`Shutdown`]: bitcask::net::Shutdown
    /// [`broadcast::Receiver`]: tokio::sync::broadcast::Receiver
    pub fn new(notify: watch::Receiver<bool>) -> Self {
        Self { notify }
    }

    /// Returns `true` if a shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        *self.notify.borrow()
    }

    /// Blocks and waits until the shutdown signal is received, if one has
    /// not been received.
    pub fn recv<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<(), watch::error::RecvError>> + 'a {
        self.notify.changed()
    }
}
