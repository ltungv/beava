use std::{future::Future, net::IpAddr, sync::Arc, time::Duration};

use beava_core::wire::{Frame, CT_JSON, OP_GET_RESPONSE, OP_PING, OP_PUSH};
use beava_core::{registry::Registry, wire::OP_ERROR_RESPONSE};
use beava_persistence::WalSink;
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::WireRequest;
use bytes::Bytes;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch, Semaphore},
    time,
};
use tracing::{debug, error, info};

use super::connection::Connection;
use crate::apply_shard::ApplyShard;
use crate::runtime_core_glue::GlueResponse;
use crate::{idem_cache::IdemCache, registry_debug::DevAggState, shutdown::Shutdown, AppState};

/// Bounded capacity for the apply-worker dispatch channel. Mirrors the
/// `read_rx` capacity used by `run_mio_event_loop` (16_384) so backpressure
/// semantics match the mio path.
const APPLY_CHANNEL_CAPACITY: usize = 16_384;

/// A unit of work shipped from a connection task to the synchronous apply
/// worker thread.
///
/// Carries the parsed `WireRequest` alongside a `oneshot::Sender` the worker
/// uses to return the result. This is the per-request analogue of the mio
/// loop's `RingItem` + per-worker `write_tx` pair — but the reply path is
/// rendezvous-bound to a single waiting task instead of routed by `slot_idx`.
struct Dispatch {
    request: WireRequest,
    reply_tx: oneshot::Sender<Vec<GlueResponse>>,
}

/// Provide methods and hold states for a Redis server. The server will exist when `shutdown`
/// finishes, or when there's an error.
pub struct Server<S> {
    listener: Listener,
    shutdown: S,
    /// Join handle for the synchronous apply worker thread.
    apply_worker_handle: Option<std::thread::JoinHandle<()>>,
}

/// The server's runtime state that is shared across all connections.
/// This is also in charge of listening for new inbound connections.
struct Listener {
    // The TCP socket for listening for inbound connection
    listener: TcpListener,

    /// Min number of milliseconds to wait for when retrying to accept a new connection.
    min_backoff_ms: u64,

    /// Max number of milliseconds to wait for when retrying to accept a new connection.
    max_backoff_ms: u64,

    // Semaphore with `MAX_CONNECTIONS`.
    //
    // When a handler is dropped, the semaphore is decremented to grant a
    // permit. When waiting for connections to close, the listener will be
    // notified once a permit is granted.
    limit_connections: Arc<Semaphore>,

    // Broacast channeling to signal a shutdown to all active connections.
    //
    // The server is responsible for gracefully shutting down active connections.
    // When a connection is spawned, it is given a broadcast receiver handle.
    // When the server wants to gracefully shutdown its connections, a `()` value
    // is sent. Each active connection receives the value, reaches a safe terminal
    // state, and completes the task.
    notify_shutdown: watch::Sender<bool>,

    // This channel ensures that the server will wait for all connections to
    // complete processing before shutting down.
    //
    // Tokio's channnels are closed when all the `Sender` handles are dropped.
    // When a connection handler is created, it is given clone of the of
    // `shutdown_complete_tx`, which is dropped when the listener shutdowns.
    // When all the listeners shut down, the channel is closed and
    // `shutdown_complete_rx.receive()` will return `None`. At this point, it
    // is safe for the server to quit.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,

    /// Sender half of the dispatch channel. Cloned into each `Handler` so
    /// connection tasks can ship `WireRequest`s to the apply worker thread.
    dispatch_tx: mpsc::Sender<Dispatch>,
}

/// Reads client requests and applies those to the storage.
struct Handler {
    // Writes and reads frame.
    connection: Connection,

    // The semaphore that granted the permit for this handler.
    // The handler is in charge of releasing its permit.
    limit_connections: Arc<Semaphore>,

    // Receives shut down signal.
    shutdown: Shutdown,

    // Signals that the handler finishes executing.
    _shutdown_complete: mpsc::Sender<()>,

    /// Sender half of the apply-worker dispatch channel. Each inbound
    /// request is paired with a fresh `oneshot::Sender` and shipped to the
    /// apply worker; the handler awaits the corresponding `oneshot::Receiver`
    /// for the `GlueResponse`.
    dispatch_tx: mpsc::Sender<Dispatch>,
}

/// Network configuration
pub struct Configuration {
    /// The host address.
    pub host: IpAddr,

    /// The port number.
    pub port: u16,

    /// Min number of milliseconds to wait for when retrying to accept a new connection.
    pub min_backoff_ms: u64,

    /// Max number of milliseconds to wait for when retrying to accept a new connection.
    pub max_backoff_ms: u64,

    /// Max number of concurrent connections that can be served by the server.
    pub max_connections: usize,
}

impl<S> Server<S> {
    /// Creates a new server.
    ///
    /// Binds the TCP listener, builds the shared `AppState` and `ApplyShard`,
    /// and spawns a dedicated OS thread that drains the dispatch channel and
    /// applies every `WireRequest` synchronously — no `.await`, no locks on
    /// the hot path.
    pub async fn new(shutdown: S, conf: Configuration) -> Result<Self, super::Error> {
        info!("starting server");
        // Ignoring the broadcast received because one can be created by
        // calling `subscribe()` on the `Sender`
        let (notify_shutdown, _) = watch::channel(false);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        // Build the shared application state.
        let agg_state = DevAggState::new(Arc::new(Registry::new()));
        let (wal_sink, _wal_worker) = WalSink::spawn_no_op();
        let idem_cache = Arc::new(IdemCache::new());
        let state = Arc::new(AppState::new(agg_state, wal_sink, idem_cache));

        // Build the WAL ring buffer + LSN tracker (no-op / in-memory for now).
        let wal_lsn = Arc::new(WalLsn::new());
        let wal_ring = Arc::new(WalBufferRing::new(3, 1 << 24, Arc::clone(&wal_lsn)));

        // Build the apply shard — owns the synchronous dispatch path.
        let apply_shard = ApplyShard::new(state, wal_ring, wal_lsn);

        // Dispatch channel: connection tasks → apply worker thread.
        let (dispatch_tx, dispatch_rx) = mpsc::channel::<Dispatch>(APPLY_CHANNEL_CAPACITY);

        // Spawn the synchronous apply worker on a dedicated OS thread.
        // This thread never touches tokio — `blocking_recv()` is the only
        // bridge. Same invariant as `beava-apply` in `run_mio_event_loop`.
        let apply_worker_handle = std::thread::Builder::new()
            .name("apply-worker".to_owned())
            .spawn(move || {
                run_apply_worker(dispatch_rx, apply_shard);
            })
            .expect("failed to spawn apply-worker thread");

        let listener = Listener {
            listener: TcpListener::bind(&format!("{}:{}", conf.host, conf.port)).await?,
            min_backoff_ms: conf.min_backoff_ms,
            max_backoff_ms: conf.max_backoff_ms,
            limit_connections: Arc::new(Semaphore::new(conf.max_connections)),
            notify_shutdown,
            shutdown_complete_rx,
            shutdown_complete_tx,
            dispatch_tx,
        };

        Ok(Self {
            listener,
            shutdown,
            apply_worker_handle: Some(apply_worker_handle),
        })
    }
}

impl<S> Server<S>
where
    S: Future,
{
    /// Runs the server that exits when `shutdown` finishes, or when there's
    /// an error.
    pub async fn run(mut self) {
        // Concurrently run the tasks and blocks the current task until
        // one of the running tasks finishes. The block that is associated
        // with the task gets to run, when the task is the first to finish.
        // Under normal circumstances, this blocks until `shutdown` finishes.
        tokio::select! {
            result = self.listener.listen() => {
                if let Err(err) = result {
                    // The server has been failing to accept inbound connections
                    // for multiple times, so it's giving up and shutting down.
                    // Error occured while handling individual connection don't
                    // propagate further.
                    error!(cause = %err, "failed to accept");
                }
            }
            _ = self.shutdown => {
                info!("shutting down");
            }
        }

        // Dropping this so tasks that have called `subscribe()` will be notified for
        // shutdown and can gracefully exit.
        drop(self.listener.notify_shutdown);

        // Dropping this so there's no dangling `Sender`. Otherwise, awaiting on the
        // channel's received will block forever because we still holding the last
        // sender instance.
        drop(self.listener.shutdown_complete_tx);

        // Drop the dispatch sender so the apply worker thread's
        // `blocking_recv()` returns `None` and the thread exits.
        drop(self.listener.dispatch_tx);

        // Awaiting for all active connections to finish processing.
        self.listener.shutdown_complete_rx.recv().await;

        // Wait for the apply worker thread to exit.
        if let Some(handle) = self.apply_worker_handle.take() {
            if let Err(e) = handle.join() {
                error!("apply-worker thread panicked: {e:?}");
            }
        }
    }
}

impl Listener {
    /// Accepts a new connection.
    ///
    /// Returns the a [`TcpStream`] on success. Retries with an exponential
    /// backoff strategy when there's an error. If the backoff time passes
    /// to maximum allowed time, returns an error.
    ///
    /// [`TcpStream`]: tokio::net::TcpStream
    async fn accept(&mut self) -> Result<TcpStream, super::Error> {
        let mut backoff = self.min_backoff_ms;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    socket.set_nodelay(true)?;
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > self.max_backoff_ms {
                        return Err(err.into());
                    }
                }
            }

            // Wait for `backoff` milliseconds
            time::sleep(Duration::from_millis(backoff)).await;

            // Doubling the backoff time
            backoff <<= 1;
        }
    }
}

impl Listener {
    async fn listen(&mut self) -> Result<(), super::Error> {
        info!("listening for new connections");

        loop {
            // Wait for a permit to become available.
            //
            // For convenient, the handle is bounded to the semaphore's lifetime
            // and when it gets dropped, it decrements the count. Because we're
            // releasing the permit in a different task from the one we acquired it
            // in, `forget()` is use to drop the semaphore handle without releasing
            // the permit at the end of this scope.
            self.limit_connections.acquire().await.unwrap().forget();

            // Accepts a new connection and retries on error. If this function
            // returns an error, it means that the server could not accept any
            // new connection and it is aborting.
            let socket = self.accept().await?;

            // Creating the handler's state for managing the new connection
            let handler = Handler {
                connection: Connection::new(socket, 1 << 14),
                limit_connections: Arc::clone(&self.limit_connections),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                dispatch_tx: self.dispatch_tx.clone(),
            };

            // Handle the connection in a new task
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause=?err, "connection error");
                }
            });
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Reads frames from the TCP stream, converts each into a `WireRequest`,
    /// dispatches to the apply worker thread via the `mpsc` channel, awaits
    /// the `GlueResponse` via a per-request `oneshot`, and writes the
    /// response frame back on the connection.
    ///
    /// Currently, pipelining is not implemented. See for more details at:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[tracing::instrument(skip(self))]
    async fn run(mut self) -> Result<(), super::Error> {
        // Keeps ingesting frames when the server is still running
        while !self.shutdown.is_shutdown() {
            // Awaiting for a shutdown event or a new frame
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            // No frame left means the client closed the connection, so we can
            // return with no error
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            debug!(?frame);

            // Convert Frame → WireRequest.
            let request = WireRequest::from(frame);

            // Pair the request with a fresh oneshot so the apply worker can
            // identify "who to reply to" without explicit routing keys.
            let (reply_tx, reply_rx) = oneshot::channel();

            if self
                .dispatch_tx
                .send(Dispatch { request, reply_tx })
                .await
                .is_err()
            {
                error!("apply worker channel closed; aborting connection");
                return Ok(());
            }

            // Suspend until the apply worker produces a response. No CPU
            // spin — the task yields to the tokio scheduler until the
            // oneshot fires.
            let responses = match reply_rx.await {
                Ok(resps) => resps,
                Err(_canceled) => {
                    error!("apply worker dropped reply_tx without responding");
                    return Ok(());
                }
            };

            // Encode each GlueResponse as a Frame and write it on the wire.
            for resp in responses {
                self.connection.write_frame(&resp.into()).await?;
            }
        }
        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Releases the permit that was granted for this handler. Performing this
        // in the `Drop` implementation ensures that the permit is always
        // automatically returned when the handler finishes
        self.limit_connections.add_permits(1);
    }
}

fn run_apply_worker(mut dispatch_rx: mpsc::Receiver<Dispatch>, apply_shard: ApplyShard) {
    info!("apply-worker thread started");

    while let Some(Dispatch { request, reply_tx }) = dispatch_rx.blocking_recv() {
        let responses = apply_shard.dispatch_wire_request_sync(request);
        // `send` only fails if the receiving connection task was dropped
        // (e.g. client disconnected while the request was in flight).
        // That's fine — discard the response and move on.
        let _ = reply_tx.send(responses);
    }

    info!("apply-worker thread: dispatch_rx closed, exiting");
}

impl From<GlueResponse> for Frame {
    fn from(resp: GlueResponse) -> Self {
        match resp {
            GlueResponse::Pong { registry_version } => {
                let body = serde_json::json!({
                    "server_version": crate::VERSION,
                    "registry_version": registry_version,
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_PING, CT_JSON, Bytes::from(b))
            }
            GlueResponse::PushAck {
                ack_lsn,
                registry_version,
            } => {
                let body = serde_json::json!({
                    "ack_lsn": ack_lsn,
                    "idempotent_replay": false,
                    "registry_version": registry_version,
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_PUSH, CT_JSON, Bytes::from(b))
            }
            GlueResponse::PushReplay {
                registry_version,
                ack_lsn,
                cached_body: _,
            } => {
                let body = match ack_lsn {
                    Some(lsn) => serde_json::json!({
                        "ack_lsn": lsn,
                        "idempotent_replay": true,
                        "registry_version": registry_version,
                    }),
                    None => serde_json::json!({
                        "idempotent_replay": true,
                        "registry_version": registry_version,
                    }),
                };
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_PUSH, CT_JSON, Bytes::from(b))
            }
            GlueResponse::Register { body, tcp_op, .. } => {
                Frame::new(tcp_op, CT_JSON, body.clone())
            }
            GlueResponse::PushError {
                code,
                registry_version,
            } => {
                let body = serde_json::json!({
                    "error": {"code": code},
                    "registry_version": registry_version,
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::QueryResult { body, format } => {
                Frame::new(OP_GET_RESPONSE, format, body.clone())
            }
            GlueResponse::QueryNotFound { code } => {
                let body = serde_json::json!({"error": {"code": code}});
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::ResetOk { registry_version } => {
                let body = serde_json::json!({
                    "reset": true,
                    "registry_version": registry_version,
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_GET_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::ResetForbidden => {
                let body = serde_json::json!({
                    "error": {
                        "code": "reset_disabled_in_production",
                        "reason": "server is not in test mode",
                    }
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::TcpError {
                code,
                message,
                extras,
            } => {
                let mut error_obj = serde_json::Map::new();
                error_obj.insert("code".to_string(), serde_json::json!(code));
                error_obj.insert("message".to_string(), serde_json::json!(message));
                if let serde_json::Value::Object(extras_obj) = extras {
                    for (k, v) in extras_obj {
                        error_obj.insert(k.clone(), v.clone());
                    }
                }
                let body = serde_json::json!({"error": serde_json::Value::Object(error_obj)});
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::UnsupportedRequestShape { hint } => {
                let body = serde_json::json!({
                    "error": {
                        "code": "unsupported_request_shape",
                        "message": hint,
                    }
                });
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            GlueResponse::HealthOk => {
                let b = br#"{"status":"ok"}"#;
                Frame::new(OP_PING, CT_JSON, &b[..])
            }
            GlueResponse::InternalError { reason } => {
                let body =
                    serde_json::json!({"error": {"code": "internal_error", "reason": reason}});
                let b = serde_json::to_vec(&body).unwrap_or_default();
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, Bytes::from(b))
            }
            // Catch-all for HTTP-only variants that shouldn't appear on a TCP
            // connection (ReadyOk, RegistrySnapshot, HttpRouteNotFound, etc.).
            _ => Frame::new(
                OP_ERROR_RESPONSE,
                CT_JSON,
                Bytes::from_static(b"{\"error\":{\"code\":\"unsupported\"}}"),
            ),
        }
    }
}
