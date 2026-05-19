//! Parsed wire requests produced by the TCP frame parser and HTTP parser.
//!
//! A `WireRequest` is the unified command type dispatched to the apply callback
//! regardless of which transport delivered it. Mirrors Redis's `client::argv`
//! but uses an enum instead of a slice of `robj*`.

use beava_core::wire::{
    Frame, CT_JSON, CT_MSGPACK, OP_BATCH_GET, OP_GET, OP_GET_MULTI, OP_MGET, OP_PING, OP_PUSH,
    OP_REGISTER, OP_RESET,
};
use bytes::Bytes;

use crate::tcp_listener::{parse_json_envelope, parse_msgpack_envelope};

/// A fully-parsed inbound request, ready for the apply thread.
///
/// Produced by `tcp_listener::parse_wire_request` (framed TCP) or by the HTTP
/// state machine. The apply thread receives these via the dispatch callback
/// and never touches raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireRequest {
    /// OP_PING (0x0000) — health probe; no payload.
    Ping,
    /// OP_REGISTER (0x0001) — register a feature pipeline DAG.
    Register { payload: Bytes },
    /// OP_PUSH (0x0010) — fire-and-forget event push.
    /// `event_name` is the stream name; `body` is the raw payload bytes (JSON or MsgPack).
    /// `body_format` indicates the encoding: `beava_core::wire::CT_JSON` or `CT_MSGPACK`.
    TcpPush {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push/:event — same semantic as TcpPush but via HTTP.
    /// Always `CT_JSON` format (HTTP path carries JSON bodies only).
    HttpPush {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push-sync/:event — synchronous push (await fsync).
    HttpPushSync {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push-batch/:event — batched push.
    HttpPushBatch {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /get — batch feature read.
    HttpGet { body: Bytes },
    /// HTTP GET /get/:feature/:key — single feature read.
    HttpGetSingle { feature: String, key: String },
    /// OP_GET (0x0020) — TCP single-key feature read.
    ///
    /// `body` is the JSON or MsgPack payload encoding `{feature, key}`;
    /// `body_format` is `beava_core::wire::CT_JSON` (0x01) or `CT_MSGPACK` (0x02).
    /// Deserialisation happens at dispatch time so the wire crate stays
    /// serialiser-agnostic.
    TcpGet { body: Bytes, body_format: u8 },
    /// OP_MGET (0x0021) — TCP batched single-feature multi-key read.
    ///
    /// `body` payload encodes `{feature: <name>, keys: [...]}`; same content_type rules.
    TcpMGet { body: Bytes, body_format: u8 },
    /// OP_GET_MULTI (0x0022) — TCP batched multi-feature multi-key read.
    ///
    /// `body` payload encodes `{keys: [...], features: [...]}` (mirrors HTTP /get);
    /// same content_type rules.
    TcpGetMulti { body: Bytes, body_format: u8 },
    /// OP_BATCH_GET (0x0024) — TCP heterogeneous batched read.
    ///
    /// `body` payload encodes `{requests: [{table, entity_id}, ...]}`;
    /// `body_format` is `CT_JSON` (0x01) or `CT_MSGPACK` (0x02). Composes
    /// natively with the global-table empty-string entity_id sentinel. The
    /// dispatch layer (`apply_shard.rs::dispatch_batch_get_sync`) walks the
    /// request list, calls per-entity feature lookup, and aggregates results
    /// with partial-failure semantics (unknown_table becomes a per-tuple
    /// error inside `results` rather than a whole-batch 4xx).
    TcpBatchGet { body: Bytes, body_format: u8 },
    /// HTTP POST /batch_get — same payload as TcpBatchGet, JSON-only.
    HttpBatchGet { body: Bytes },
    /// OP_RESET (0x0040) — TCP full-clear request.
    ///
    /// `body` payload is empty `{}` JSON (or empty msgpack `{}`); the
    /// content is opaque to the parser. The dispatch arm
    /// (`apply_shard.rs::dispatch_reset_sync`) honors the server's
    /// `effective_test_mode` flag — if false, returns the
    /// `reset_disabled_in_production` error (HTTP 403 / wire OP_ERROR_RESPONSE).
    /// If true, drops every per-entity state table + every registered
    /// descriptor and bumps `registry_version`.
    TcpReset { body: Bytes, body_format: u8 },
    /// HTTP POST /reset — same semantics as `TcpReset`. JSON-only on HTTP.
    HttpReset { body: Bytes },
    /// POST request whose Content-Type was not `application/json` (or absent).
    /// Encoded as 415 with a structured `unsupported_media_type` body.
    /// `received` is what the client sent (or the empty string when the
    /// header was absent); `path` is the HTTP request path.
    HttpUnsupportedMediaType { received: String, path: String },
    /// GET /health — liveness probe. No payload, no apply-thread roundtrip;
    /// the dispatch layer returns `GlueResponse::HealthOk` directly so
    /// /health stays responsive even before WAL recovery completes.
    HttpHealth,
    /// POST /ping — verb-style liveness probe. Distinct from `HttpHealth`
    /// (GET /health): a verb-style symmetric endpoint that matches the TCP
    /// `OP_PING (0x0000)` semantics. The dispatch layer returns
    /// `GlueResponse::HealthOk` (same `200 {"status":"ok"}` shape as /health)
    /// — keeping the two endpoints body-identical lets fixtures poll either.
    HttpPing,
    /// GET /ready — readiness probe on the data-plane port. Mirrors the
    /// admin sidecar's /ready for back-compat with TestServer-using test
    /// files that poll `base_url()` for readiness.
    HttpReady,
    /// GET /registry — registry snapshot dump on the data-plane port.
    /// Mirrors the admin sidecar's /registry for back-compat with tests
    /// that GET `/registry` for schema-propagation assertions.
    HttpRegistry,
    /// HTTP request whose path didn't match the router table. Distinct from
    /// `ParseError` (which covers wire-level decode failures) so the encoder
    /// can return `404 Not Found` for unknown routes.
    HttpNotFound { path: String },
    /// HTTP request whose path matched a route but with the wrong method.
    /// Encoder returns `405 Method Not Allowed`.
    HttpMethodNotAllowed { method: String, path: String },
    /// Unknown or reserved opcode; contains the raw opcode for error reporting.
    Unknown { op: u16 },
    /// Malformed frame (parse error).
    ParseError { reason: String },
}

impl From<Frame> for WireRequest {
    fn from(frame: Frame) -> Self {
        let req = match frame.op {
            OP_PING => WireRequest::Ping,
            OP_REGISTER => match frame.content_type {
                // TCP register accepts CT_JSON only. Other content types surface as
                // `unsupported_content_type` via ParseError — the apply-shard
                // dispatcher classifies it as a TcpError.
                CT_JSON => WireRequest::Register {
                    payload: frame.payload,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type for register: {other:#04x}"),
                },
            },
            OP_PUSH => {
                match frame.content_type {
                    CT_JSON => {
                        // Zero-copy envelope scan. Body slice aliases
                        // frame.payload directly; no re-serialise.
                        match parse_json_envelope(&frame.payload) {
                            Ok((event_name, body_bytes)) => {
                                // Slice frame.payload to keep the Bytes refcounted view.
                                let body_start =
                                    body_bytes.as_ptr() as usize - frame.payload.as_ptr() as usize;
                                let body_end = body_start + body_bytes.len();
                                let body = frame.payload.slice(body_start..body_end);
                                WireRequest::TcpPush {
                                    event_name: event_name.to_string(),
                                    body,
                                    body_format: CT_JSON,
                                }
                            }
                            Err(e) => WireRequest::ParseError {
                                reason: e.to_string(),
                            },
                        }
                    }
                    CT_MSGPACK => {
                        // Hand-rolled scanner via rmp::decode primitives. No serde,
                        // no JsonValue, no body re-encode — body slice aliases
                        // frame.payload directly.
                        match parse_msgpack_envelope(&frame.payload) {
                            Ok((event_name, body_bytes)) => {
                                // Bytes::from triggers a refcount-bump copy out of the
                                // frame.payload Bytes. To stay zero-copy across the
                                // WireRequest boundary we slice the original Bytes.
                                let body_start =
                                    body_bytes.as_ptr() as usize - frame.payload.as_ptr() as usize;
                                let body_end = body_start + body_bytes.len();
                                let body = frame.payload.slice(body_start..body_end);
                                WireRequest::TcpPush {
                                    event_name: event_name.to_string(),
                                    body,
                                    body_format: CT_MSGPACK,
                                }
                            }
                            Err(e) => WireRequest::ParseError {
                                reason: format!("msgpack envelope parse failed: {e}"),
                            },
                        }
                    }
                    other => WireRequest::ParseError {
                        reason: format!("unsupported content_type: {other:#04x}"),
                    },
                }
            }
            // TCP /get variants — body is opaque to the parser; dispatch
            // (apply_shard) deserialises the JSON / MsgPack body into
            // {feature, key} or {keys, features}.
            OP_GET => match frame.content_type {
                CT_JSON | CT_MSGPACK => WireRequest::TcpGet {
                    body: frame.payload,
                    body_format: frame.content_type,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            },
            OP_MGET => match frame.content_type {
                CT_JSON | CT_MSGPACK => WireRequest::TcpMGet {
                    body: frame.payload,
                    body_format: frame.content_type,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            },
            OP_GET_MULTI => match frame.content_type {
                CT_JSON | CT_MSGPACK => WireRequest::TcpGetMulti {
                    body: frame.payload,
                    body_format: frame.content_type,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            },
            // OP_BATCH_GET (0x0024) — heterogeneous batched read. Body shape
            // `{"requests":[{"table","entity_id"}, ...]}` is opaque to the parser;
            // dispatch (`apply_shard.rs::dispatch_batch_get_sync`) deserialises
            // per body_format.
            OP_BATCH_GET => match frame.content_type {
                CT_JSON | CT_MSGPACK => WireRequest::TcpBatchGet {
                    body: frame.payload,
                    body_format: frame.content_type,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            },
            // OP_RESET (0x0040) — full state + registry clear, gated server-side
            // on test_mode. Body is empty `{}`; the parser is body-shape-agnostic
            // (dispatch tolerates any body for compat).
            OP_RESET => match frame.content_type {
                CT_JSON | CT_MSGPACK => WireRequest::TcpReset {
                    body: frame.payload,
                    body_format: frame.content_type,
                },
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            },
            op => WireRequest::Unknown { op },
        };
        req
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beava_core::wire::{CT_JSON, CT_MSGPACK};

    #[test]
    fn test_tcp_push_carries_body_format() {
        let body = Bytes::from_static(b"\x82\xa5event\xa3Txn\xa4body\x81\xa6amount\x63");
        let req = WireRequest::TcpPush {
            event_name: "Txn".to_string(),
            body: body.clone(),
            body_format: CT_MSGPACK,
        };
        match req {
            WireRequest::TcpPush {
                event_name,
                body: b,
                body_format,
            } => {
                assert_eq!(event_name, "Txn");
                assert_eq!(b, body);
                assert_eq!(body_format, CT_MSGPACK);
                assert_ne!(body_format, CT_JSON);
            }
            other => panic!("expected TcpPush, got {other:?}"),
        }
    }

    #[test]
    fn test_http_push_carries_body_format() {
        let body = Bytes::from_static(b"{\"amount\":99}");
        let req = WireRequest::HttpPush {
            event_name: "Txn".to_string(),
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::HttpPush {
                event_name: _,
                body: _,
                body_format,
            } => {
                assert_eq!(body_format, CT_JSON);
            }
            other => panic!("expected HttpPush, got {other:?}"),
        }
    }

    #[test]
    fn test_tcp_get_carries_body_format() {
        let body = Bytes::from_static(br#"{"feature":"cnt","key":"alice"}"#);
        let req = WireRequest::TcpGet {
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::TcpGet {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_JSON);
                assert_ne!(body_format, CT_MSGPACK);
            }
            other => panic!("expected TcpGet, got {other:?}"),
        }
    }

    #[test]
    fn test_tcp_mget_carries_body_format() {
        let body = Bytes::from_static(b"\x82\xa7feature\xa3cnt\xa4keys\x91\xa5alice");
        let req = WireRequest::TcpMGet {
            body: body.clone(),
            body_format: CT_MSGPACK,
        };
        match req {
            WireRequest::TcpMGet {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_MSGPACK);
                assert_ne!(body_format, CT_JSON);
            }
            other => panic!("expected TcpMGet, got {other:?}"),
        }
    }

    #[test]
    fn test_tcp_get_multi_carries_body_format() {
        let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
        let req = WireRequest::TcpGetMulti {
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::TcpGetMulti {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_JSON);
                assert_ne!(body_format, CT_MSGPACK);
            }
            other => panic!("expected TcpGetMulti, got {other:?}"),
        }
    }
}
