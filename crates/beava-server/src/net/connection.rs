//! A wrapper that turns a stream of bytes into a stream of frames.

use beava_core::wire::{decode_frame, Frame};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

/// Sends and receives [`Frame`] values from the remote peer.
///
/// Wraps any async read+write stream with buffered I/O and exposes a
/// frame-level interface. Internally uses [`Frame::check`] to validate
/// completeness without allocating before consuming bytes from the read
/// buffer — the same pattern used by `bitcask::net::Connection`.
pub struct Connection<S = TcpStream> {
    /// Wraps a stream inside a `BufWriter` to reduce the number of write syscalls.
    stream: BufWriter<S>,
    /// Buffered data from read operations.
    buffer: BytesMut,
    /// Maximum payload size (in bytes) that a single frame may carry.
    max_frame_bytes: u32,
}

impl<S> Connection<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    /// Creates a new connection over the given readable and writable stream,
    /// then initialises the inner read/write buffers.
    pub fn new(stream: S, max_frame_bytes: u32) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(8 * 1024),
            max_frame_bytes,
        }
    }

    /// Reads the next complete frame from the underlying stream.
    ///
    /// Returns the received frame if one could be parsed. When the
    /// underlying stream is closed and there is no data left to read,
    /// returns `Ok(None)`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, super::Error> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buffer, self.max_frame_bytes)? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    // Peer closed cleanly — all data has been consumed.
                    return Ok(None);
                } else {
                    // Peer closed mid-frame.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "connection reset by peer",
                    )
                    .into());
                }
            }
        }
    }

    /// Writes a frame to the underlying stream and flushes.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), super::Error> {
        const HEADER_LEN: usize = size_of::<u16>() + size_of::<u8>();
        let payload_len = frame.payload.len();
        debug_assert!(
            payload_len <= (u32::MAX as usize) - HEADER_LEN,
            "payload too large for u32 length prefix"
        );
        let total_len = (payload_len + HEADER_LEN) as u32;
        self.stream.write_u32(total_len).await?;
        self.stream.write_u16(frame.op).await?;
        self.stream.write_u8(frame.content_type).await?;
        self.stream.write_all(&frame.payload).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beava_core::wire::{
        encode_frame, CT_JSON, CT_MSGPACK, OP_ERROR_RESPONSE, OP_GET, OP_PING, OP_PUSH,
    };
    use bytes::Bytes;
    use std::io::Cursor;

    #[tokio::test]
    async fn write_frame_check_sent_buffer() {
        for (frame, expected_bytes) in get_test_cases() {
            assert_frame_write(frame, expected_bytes).await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_frame_check_received_frame() {
        for (expected_frame, wire_bytes) in get_test_cases() {
            assert_frame_read(wire_bytes, expected_frame).await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_frame_returns_none_on_clean_close() {
        let stream = Cursor::new(Vec::new());
        let mut conn = Connection::new(stream, 4 * 1024 * 1024);
        let frame = conn.read_frame().await.unwrap();
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn read_frame_returns_error_on_partial_close() {
        // Only the length prefix, then EOF — incomplete frame.
        let stream = Cursor::new(vec![0u8, 0, 0, 5]);
        let mut conn = Connection::new(stream, 4 * 1024 * 1024);
        let err = conn.read_frame().await.unwrap_err();
        assert!(
            matches!(err, super::super::Error::Io(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset),
            "expected ConnectionReset, got {err:?}"
        );
    }

    #[tokio::test]
    async fn read_two_frames_from_single_buffer() {
        // Two complete frames concatenated.
        let mut wire = BytesMut::new();
        let f1 = Frame::new(OP_PING, CT_JSON, Bytes::new());
        let f2 = Frame::new(OP_PUSH, CT_JSON, b"hello".as_slice());
        encode_frame(&f1, &mut wire);
        encode_frame(&f2, &mut wire);

        let stream = Cursor::new(wire.to_vec());
        let mut conn = Connection::new(stream, 4 * 1024 * 1024);

        let got1 = conn.read_frame().await.unwrap().unwrap();
        assert_eq!(got1, f1);
        let got2 = conn.read_frame().await.unwrap().unwrap();
        assert_eq!(got2, f2);
        // stream drained
        assert!(conn.read_frame().await.unwrap().is_none());
    }

    async fn assert_frame_write(
        frame: Frame,
        expected_buffer: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = Cursor::new(Vec::new());
        let mut conn = Connection::new(&mut stream, 4 * 1024 * 1024);

        conn.write_frame(&frame).await?;
        assert_eq!(stream.get_ref(), expected_buffer);

        Ok(())
    }

    async fn assert_frame_read(
        buf: &[u8],
        expected_frame: Frame,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream = Cursor::new(Vec::from(buf));
        let mut conn = Connection::new(stream, 4 * 1024 * 1024);

        let frame = conn.read_frame().await?;
        assert_eq!(frame, Some(expected_frame));

        Ok(())
    }

    /// Test cases: (Frame, expected wire bytes).
    ///
    /// Wire layout: `[u32 BE length][u16 BE op][u8 ct][payload]`
    /// where `length = 3 + payload.len()`.
    fn get_test_cases() -> Vec<(Frame, &'static [u8])> {
        vec![
            // Ping with empty payload
            (
                Frame::new(OP_PING, CT_JSON, Bytes::new()),
                // len=3, op=0x0000, ct=0x01
                &[0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x01],
            ),
            // Push with JSON payload
            (
                Frame::new(OP_PUSH, CT_JSON, &b"{}"[..]),
                // len=5, op=0x0010, ct=0x01, payload="{}"
                &[0x00, 0x00, 0x00, 0x05, 0x00, 0x10, 0x01, b'{', b'}'],
            ),
            // Get with JSON payload
            (
                Frame::new(OP_GET, CT_JSON, &b"key"[..]),
                // len=6, op=0x0020, ct=0x01, payload="key"
                &[0x00, 0x00, 0x00, 0x06, 0x00, 0x20, 0x01, b'k', b'e', b'y'],
            ),
            // Error response
            (
                Frame::new(OP_ERROR_RESPONSE, CT_JSON, &b"err"[..]),
                // len=6, op=0xFFFF, ct=0x01, payload="err"
                &[0x00, 0x00, 0x00, 0x06, 0xFF, 0xFF, 0x01, b'e', b'r', b'r'],
            ),
            // MsgPack content type with empty payload
            (
                Frame::new(OP_PING, CT_MSGPACK, Bytes::new()),
                // len=3, op=0x0000, ct=0x02
                &[0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x02],
            ),
        ]
    }
}
