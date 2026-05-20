//! Open-loop PING latency benchmark for the beava TCP server.
//!
//! Sends PING frames on a fixed schedule (one per `connections/rps` seconds per
//! connection) and records the round-trip latency from the *scheduled* send time
//! to response receipt.  Using the scheduled time rather than the actual send
//! time avoids the coordinated-omission problem: if the server is slow and
//! requests queue up, the full queuing delay shows up in the latency histogram
//! rather than being silently discarded.
//!
//! With `--rps 0` (the default) requests are sent as fast as possible, giving
//! a closed-loop baseline.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PING};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::{stream::FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const MAX_FRAME_BYTES: u32 = 1 << 14; // 16 KiB — matches the server default
const HIST_SIGFIGS: u8 = 3;
const HIST_MAX_US: u64 = 60_000_000; // 60 s expressed in microseconds

#[derive(Parser, Debug)]
#[command(about = "Open-loop PING latency benchmark for the beava TCP server")]
struct Cli {
    /// Server TCP address (wire-protocol port; default 8081)
    #[arg(long, default_value = "127.0.0.1:8081")]
    addr: SocketAddr,

    /// Number of concurrent TCP connections
    #[arg(long, default_value_t = 1)]
    connections: usize,

    /// Target requests per second across all connections combined (0 = max rate)
    #[arg(long, default_value_t = 0)]
    rps: u64,

    /// Measurement window duration (after warmup)
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30s")]
    duration: Duration,

    /// Warmup period excluded from measurements
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5s")]
    warmup: Duration,
}

/// Drives one TCP connection for the full `warmup + duration` window.
///
/// Returns the latency histogram for the measurement window only.
async fn run_worker(
    addr: SocketAddr,
    ping: Bytes,
    // Inter-request interval for this connection; `None` = max rate.
    interval: Option<Duration>,
    end: Instant,
    recording: Arc<AtomicBool>,
) -> Result<Histogram<u64>> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, HIST_MAX_US, HIST_SIGFIGS)?;

    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // The sender forwards the *scheduled* send time through this channel so
    // the receiver can compute latency that includes any queuing delay.
    let (ts_tx, mut ts_rx) = tokio::sync::mpsc::channel::<Instant>(4096);

    let sender = tokio::spawn(async move {
        let mut next = Instant::now();
        while Instant::now() < end {
            // Pace to the scheduled deadline when a rate limit is set.
            let scheduled = if let Some(iv) = interval {
                let now = Instant::now();
                if now < next {
                    tokio::time::sleep(next - now).await;
                }
                let t = next;
                next += iv;
                t
            } else {
                Instant::now()
            };

            if ts_tx.send(scheduled).await.is_err() {
                break;
            }
            if write_half.write_all(&ping).await.is_err() {
                break;
            }
        }
    });

    // For a single TCP stream, responses always arrive in the same order as
    // requests, so a simple FIFO match is correct and sufficient.
    let mut buf = BytesMut::with_capacity(8 * 1024);
    'outer: while let Some(scheduled) = ts_rx.recv().await {
        loop {
            match decode_frame(&mut buf, MAX_FRAME_BYTES) {
                Ok(Some(frame)) => {
                    dbg!(frame);
                    break;
                }
                Ok(None) => {
                    if read_half.read_buf(&mut buf).await.unwrap_or(0) == 0 {
                        break 'outer;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        if recording.load(Ordering::Acquire) {
            let us = (scheduled.elapsed().as_micros() as u64).clamp(1, HIST_MAX_US);
            hist.record(us).ok();
        }
    }

    let _ = sender.await;
    Ok(hist)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Per-connection interval derived from the global RPS target.
    let interval =
        (cli.rps > 0).then(|| Duration::from_secs_f64(cli.connections as f64 / cli.rps as f64));

    let end = Instant::now() + cli.warmup + cli.duration;
    let ping = {
        let frame = Frame::new(OP_PING, CT_JSON, Bytes::new());
        let mut buf = BytesMut::with_capacity(7);
        encode_frame(&frame, &mut buf);
        buf.freeze()
    };

    let recording = Arc::new(AtomicBool::new(false));

    eprintln!(
        "beava-ping-bench: addr={} connections={} rps={} warmup={:?} duration={:?}",
        cli.addr,
        cli.connections,
        if cli.rps == 0 {
            "max".to_string()
        } else {
            cli.rps.to_string()
        },
        cli.warmup,
        cli.duration,
    );

    {
        let rec = recording.clone();
        let warmup = cli.warmup;
        tokio::spawn(async move {
            tokio::time::sleep(warmup).await;
            rec.store(true, Ordering::Release);
            eprintln!("beava-ping-bench: warmup done, recording");
        });
    }

    let mut workers = FuturesUnordered::new();
    for _ in 0..cli.connections {
        workers.push(tokio::spawn(run_worker(
            cli.addr,
            ping.clone(),
            interval,
            end,
            recording.clone(),
        )));
    }

    let mut combined = Histogram::<u64>::new_with_bounds(1, HIST_MAX_US, HIST_SIGFIGS)?;
    while let Some(worker) = workers.next().await {
        combined.add(worker??)?;
    }

    let samples = combined.len();
    let throughput = samples as f64 / cli.duration.as_secs_f64();

    println!("samples:    {samples}");
    println!("throughput: {throughput:.0} req/s");
    println!("min:        {:.3} ms", combined.min() as f64 / 1_000.0);
    println!("mean:       {:.3} ms", combined.mean() / 1_000.0);
    println!(
        "p50:        {:.3} ms",
        combined.value_at_quantile(0.50) as f64 / 1_000.0
    );
    println!(
        "p90:        {:.3} ms",
        combined.value_at_quantile(0.90) as f64 / 1_000.0
    );
    println!(
        "p99:        {:.3} ms",
        combined.value_at_quantile(0.99) as f64 / 1_000.0
    );
    println!(
        "p99.9:      {:.3} ms",
        combined.value_at_quantile(0.999) as f64 / 1_000.0
    );
    println!("max:        {:.3} ms", combined.max() as f64 / 1_000.0);

    Ok(())
}
