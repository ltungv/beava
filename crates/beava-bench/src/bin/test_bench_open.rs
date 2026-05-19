use std::collections::VecDeque;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PING};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hdrhistogram::Histogram;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::LocalOptions;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tokio_util::task::LocalPoolHandle;

const SIGNIFICANT_DECIMAL_DIGITS: u8 = 4;
const MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "bench_client",
    about = "Open-loop load generator for the beava wire protocol"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    #[arg(long, value_parser = humantime::parse_duration, default_value= "1m")]
    duration: Duration,

    #[arg(long, default_value_t = 64)]
    connections: usize,

    #[arg(long, default_value_t = 4)]
    threads: usize,

    #[arg(long, default_value_t = 500_000)]
    rps: u64,
}

fn main() {
    let args = Args::parse();
    println!("{args:?}");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build_local(LocalOptions::default())
        .unwrap();

    let measurements: Vec<Histogram<u64>> = runtime.block_on(run(&args));

    let mut total_requests: u64 = 0;
    let mut combined_round_trip = Histogram::<u64>::new_with_max(
        Duration::from_secs(60).as_micros().try_into().unwrap(),
        SIGNIFICANT_DECIMAL_DIGITS,
    )
    .unwrap();

    for round_trip_durations in measurements.into_iter() {
        total_requests += round_trip_durations.len();
        combined_round_trip = combined_round_trip.add(round_trip_durations);
    }

    let actual_rps = total_requests as f64 / args.duration.as_secs_f64();

    println!("Total requests: {}", total_requests);
    println!("Actual RPS: {:.2}", actual_rps);

    println!("Latency (µs):");
    println!("  avg: {:.2}", combined_round_trip.mean());
    println!("  p50: {}", combined_round_trip.value_at_quantile(0.50));
    println!("  p95: {}", combined_round_trip.value_at_quantile(0.95));
    println!("  p99: {}", combined_round_trip.value_at_quantile(0.99));
    println!("  max: {}", combined_round_trip.max());
}

async fn run(args: &Args) -> Vec<Histogram<u64>> {
    let local_pool = LocalPoolHandle::new(args.threads);

    let base_rps_per_conn = args.rps / args.connections as u64;
    let extra_rps = args.rps % args.connections as u64;

    let barrier = Arc::new(Barrier::new(args.connections));
    let reader_tasks = FuturesUnordered::new();
    let writer_tasks = FuturesUnordered::new();

    for i in 0..args.connections {
        let conn_rps = if (i as u64) < extra_rps {
            base_rps_per_conn + 1
        } else {
            base_rps_per_conn
        };

        let interval_micros = 1_000_000 / conn_rps;
        println!("rps={} interval={}us", conn_rps, interval_micros);

        let duration = args.duration;
        let barrier = barrier.clone();

        let stream = match TcpStream::connect(args.addr.clone()).await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("connect error: {e}");
                return Vec::new();
            }
        };
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("set nodelay error: {e}");
        }

        let (frame_start_tx, frame_start_rx) = mpsc::channel::<Instant>(4096 * 4);

        let (read_half, write_half) = tokio::io::split(stream);
        let reader_thread = i % args.threads;
        let writer_thread = (i + (args.threads / 2).max(1)) % args.threads;

        reader_tasks.push(local_pool.spawn_pinned_by_idx(
            move || reader_task(read_half, frame_start_rx),
            reader_thread,
        ));

        writer_tasks.push(local_pool.spawn_pinned_by_idx(
            move || {
                writer_task(
                    write_half,
                    frame_start_tx,
                    barrier,
                    interval_micros,
                    duration,
                )
            },
            writer_thread,
        ));
    }

    // writer_tasks.map(|t| t.unwrap()).collect::<Vec<_>>().await;
    reader_tasks.map(|t| t.unwrap()).collect::<Vec<_>>().await
}

async fn writer_task(
    mut stream: tokio::io::WriteHalf<TcpStream>,
    frame_start_tx: mpsc::Sender<Instant>,
    barrier: Arc<Barrier>,
    interval_micros: u64,
    duration: Duration,
) {
    let frame = Frame::new(OP_PING, CT_JSON, Bytes::new());
    let frame_buf = {
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        buf.freeze()
    };

    let mut rng = rand::thread_rng();
    let jitter = rng.gen_range(0..interval_micros);
    barrier.wait().await;
    tokio::time::sleep(Duration::from_micros(jitter)).await;

    let loop_end = Instant::now().add(duration);
    let mut ticker = tokio::time::interval(Duration::from_micros(interval_micros));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let now = Instant::now();
        if now >= loop_end {
            break;
        }
        if let Err(e) = stream.write_all(&frame_buf).await {
            eprintln!("write error: {e}");
            break;
        }
        if let Err(e) = stream.flush().await {
            eprintln!("flush error: {e}");
            break;
        }
        if let Err(e) = frame_start_tx.send(now).await {
            eprintln!("frame start error: {e}");
            break;
        }
    }
    let _ = stream.shutdown().await;
}

async fn reader_task(
    mut stream: tokio::io::ReadHalf<TcpStream>,
    mut frame_start_rx: mpsc::Receiver<Instant>,
) -> Histogram<u64> {
    let mut tx_closed = false;
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut pending_decoded: VecDeque<Instant> = VecDeque::with_capacity(4096 * 4);
    let mut pending_frame_start: VecDeque<Instant> = VecDeque::with_capacity(4096 * 4);
    let mut histogram = Histogram::<u64>::new_with_max(
        Duration::from_secs(60).as_micros().try_into().unwrap(),
        SIGNIFICANT_DECIMAL_DIGITS,
    )
    .unwrap();

    loop {
        // Drain all complete frames that are already in the buffer.
        loop {
            match decode_frame(&mut read_buf, MAX_FRAME_BYTES) {
                Ok(Some(_)) => {
                    if let Some(frame_start) = pending_frame_start.pop_front() {
                        histogram
                            .record(frame_start.elapsed().as_micros().try_into().unwrap())
                            .unwrap();
                    } else {
                        pending_decoded.push_back(Instant::now());
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("decode error: {e}");
                    return histogram;
                }
            }
        }

        // When the sender is closed and all pending timestamps are matched, we're done.
        if tx_closed && pending_frame_start.is_empty() {
            break;
        }

        select! {
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(n) => if n == 0 {
                        break;
                    },
                    Err(e) => {
                        eprintln!("read error: {e}");
                        break;
                    }
                }
            }
            recv = frame_start_rx.recv(), if !tx_closed => {
                match recv {
                    Some(frame_start) => {
                        if let Some(frame_parsed) = pending_decoded.pop_front() {
                            histogram
                                .record((frame_parsed - frame_start).as_micros().try_into().unwrap())
                                .unwrap();
                        } else {
                            pending_frame_start.push_back(frame_start);
                        }
                    }
                    None => {
                        tx_closed = true;
                    }
                }
            }
        }
    }

    histogram
}
