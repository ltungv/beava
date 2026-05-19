use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beava_core::wire::{Frame, CT_JSON, OP_PING};
use beava_server::net::connection::Connection;
use bytes::Bytes;
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hdrhistogram::Histogram;
use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::Barrier;

const SIGNIFICANT_DECIMAL_DIGITS: u8 = 4;
const MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "bench_client",
    about = "Closed-loop load generator for the beava wire protocol"
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
    rps: usize,
}

fn main() {
    let args = Args::parse();
    println!("{args:?}");

    let base_rps_per_conn = args.rps / args.connections;
    let extra_rps = args.rps % args.connections;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(args.threads)
        .build()
        .unwrap();

    let measurements = runtime.block_on(async move {
        let barrier = Arc::new(Barrier::new(args.connections));
        let tasks = FuturesUnordered::new();

        for i in 0..args.connections {
            let conn_rps = if i < extra_rps {
                base_rps_per_conn + 1
            } else {
                base_rps_per_conn
            };
            let addr = args.addr.clone();
            let barrier = barrier.clone();

            let mut rng = rand::thread_rng();
            let interval_micros = 1_000_000 / conn_rps as u64;
            println!("rps={} interval={}us", conn_rps, interval_micros);
            let jitter = rng.gen_range(0..interval_micros);
            let frame = Frame::new(OP_PING, CT_JSON, Bytes::new());

            tasks.push(async move {
                let mut round_trip_durations = Histogram::<u64>::new_with_max(
                    Duration::from_mins(1).as_micros().try_into().unwrap(),
                    SIGNIFICANT_DECIMAL_DIGITS,
                )
                .unwrap();

                let stream = match TcpStream::connect(addr).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("connect error: {e}");
                        return round_trip_durations;
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("set nodelay error: {e}");
                }
                let mut conn = Connection::new(stream, MAX_FRAME_BYTES);

                barrier.wait().await;
                tokio::time::sleep(std::time::Duration::from_micros(jitter)).await;

                let loop_end = Instant::now().add(args.duration);
                let mut ticker = tokio::time::interval(Duration::from_micros(interval_micros));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    ticker.tick().await;

                    let request_start = Instant::now();
                    if request_start >= loop_end {
                        break;
                    }

                    let elapsed = {
                        if let Err(e) = conn.write_frame(&frame).await {
                            eprintln!("write frame error: {e}");
                            continue;
                        }
                        let frame = match conn.read_frame().await {
                            Ok(frame) => frame,
                            Err(e) => {
                                eprintln!("read frame error: {e}");
                                continue;
                            }
                        };
                        if frame.is_none() {
                            break;
                        }
                        request_start.elapsed()
                    };

                    let round_trip_duration: u64 = elapsed.as_micros().try_into().unwrap();
                    round_trip_durations
                        .record_correct(round_trip_duration, interval_micros)
                        .unwrap();
                }
                round_trip_durations
            });
        }
        tasks.collect::<Vec<_>>().await
    });

    let mut total_requests: u64 = 0;
    let mut combined_round_trip = Histogram::<u64>::new_with_max(
        Duration::from_mins(1).as_micros().try_into().unwrap(),
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
    println!("  p95: {}", combined_round_trip.value_at_quantile(0.95));
    println!("  p99: {}", combined_round_trip.value_at_quantile(0.99));
}
