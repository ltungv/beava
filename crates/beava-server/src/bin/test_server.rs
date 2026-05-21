use std::net::{IpAddr, Ipv4Addr};

use beava_server::net::server::{self, Server};
use tokio::signal;
use tracing::subscriber::set_global_default;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let console_layer = console_subscriber::spawn();

    // build a `Subscriber` by combining layers with a
    // `tracing_subscriber::Registry`:
    tracing_subscriber::registry()
        // add the console layer to the subscriber
        .with(console_layer)
        // add other layers...
        .with(
            FmtSubscriber::builder()
                .with_env_filter(env_filter)
                .finish(),
        )
        .init();

    set_global_default(subscriber).expect("Failed to set subscriber.");

    let server = Server::new(
        signal::ctrl_c(),
        server::Configuration {
            host: IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
            port: 8888,
            min_backoff_ms: 125,
            max_backoff_ms: 64000,
            max_connections: 1024,
        },
    )
    .await?;

    server.run().await;
    Ok(())
}
