use std::net::{IpAddr, Ipv4Addr};

use beava_server::net::server::{self, Server};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let console_layer = console_subscriber::spawn();
    // let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(console_layer)
        // .with(fmt_layer)
        .init();

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
