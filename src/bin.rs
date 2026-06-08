use std::error::Error;
use std::net::ToSocketAddrs;

use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod data;
mod error;
mod hash;
mod tokad;
mod tui;

use crate::tokad::start_server;
use crate::tui::run_tui;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(default_value_t = 50051)]
    port: u16,

    seed: Option<String>,
    #[arg(short, long)]
    verbose: bool,
    #[arg(short, long)]
    daemon: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    use tracing_subscriber::Layer;

    let args = Args::parse();

    let seed = match args.seed {
        Some(seed) => {
            let addrs: Vec<_> = seed
                .to_socket_addrs()
                .or_else(|_| (seed, 50051).to_socket_addrs())?
                .collect();
            if addrs.is_empty() {
                return Err("Init address cannot be resolved".into());
            }
            Some(addrs[0])
        }
        None => None,
    };

    if args.daemon {
        let filter = tracing_subscriber::filter::Targets::new()
            .with_target("tokad::tokad", tracing_subscriber::filter::LevelFilter::INFO);

        #[cfg(all(tokio_unstable, feature = "tokio-console"))]
        let registry = tracing_subscriber::registry()
            .with(console_subscriber::ConsoleLayer::builder().with_default_env().spawn())
            .with(tracing_subscriber::fmt::layer().with_filter(filter));

        #[cfg(not(all(tokio_unstable, feature = "tokio-console")))]
        let registry = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(filter));

        registry.init();
        let (_state, server_handle, _loop_handle) = start_server(args.port, seed)?;
        server_handle.await??;
    } else {
        // Initialize tui-tracing layer and viewer with tokad::tokad module filter
        let (tui_layer, store) = tui_tracing::TraceLayer::new();
        let filter = tracing_subscriber::filter::Targets::new()
            .with_target("tokad::tokad", tracing_subscriber::filter::LevelFilter::TRACE);

        #[cfg(all(tokio_unstable, feature = "tokio-console"))]
        let registry = tracing_subscriber::registry()
            .with(console_subscriber::ConsoleLayer::builder().with_default_env().spawn())
            .with(tui_layer.with_filter(filter));

        #[cfg(not(all(tokio_unstable, feature = "tokio-console")))]
        let registry = tracing_subscriber::registry()
            .with(tui_layer.with_filter(filter));

        registry.init();
        let mut traces = tui_tracing::TraceViewer::new(store);
        traces.set_filter(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::INFO));

        let (state, _server_handle, _loop_handle) = start_server(args.port, seed)?;
        run_tui(state, traces).await?;
    }

    Ok(())
}
