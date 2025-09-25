use std::net::ToSocketAddrs;
use std::sync::mpsc::channel;
use std::error::Error;

use clap::Parser;

mod tokad;
mod tui;

use crate::tui::start_console;
use crate::tokad::start_server;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(default_value_t = 50051)]
    port: u16,

    seed: Option<String>,
    #[arg(short, long)]
    verbose: bool
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args = Args::parse();

    let seed = match args.seed {
        Some(seed) => {
            let addrs : Vec<_> = seed.to_socket_addrs()?.collect();
            if addrs.is_empty() {
                return Err("Init address cannot be resolved".into());
            }
            Some(addrs[0])
        }
        None => None
    };

    let (con_snd, con_rcv) = channel();
    let (state, _server_handle) = start_server(args.port, Some(con_snd), seed)?;
    let console_handle = start_console(Some(state), Some(con_rcv));
    console_handle.await??;
    Ok(())
}
