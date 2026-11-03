// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

mod catalog;
mod config;
mod journal;
mod server;

use clap::Parser;
use std::{io::Read, path::PathBuf};
use tokio::signal::unix::{SignalKind, signal};

#[derive(Parser)]
#[command(
    version,
    about = "First-party analytics collector; one private stream per site/environment"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Validate configuration and catalogs without opening the log or listener
    #[arg(long)]
    check: bool,
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut bytes = Vec::new();
    std::fs::File::open(args.config)?
        .take(65537)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("collector configuration exceeds 64 KiB".into());
    }
    let config: config::Config = serde_json::from_slice(&bytes)
        .map_err(|_| "invalid collector configuration JSON/schema")?;
    config.validate()?;
    let registry = catalog::Registry::load(&config)?;
    if args.check {
        eprintln!("Collector configuration and catalogs valid");
        return Ok(());
    }
    let journal = journal::Journal::open(&config)?;
    if journal.recovered_partial {
        eprintln!("WARNING: incomplete unacknowledged log tail preserved as recovery evidence");
    }
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let (app, writer) = server::App::start(config, registry, journal)?;
    let mut reload = signal(SignalKind::hangup())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    eprintln!("Analytics collector ready; private health endpoint available on loopback");
    let result = {
        let serving = server::serve(listener, app.clone());
        tokio::pin!(serving);
        loop {
            tokio::select! {
                result = &mut serving => break result,
                _ = terminate.recv() => break Ok(()),
                _ = interrupt.recv() => break Ok(()),
                _ = reload.recv() => {
                    let reload_app = app.clone();
                    match tokio::task::spawn_blocking(move || reload_app.reload()).await? {
                        Ok(()) => eprintln!("Analytics catalogs reloaded"),
                        Err(error) => eprintln!("ERROR: analytics catalog reload rejected; previous registry retained: {error}"),
                    }
                }
            }
        }
    };
    drop(app);
    tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| "analytics writer thread panicked")??;
    result?;
    Ok(())
}
