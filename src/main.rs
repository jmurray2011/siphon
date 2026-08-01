//! `siphon` - mirror HTTP APIs into a local database, scheduled against a
//! measured rate-limit budget.

use clap::Parser;
use siphon::cli::Cli;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match Cli::parse().execute().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // Printed rather than returned so the whole cause chain is visible;
            // `Termination` for `anyhow::Error` shows only the outermost layer.
            eprintln!("error: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
