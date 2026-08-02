//! The command line, and the wiring that every command shares.

pub mod run;
pub mod status;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand, ValueEnum};

use crate::budget::Budget;
use crate::clock::{Clock, SystemClock};
use crate::config::Config;
use crate::governor::{AdmissionPolicy, Governor, StreamState};
use crate::http::SourceClient;
use crate::store::Store;
use crate::sync::{RunOptions, SyncWorker};

/// The default config path when none is given.
const DEFAULT_CONFIG: &str = "siphon.yaml";

/// Mirror HTTP APIs into a local database, scheduled against a measured
/// rate-limit budget.
#[derive(Debug, Parser)]
#[command(name = "siphon", version, about, long_about = None)]
pub struct Cli {
    /// Path to the config file.
    #[arg(short, long, global = true, default_value = DEFAULT_CONFIG)]
    config: PathBuf,

    /// How to format logs.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Pretty)]
    log_format: LogFormat,

    #[command(subcommand)]
    command: Command,
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable.
    Pretty,
    /// One JSON object per line.
    Json,
}

/// What siphon was asked to do.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run as a daemon; the governor schedules every stream.
    Run {
        /// Config file, if not the one given by `--config`.
        // Named apart from the global `--config` so the two argument ids do not
        // collide, which clap reports as an unexpected `--config`.
        #[arg(value_name = "CONFIG")]
        config_file: Option<PathBuf>,
    },
    /// Sync one stream once, bypassing admission.
    Sync {
        /// The stream to sync.
        stream: String,
        /// Ignore the stored cursor and resume point, and walk from the start.
        #[arg(long)]
        full: bool,
    },
    /// Fetch a page and print what would be written, without writing it.
    Test {
        /// The stream to test.
        stream: String,
        /// How many pages to fetch.
        #[arg(long, default_value_t = 1)]
        pages: u32,
    },
    /// Parse and check the config. Makes no network calls.
    Validate {
        /// Config file, if not the one given by `--config`.
        #[arg(value_name = "CONFIG")]
        config_file: Option<PathBuf>,
    },
    /// Per-stream SLO attainment, lag and budget.
    Status,
}

impl Cli {
    /// Runs the requested command.
    ///
    /// # Errors
    ///
    /// Fails if the config is invalid, the store cannot be opened, or a
    /// credential cannot be resolved.
    pub async fn execute(self) -> anyhow::Result<()> {
        init_tracing(self.log_format);

        match &self.command {
            Command::Validate { config_file } => {
                status::validate(&self.config_path(config_file.as_deref()))
            }
            Command::Run { config_file } => {
                let app =
                    App::build(&self.config_path(config_file.as_deref()), Storage::OnDisk).await?;
                run::daemon(app).await
            }
            Command::Sync { stream, full } => {
                let app = App::build(&self.config, Storage::OnDisk).await?;
                run::once(&app, stream, *full).await
            }
            Command::Test { stream, pages } => {
                let app = App::build(&self.config, Storage::Ephemeral).await?;
                run::probe(&app, stream, *pages).await
            }
            Command::Status => {
                let app = App::build(&self.config, Storage::OnDisk).await?;
                status::report(&app).await
            }
        }
    }

    fn config_path(&self, positional: Option<&Path>) -> PathBuf {
        positional.map_or_else(|| self.config.clone(), Path::to_path_buf)
    }
}

/// Whether a command writes to the real mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    /// The configured SQLite file.
    OnDisk,
    /// A throwaway in-memory database, for `siphon test`.
    Ephemeral,
}

/// Everything a command needs, wired together.
pub struct App {
    /// The validated config.
    pub config: Config,
    /// The mirror.
    pub store: Store,
    /// Admission and pacing.
    pub governor: Arc<Governor>,
    /// The stream driver.
    pub worker: Arc<SyncWorker>,
    /// The clock everything reads time from.
    pub clock: Arc<dyn Clock>,
}

impl App {
    /// Loads the config and builds the store, clients, governor and worker.
    ///
    /// Credentials resolve here, before any request goes out, so a missing token
    /// is a startup failure rather than a 401 discovered three pages in.
    ///
    /// # Errors
    ///
    /// Fails if the config is invalid, the store cannot be opened, or a
    /// credential cannot be resolved.
    pub async fn build(config_path: &Path, storage: Storage) -> anyhow::Result<Self> {
        let config = Config::load(config_path)
            .with_context(|| format!("loading config from {}", config_path.display()))?;

        for lint in config.lint() {
            tracing::warn!(subject = %lint.subject, "{}", lint.message);
        }

        let store = match storage {
            Storage::OnDisk => Store::open(&config.store.path)
                .with_context(|| format!("opening store at {}", config.store.path.display()))?,
            Storage::Ephemeral => Store::open_in_memory().context("opening in-memory store")?,
        };

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let now = clock.now();

        let mut clients = BTreeMap::new();
        let mut budgets = BTreeMap::new();
        for (name, source) in &config.sources {
            clients.insert(
                name.clone(),
                // No added context: every `HttpError` already names the source
                // it failed on, and restating it here reads as two failures.
                Arc::new(SourceClient::new(Arc::clone(source))?),
            );

            // A learned ceiling survives restarts, so a deploy does not cost a
            // round of 429s to rediscover the limit.
            let budget = match store.load_budget(name).await? {
                Some(snapshot) => Budget::restore(source.budget.clone(), snapshot, now),
                None => Budget::new(source.budget.clone()),
            };
            budgets.insert(name.clone(), budget);
        }

        let checkpoints = store.checkpoints().await?;
        let streams = config
            .streams
            .iter()
            .map(|stream| StreamState {
                name: stream.name.clone(),
                source: stream.source.clone(),
                freshness: stream.freshness,
                priority: stream.priority,
                last_success_at: checkpoints
                    .get(&stream.name)
                    .and_then(|c| c.last_success_at),
                running: false,
            })
            .collect();

        let governor = Arc::new(Governor::new(
            Arc::clone(&clock),
            AdmissionPolicy::default(),
            budgets,
            streams,
        ));
        let worker = Arc::new(SyncWorker::new(
            store.clone(),
            Arc::clone(&governor),
            Arc::clone(&clock),
            clients,
        ));

        Ok(Self {
            config,
            store,
            governor,
            worker,
            clock,
        })
    }

    /// Looks up a stream by name.
    ///
    /// # Errors
    ///
    /// Fails if no stream by that name is configured.
    pub fn stream(&self, name: &str) -> anyhow::Result<Arc<crate::config::Stream>> {
        self.config.stream(name).map(Arc::clone).ok_or_else(|| {
            let known: Vec<&str> = self
                .config
                .streams
                .iter()
                .map(|s| s.name.as_str())
                .collect();
            anyhow!(
                "unknown stream `{name}`; configured streams: {}",
                known.join(", ")
            )
        })
    }

    /// Writes every source's learned budget back to the store.
    ///
    /// # Errors
    ///
    /// Fails if the store rejects the write.
    pub async fn persist_budgets(&self) -> anyhow::Result<()> {
        let now = self.clock.now();
        for (source, snapshot) in self.governor.budget_snapshots() {
            self.store.save_budget(&source, snapshot, now).await?;
        }
        Ok(())
    }
}

/// Default options for a scheduled run.
#[must_use]
pub fn scheduled(deficit: f64) -> RunOptions {
    RunOptions {
        deficit,
        full: false,
    }
}

fn init_tracing(format: LogFormat) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_env("SIPHON_LOG")
        .or_else(|_| EnvFilter::try_new("siphon=info,warn"))
        .unwrap_or_default();

    let builder = fmt().with_env_filter(filter).with_target(false);
    let installed = match format {
        LogFormat::Json => builder.json().try_init(),
        LogFormat::Pretty => builder.try_init(),
    };
    // A second initialisation only happens in tests, where the first one wins.
    drop(installed);
}
