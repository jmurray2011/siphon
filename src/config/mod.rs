//! The config format, which is the actual product; everything else is
//! implementation.
//!
//! Loading is two steps. [`raw`] is what serde sees: the file's shape, with
//! unknown keys refused. This module turns that into the validated types the
//! rest of siphon uses, where a `Stream` cannot exist unless its JSONPaths
//! compile, its pagination block is coherent, and its `{{ vars }}` are declared.
//! A bad config never starts a run.
//!
//! The validated records are immutable: they are built only here and shared as
//! [`Arc`], so their fields are readable but not reachable for mutation.

pub mod duration;
pub mod raw;
pub mod secret;
pub mod template;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::budget::{BudgetConfigError, BudgetPolicy};
use crate::extract::{RecordSpec, RecordSpecError};
use crate::governor::Priority;
use crate::pagination::{PaginationConfigError, Paginator};

use duration::format_duration;
use raw::{ApiKeyLocation, Direction, RawAuth, RawConfig, RawRetry, WriteMode};
use secret::SecretRef;

/// The only config format version that exists.
const SUPPORTED_VERSION: u32 = 1;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(300);
const DEFAULT_QUARANTINE_AFTER: u32 = 3;

/// A validated config.
#[derive(Debug)]
pub struct Config {
    /// Where the mirror is written.
    pub store: StoreSpec,
    /// APIs to pull from, by name.
    pub sources: BTreeMap<String, Arc<Source>>,
    /// Endpoints to mirror, in file order.
    pub streams: Vec<Arc<Stream>>,
}

/// Where the mirror is written.
///
/// An enum rather than a struct with optional fields: the two targets need
/// disjoint information, and the choice is made once at startup from a config
/// file, which is exactly what an enum models.
#[derive(Debug, Clone)]
pub enum StoreSpec {
    /// A local SQLite file.
    Sqlite {
        /// Path to the database file.
        path: PathBuf,
    },
    /// A Postgres server.
    Postgres {
        /// How to reach it. Resolved at connect time, not here.
        url: secret::SecretRef,
    },
}

impl StoreSpec {
    /// A description safe to log: a Postgres connection string can carry a
    /// password, so only its origin is ever printed.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Sqlite { path } => path.display().to_string(),
            Self::Postgres { url } => format!("postgres ({})", url.origin()),
        }
    }
}

/// One API, with everything needed to talk to it.
#[derive(Debug)]
pub struct Source {
    /// The name it is referred to by.
    pub name: String,
    /// Scheme, host and any path prefix.
    pub base_url: url::Url,
    /// How to authenticate.
    pub auth: Auth,
    /// Headers sent with every request.
    pub headers: BTreeMap<String, String>,
    /// How the rate-limit budget is measured and spent.
    pub budget: BudgetPolicy,
    /// Whether to send conditional-request validators.
    pub conditional_requests: bool,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Retry behaviour for transient failures.
    pub retry: RetryPolicy,
}

/// How to authenticate to a source.
///
/// Secrets are held as references and resolved where they go onto the wire, so
/// `siphon validate` can check a config on a machine that holds no credentials.
#[derive(Debug, Clone)]
pub enum Auth {
    /// No credentials.
    None,
    /// `Authorization: Bearer <token>`.
    Bearer(SecretRef),
    /// HTTP basic auth.
    Basic {
        /// The user name.
        username: SecretRef,
        /// The password.
        password: SecretRef,
    },
    /// A key in a header or query parameter.
    ApiKey {
        /// Name of the header or parameter.
        name: String,
        /// The key.
        value: SecretRef,
        /// Where to put it.
        location: ApiKeyLocation,
    },
}

impl Auth {
    /// Every secret this auth method needs, for lint reporting.
    fn secrets(&self) -> Vec<&SecretRef> {
        match self {
            Self::None => Vec::new(),
            Self::Bearer(token) => vec![token],
            Self::Basic { username, password } => vec![username, password],
            Self::ApiKey { value, .. } => vec![value],
        }
    }
}

/// Retry behaviour for transient failures.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Attempts a single request gets, including the first.
    pub max_attempts: u32,
    /// Backoff before the second attempt.
    pub initial_backoff: Duration,
    /// Ceiling the backoff doubles up to.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

impl RetryPolicy {
    /// Backoff before attempt number `attempt`, counting the first as 1.
    ///
    /// Exponential with full jitter: `rand_unit` is a value in `[0, 1)` supplied
    /// by the caller, which keeps this deterministic under test while leaving
    /// the randomness where it belongs - at the edge.
    #[must_use]
    pub fn backoff(&self, attempt: u32, rand_unit: f64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(16);
        let ceiling = self
            .initial_backoff
            .saturating_mul(2u32.saturating_pow(exponent))
            .min(self.max_backoff);
        ceiling.mul_f64(rand_unit.clamp(0.0, 1.0))
    }
}

/// One endpoint to mirror.
#[derive(Debug)]
pub struct Stream {
    /// Unique name; also the key in the store.
    pub name: String,
    /// Which source this stream spends budget from.
    pub source: String,
    /// How stale this stream may get.
    pub freshness: Option<Duration>,
    /// What it is worth relative to the other streams.
    pub priority: Priority,
    /// Which way the walk runs.
    pub direction: Direction,
    /// The request to issue.
    pub request: RequestSpec,
    /// How to walk pages.
    pub paginator: Paginator,
    /// How to find records in the response.
    pub records: RecordSpec,
    /// Consecutive fatal failures on one page before it is quarantined.
    pub quarantine_after: u32,
}

impl Stream {
    /// True when this stream resumes from a stored high-water mark.
    ///
    /// A backward walk deliberately does not: it is working through history, so
    /// the cursor of the newest record it has seen tells it nothing about where
    /// its walk should resume.
    #[must_use]
    pub const fn is_incremental(&self) -> bool {
        matches!(self.direction, Direction::Forward)
    }
}

/// The request a stream issues, before pagination and cursor parameters.
#[derive(Debug)]
pub struct RequestSpec {
    /// Path appended to the source's `base_url`, with `{{ vars }}` resolved.
    pub path: String,
    /// Query parameters, with `{{ vars }}` resolved.
    ///
    /// A `{{ cursor }}` placeholder is deliberately *not* resolved here: it
    /// carries the high-water mark, which is only known per run.
    pub params: BTreeMap<String, String>,
    /// Headers for this stream only.
    pub headers: BTreeMap<String, String>,
    /// Whether any parameter interpolates the cursor.
    interpolates_cursor: bool,
}

impl RequestSpec {
    /// True when the high-water mark belongs inside a parameter rather than
    /// appended as one of its own.
    ///
    /// This is what an API with a query language needs: the mark is one clause
    /// of a JQL, SOQL or OData expression, not a parameter beside it.
    #[must_use]
    pub const fn interpolates_cursor(&self) -> bool {
        self.interpolates_cursor
    }
}

/// Why a config was rejected.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The `store` block does not match the backend it names.
    #[error("store: {message}")]
    Store {
        /// What is wrong with it.
        message: String,
    },
    /// The file could not be read.
    #[error("cannot read config `{path}`")]
    Read {
        /// The path that was tried.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The YAML did not parse, or did not match the schema.
    #[error("cannot parse config: {0}")]
    Parse(#[from] serde_saphyr::Error),
    /// The `version` key named a format siphon does not know.
    #[error("config version {found} is not supported; this build understands version {SUPPORTED_VERSION}")]
    Version {
        /// The version that was declared.
        found: u32,
    },
    /// `base_url` was not a URL.
    #[error("source `{source_name}` has an invalid base_url `{value}`: {cause}")]
    BaseUrl {
        /// The source that declared it.
        source_name: String,
        /// What was written.
        value: String,
        /// The parse failure.
        cause: url::ParseError,
    },
    /// A source's budget block was invalid.
    #[error("source `{source_name}`: {source}")]
    Budget {
        /// The source that declared it.
        source_name: String,
        /// The underlying failure.
        #[source]
        source: BudgetConfigError,
    },
    /// Two streams share a name.
    #[error("stream `{0}` is declared more than once")]
    DuplicateStream(String),
    /// A stream referenced a source that does not exist.
    #[error("stream `{stream}` refers to unknown source `{source_name}`")]
    UnknownSource {
        /// The stream that referred to it.
        stream: String,
        /// The name it used.
        source_name: String,
    },
    /// A stream's pagination block was invalid.
    #[error("stream `{stream}`: {source}")]
    Pagination {
        /// The stream that declared it.
        stream: String,
        /// The underlying failure.
        #[source]
        source: PaginationConfigError,
    },
    /// A stream's records block was invalid.
    #[error("stream `{stream}`: {source}")]
    Records {
        /// The stream that declared it.
        stream: String,
        /// The underlying failure.
        #[source]
        source: RecordSpecError,
    },
    /// A `{{ var }}` was used without being declared.
    #[error("stream `{stream}`: {source}")]
    Template {
        /// The stream that used it.
        stream: String,
        /// The underlying failure.
        #[source]
        source: template::TemplateError,
    },
    /// `base_url` referenced an environment variable that is not set.
    #[error("source `{source_name}`: base_url references `${{{variable}}}`, which is not set")]
    BaseUrlEnv {
        /// The source in question.
        source_name: String,
        /// The variable it expected.
        variable: String,
    },
    /// A `for_each` block could not be expanded.
    #[error("stream `{stream}`: {message}")]
    ForEach {
        /// The stream in question.
        stream: String,
        /// What is wrong with it.
        message: String,
    },
    /// A stream with a deadline declared no freshness target, or the other way round.
    #[error("stream `{stream}`: {message}")]
    Freshness {
        /// The stream in question.
        stream: String,
        /// What is wrong with it.
        message: String,
    },
    /// The cursor declaration and the request contradict each other.
    #[error("stream `{stream}`: {message}")]
    Cursor {
        /// The stream in question.
        stream: String,
        /// What is wrong with it.
        message: String,
    },
    /// A setting is reserved for a later version.
    #[error("stream `{stream}`: {feature} is not implemented in v0.1")]
    Unsupported {
        /// The stream that asked for it.
        stream: String,
        /// The setting.
        feature: String,
    },
}

/// An advisory finding: the config loads and runs, but probably not as intended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lint {
    /// What the finding is about, such as a source or stream name.
    pub subject: String,
    /// What the operator should know.
    pub message: String,
}

impl Config {
    /// Loads and validates a config file.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be read, does not parse, or does not validate.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text)
    }

    /// Validates a config that is already in memory.
    ///
    /// # Errors
    ///
    /// Fails if the YAML does not parse or does not validate.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig = serde_saphyr::from_str(text)?;
        Self::validate(raw)
    }

    fn validate(raw: RawConfig) -> Result<Self, ConfigError> {
        if raw.version != SUPPORTED_VERSION {
            return Err(ConfigError::Version { found: raw.version });
        }

        let defaults = &raw.defaults;
        let mut sources = BTreeMap::new();
        for (name, source) in &raw.sources {
            sources.insert(
                name.clone(),
                Arc::new(build_source(name, source, defaults)?),
            );
        }

        let mut streams = Vec::with_capacity(raw.streams.len());
        let mut seen = BTreeSet::new();
        for declared in &raw.streams {
            for stream in expand_for_each(declared)? {
                if !seen.insert(stream.name.clone()) {
                    return Err(ConfigError::DuplicateStream(stream.name.clone()));
                }
                if !sources.contains_key(&stream.source) {
                    return Err(ConfigError::UnknownSource {
                        stream: stream.name.clone(),
                        source_name: stream.source.clone(),
                    });
                }
                streams.push(Arc::new(build_stream(&stream)?));
            }
        }

        Ok(Self {
            store: build_store(raw.store)?,
            sources,
            streams,
        })
    }

    /// Finds a stream by name.
    #[must_use]
    pub fn stream(&self, name: &str) -> Option<&Arc<Stream>> {
        self.streams.iter().find(|s| s.name == name)
    }

    /// The source a stream pulls from.
    #[must_use]
    pub fn source_for(&self, stream: &Stream) -> Option<&Arc<Source>> {
        self.sources.get(&stream.source)
    }

    /// Advisory findings: things that load and run but are probably not intended.
    ///
    /// The unsatisfiable-freshness check is the one that matters. `freshness` is
    /// a target, not a guarantee, and a tool that lets an operator declare
    /// targets it can never meet - then reports the breach as if it were news -
    /// is not an abstraction, it is a lie.
    #[must_use]
    pub fn lint(&self) -> Vec<Lint> {
        let mut lints = Vec::new();

        // A libpq connection string carries a password, so an inline one is the
        // same finding as an inline credential.
        if let StoreSpec::Postgres { url } = &self.store {
            if url.is_inline() {
                lints.push(Lint {
                    subject: "store.url".to_owned(),
                    message: "connection string is written into the config file; use `${ENV_VAR}` or `file:/path` instead".to_owned(),
                });
            }
        }

        for (name, source) in &self.sources {
            for secret in source.auth.secrets() {
                if secret.is_inline() {
                    lints.push(Lint {
                        subject: format!("sources.{name}.auth"),
                        message: "credential is written into the config file; use `${ENV_VAR}` or `file:/path` instead".to_owned(),
                    });
                }
            }
            lints.extend(self.freshness_feasibility(name, source));
        }

        for stream in &self.streams {
            if stream.paginator.stops_on_seen_key() {
                lints.push(Lint {
                    subject: stream.name.clone(),
                    message: "`stop_on_seen_key` stops the walk at the first record already stored; records updated in place without moving in sort order will be missed".to_owned(),
                });
            }
            lints.extend(Self::colliding_params(stream));
        }

        lints
    }

    /// Parameters a stream sets by hand that the paginator or the incremental
    /// mark sets too.
    ///
    /// The request layer resolves the ambiguity in the only safe direction, so
    /// this is advisory rather than fatal. It is still worth saying: the
    /// operator is reading a config that does not describe the request that
    /// goes out, and a page size that looks configured but is ignored is the
    /// kind of thing found by counting rows weeks later.
    fn colliding_params(stream: &Stream) -> Vec<Lint> {
        let mut reserved = stream.paginator.reserved_params();
        reserved.extend(
            stream
                .records
                .cursor()
                .and_then(crate::extract::CursorSpec::param),
        );

        stream
            .request
            .params
            .iter()
            .filter(|(name, _)| reserved.contains(&name.as_str()))
            .map(|(name, _)| Lint {
                subject: stream.name.clone(),
                message: format!(
                    "`request.params.{name}` is also set by pagination or the incremental cursor; the configured value is ignored"
                ),
            })
            .collect()
    }

    /// Compares the requests the declared targets imply against the ceiling the
    /// operator configured for the source.
    fn freshness_feasibility(&self, name: &str, source: &Source) -> Option<Lint> {
        let ceiling = source.budget.ceiling_per_hour()?;
        let affordable = (ceiling as f64) * source.budget.utilization();

        let required: f64 = self
            .streams
            .iter()
            .filter(|s| s.source == *name)
            .filter_map(|s| s.freshness)
            .filter(|f| !f.is_zero())
            .map(|f| 3_600.0 / f.as_secs_f64())
            .sum();

        (required > affordable).then(|| Lint {
            subject: format!("sources.{name}"),
            message: format!(
                "declared freshness targets need at least {required:.0} requests/hour \
                 but the configured ceiling allows {affordable:.0}; \
                 targets will be missed in priority order"
            ),
        })
    }
}

fn build_source(
    name: &str,
    raw: &raw::RawSource,
    defaults: &raw::RawDefaults,
) -> Result<Source, ConfigError> {
    let resolved = resolve_base_url(name, &raw.base_url)?;
    let base_url = url::Url::parse(&resolved).map_err(|cause| ConfigError::BaseUrl {
        source_name: name.to_owned(),
        value: resolved.clone(),
        cause,
    })?;

    let budget =
        BudgetPolicy::from_config(raw.budget.as_ref()).map_err(|source| ConfigError::Budget {
            source_name: name.to_owned(),
            source,
        })?;

    Ok(Source {
        name: name.to_owned(),
        base_url,
        auth: build_auth(raw.auth.as_ref()),
        headers: raw::scalars(&raw.headers),
        budget,
        conditional_requests: raw.conditional_requests.unwrap_or(true),
        timeout: raw
            .timeout
            .or(defaults.timeout)
            .map_or(DEFAULT_TIMEOUT, duration::HumanDuration::get),
        retry: build_retry(raw.retry.as_ref().or(defaults.retry.as_ref())),
    })
}

fn build_auth(raw: Option<&RawAuth>) -> Auth {
    match raw {
        None | Some(RawAuth::None) => Auth::None,
        Some(RawAuth::Bearer { token }) => Auth::Bearer(token.clone()),
        Some(RawAuth::Basic { username, password }) => Auth::Basic {
            username: username.clone(),
            password: password.clone(),
        },
        Some(RawAuth::ApiKey {
            name,
            value,
            location,
        }) => Auth::ApiKey {
            name: name.clone(),
            value: value.clone(),
            location: *location,
        },
    }
}

fn build_retry(raw: Option<&RawRetry>) -> RetryPolicy {
    let defaults = RetryPolicy::default();
    let Some(raw) = raw else { return defaults };
    RetryPolicy {
        max_attempts: raw.max_attempts.unwrap_or(defaults.max_attempts).max(1),
        initial_backoff: raw
            .initial_backoff
            .map_or(defaults.initial_backoff, duration::HumanDuration::get),
        max_backoff: raw
            .max_backoff
            .map_or(defaults.max_backoff, duration::HumanDuration::get),
    }
}

/// Expands a `${VAR}` reference in `base_url` before it is parsed as a URL.
///
/// A private instance URL is not a credential, so it is not carried as one - but
/// it is not something to write into a file that lives in a repo either, and
/// without this the only way to keep it out is to generate the whole config from
/// a script.
///
/// Unlike a secret, this resolves at load rather than at request time, because
/// the URL has to parse for the config to validate at all. The cost is that
/// `siphon validate` needs the variable set; run it under whatever supplies the
/// rest of the environment.
/// Checks the `store` block against the backend it names.
///
/// The wrong-key cases are refused rather than ignored: a `path` under
/// `type: postgres` almost always means the author edited `type` and forgot the
/// rest, and silently honouring one key while dropping the other is how a
/// mirror ends up somewhere nobody looks.
fn build_store(raw: raw::RawStore) -> Result<StoreSpec, ConfigError> {
    match raw.kind {
        raw::StoreKind::Sqlite => {
            if raw.url.is_some() {
                return Err(ConfigError::Store {
                    message: "`url` is a postgres setting; a sqlite store takes `path`".to_owned(),
                });
            }
            let path = raw.path.ok_or_else(|| ConfigError::Store {
                message: "a sqlite store needs `path`".to_owned(),
            })?;
            Ok(StoreSpec::Sqlite { path })
        }
        raw::StoreKind::Postgres => {
            if raw.path.is_some() {
                return Err(ConfigError::Store {
                    message: "`path` is a sqlite setting; a postgres store takes `url`".to_owned(),
                });
            }
            let url = raw.url.ok_or_else(|| ConfigError::Store {
                message: "a postgres store needs `url`".to_owned(),
            })?;
            Ok(StoreSpec::Postgres { url })
        }
    }
}

fn resolve_base_url(source_name: &str, declared: &str) -> Result<String, ConfigError> {
    let Some(variable) = declared
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return Ok(declared.to_owned());
    };

    std::env::var(variable).map_err(|_| ConfigError::BaseUrlEnv {
        source_name: source_name.to_owned(),
        variable: variable.to_owned(),
    })
}

/// Expands a `for_each` block into one raw stream per value.
///
/// This is deliberately static: the values come from the config, not from
/// fetched records, which is what separates it from the `parent:` key reserved
/// for v0.3. A stream without `for_each` passes straight through.
///
/// The expansion happens before anything is validated, so each product is
/// checked exactly as if it had been written out by hand - which is the point.
/// Twenty near-identical blocks are how a config stops being reviewable.
fn expand_for_each(raw: &raw::RawStream) -> Result<Vec<raw::RawStream>, ConfigError> {
    if raw.for_each.is_empty() {
        return Ok(vec![raw.clone()]);
    }
    if raw.for_each.len() > 1 {
        return Err(ConfigError::ForEach {
            stream: raw.name.clone(),
            message: "`for_each` takes one variable; declare the others under `vars`".to_owned(),
        });
    }

    // The map holds exactly one entry, checked above, so this loop runs once.
    let mut expansions = Vec::new();
    for (variable, values) in &raw.for_each {
        if values.is_empty() {
            return Err(ConfigError::ForEach {
                stream: raw.name.clone(),
                message: format!("`for_each.{variable}` has no values, so the stream would vanish"),
            });
        }
        for value in values {
            let mut expanded = raw.clone();
            expanded.for_each = BTreeMap::new();
            expanded.vars.insert(variable.clone(), value.clone());
            // Rendered against the whole var set, not just the loop variable, so
            // the name can be built from anything the stream already declares.
            expanded.name =
                template::render(&raw.name, &raw::scalars(&expanded.vars)).map_err(|source| {
                    ConfigError::Template {
                        stream: raw.name.clone(),
                        source,
                    }
                })?;
            expansions.push(expanded);
        }
    }
    Ok(expansions)
}

fn build_stream(raw: &raw::RawStream) -> Result<Stream, ConfigError> {
    let name = raw.name.clone();

    if raw.write.mode != WriteMode::Upsert {
        return Err(ConfigError::Unsupported {
            stream: name,
            feature: format!("write mode `{}`", raw.write.mode.as_str()),
        });
    }
    if raw.write.soft_delete {
        return Err(ConfigError::Unsupported {
            stream: name,
            feature: "`soft_delete`".to_owned(),
        });
    }

    let freshness = raw.freshness.map(duration::HumanDuration::get);
    match (freshness, raw.priority) {
        (Some(f), _) if f.is_zero() => {
            return Err(ConfigError::Freshness {
                stream: name,
                message: "`freshness` must be greater than zero".to_owned(),
            })
        }
        (Some(_), Priority::BestEffort) => {
            return Err(ConfigError::Freshness {
                stream: name,
                message: "`best_effort` streams consume surplus budget only and cannot declare \
                          `freshness`; drop one or the other"
                    .to_owned(),
            })
        }
        (None, Priority::BestEffort) => {}
        (None, _) => {
            return Err(ConfigError::Freshness {
                stream: name,
                message: "declare `freshness` (how stale this stream may get) or \
                          `priority: best_effort` (run only on surplus budget)"
                    .to_owned(),
            })
        }
        (Some(_), _) => {}
    }

    let vars = raw::scalars(&raw.vars);
    let render = |text: &str, defer: bool| {
        let deferred: &[&str] = if defer { &[template::CURSOR] } else { &[] };
        template::render_deferring(text, &vars, deferred).map_err(|source| ConfigError::Template {
            stream: raw.name.clone(),
            source,
        })
    };

    let params: BTreeMap<String, String> = raw
        .request
        .params
        .iter()
        .map(|(k, v)| Ok((k.clone(), render(v.as_str(), true)?)))
        .collect::<Result<_, ConfigError>>()?;
    let interpolates_cursor = params.values().any(|v| v.contains("{{ cursor }}"));

    let request = RequestSpec {
        path: render(&raw.request.path, false)?,
        params,
        headers: raw::scalars(&raw.request.headers),
        interpolates_cursor,
    };

    let records = RecordSpec::from_config(&raw.records).map_err(|source| ConfigError::Records {
        stream: raw.name.clone(),
        source,
    })?;

    // A `{{ cursor }}` with nothing to fill it is a stream that would send the
    // placeholder verbatim to the upstream, which no API will do anything
    // sensible with.
    if interpolates_cursor {
        let Some(cursor) = records.cursor() else {
            return Err(ConfigError::Cursor {
                stream: raw.name.clone(),
                message: "`{{ cursor }}` is used in `request.params` but no `records.cursor` \
                          block declares where the mark comes from"
                    .to_owned(),
            });
        };
        if cursor.initial().is_none() {
            return Err(ConfigError::Cursor {
                stream: raw.name.clone(),
                message: "`{{ cursor }}` needs `records.cursor.initial`: the first run has no \
                          stored mark, and there is nothing sensible to substitute"
                    .to_owned(),
            });
        }
        if cursor.param().is_some() {
            return Err(ConfigError::Cursor {
                stream: raw.name.clone(),
                message: "`records.cursor.param` appends the mark as its own parameter and \
                          `{{ cursor }}` interpolates it into one; declare only one"
                    .to_owned(),
            });
        }
    }

    Ok(Stream {
        name: raw.name.clone(),
        source: raw.source.clone(),
        freshness,
        priority: raw.priority,
        direction: raw.direction,
        request,
        paginator: Paginator::from_config(&raw.pagination).map_err(|source| {
            ConfigError::Pagination {
                stream: raw.name.clone(),
                source,
            }
        })?,
        records,
        quarantine_after: raw
            .quarantine_after
            .unwrap_or(DEFAULT_QUARANTINE_AFTER)
            .max(1),
    })
}

/// Renders a freshness target for display.
#[must_use]
pub fn freshness_label(freshness: Option<Duration>) -> String {
    freshness.map_or_else(|| "-".to_owned(), format_duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
version: 1
store:
  type: sqlite
  path: ./data/mirror.db
sources:
  github:
    base_url: https://api.github.com
    auth:
      type: bearer
      token: ${GITHUB_TOKEN}
    headers:
      Accept: application/vnd.github+json
    budget:
      requests_per_hour: 5000
      observe:
        remaining_header: X-RateLimit-Remaining
        limit_header: X-RateLimit-Limit
        reset_header: X-RateLimit-Reset
        reset_format: unix_seconds
      utilization: 0.8
    conditional_requests: true
streams:
  - name: issues
    source: github
    freshness: 15m
    priority: high
    request:
      path: /repos/{{ owner }}/{{ repo }}/issues
      params:
        state: all
        per_page: 100
        sort: updated
        direction: asc
    vars:
      owner: rust-lang
      repo: rust
    pagination:
      strategy: link_header
      rel: next
    records:
      path: $
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since
    write:
      mode: upsert
      soft_delete: false
"#;

    fn parse(text: &str) -> Config {
        Config::parse(text).expect("config should be valid")
    }

    #[test]
    fn parses_the_worked_example() {
        let config = parse(MINIMAL);
        assert!(
            matches!(&config.store, StoreSpec::Sqlite { path } if path == &PathBuf::from("./data/mirror.db"))
        );
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.streams.len(), 1);

        let stream = config.stream("issues").expect("stream exists");
        assert_eq!(stream.freshness, Some(Duration::from_secs(900)));
        assert_eq!(stream.priority, Priority::High);
        assert_eq!(stream.request.path, "/repos/rust-lang/rust/issues");
        assert_eq!(stream.quarantine_after, DEFAULT_QUARANTINE_AFTER);

        let source = config.source_for(stream).expect("source exists");
        assert_eq!(source.timeout, DEFAULT_TIMEOUT);
        assert!(source.conditional_requests);
        assert!((source.budget.utilization() - 0.8).abs() < 1e-9);
    }

    #[test]
    fn numeric_params_do_not_have_to_be_quoted() {
        let config = parse(MINIMAL);
        let stream = config.stream("issues").expect("stream exists");
        assert_eq!(
            stream.request.params.get("per_page").map(String::as_str),
            Some("100")
        );
    }

    #[test]
    fn a_misspelled_key_is_refused() {
        let text = MINIMAL.replace("freshness: 15m", "freshnes: 15m");
        let err = Config::parse(&text).unwrap_err();
        assert!(
            format!("{err}").contains("freshnes"),
            "error should name the bad key: {err}"
        );
    }

    #[test]
    fn an_unsupported_version_is_refused() {
        let text = MINIMAL.replace("version: 1", "version: 2");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::Version { found: 2 }
        ));
    }

    #[test]
    fn an_unknown_source_reference_is_refused() {
        let text = MINIMAL.replace("source: github", "source: gitlab");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::UnknownSource { .. }
        ));
    }

    #[test]
    fn a_duplicate_stream_name_is_refused() {
        let text = format!(
            "{MINIMAL}{}",
            &MINIMAL[MINIMAL.find("  - name: issues").unwrap_or(0)..]
        );
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::DuplicateStream(_)
        ));
    }

    #[test]
    fn an_undeclared_template_var_is_refused() {
        let text = MINIMAL.replace("{{ owner }}", "{{ ownr }}");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::Template { .. }
        ));
    }

    #[test]
    fn a_bad_jsonpath_is_refused() {
        let text = MINIMAL.replace("primary_key: $.id", "primary_key: \"$[\"");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::Records { .. }
        ));
    }

    #[test]
    fn an_out_of_range_utilization_is_refused() {
        let text = MINIMAL.replace("utilization: 0.8", "utilization: 1.8");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::Budget { .. }
        ));
    }

    #[test]
    fn a_stream_with_no_freshness_and_no_best_effort_is_refused() {
        let text = MINIMAL.replace("    freshness: 15m\n", "");
        let err = Config::parse(&text).unwrap_err();
        assert!(
            matches!(err, ConfigError::Freshness { .. }),
            "expected a freshness error, got {err}"
        );
    }

    #[test]
    fn a_best_effort_stream_with_freshness_is_refused() {
        let text = MINIMAL.replace("priority: high", "priority: best_effort");
        assert!(matches!(
            Config::parse(&text).unwrap_err(),
            ConfigError::Freshness { .. }
        ));
    }

    #[test]
    fn a_best_effort_backfill_needs_no_freshness() {
        let text = MINIMAL
            .replace("    freshness: 15m\n", "")
            .replace("priority: high", "priority: best_effort");
        let config = parse(&text);
        let stream = config.stream("issues").expect("stream exists");
        assert_eq!(stream.priority, Priority::BestEffort);
        assert_eq!(stream.freshness, None);
    }

    /// A stream shaped like an API with a query language, where the mark is one
    /// clause of an expression rather than a parameter of its own.
    const QUERY_LANGUAGE: &str = r#"
version: 1
store:
  type: sqlite
  path: ./x.db
sources:
  api:
    base_url: https://api.example.com
streams:
  - name: issues
    source: api
    freshness: 15m
    request:
      path: /search
      params:
        query: 'project = {{ project }} AND updated >= "{{ cursor }}" ORDER BY updated ASC'
    vars:
      project: ABC
    pagination:
      strategy: cursor
      cursor_param: nextPageToken
      cursor_path: $.nextPageToken
    records:
      path: $.issues
      primary_key: $.key
      cursor:
        field: $.fields.updated
        type: rfc3339
        format: "%Y-%m-%d %H:%M"
        initial: "1970-01-01 00:00"
"#;

    #[test]
    fn a_cursor_placeholder_survives_config_load() {
        let config = parse(QUERY_LANGUAGE);
        let stream = config.stream("issues").expect("stream exists");
        assert!(stream.request.interpolates_cursor());
        // Declared vars are resolved; the cursor deliberately is not.
        assert_eq!(
            stream.request.params.get("query").map(String::as_str),
            Some(r#"project = ABC AND updated >= "{{ cursor }}" ORDER BY updated ASC"#)
        );
    }

    #[test]
    fn a_cursor_placeholder_without_a_cursor_block_is_refused() {
        let text = QUERY_LANGUAGE
            .replace("      cursor:\n", "      unused:\n")
            .replace("        field: $.fields.updated\n", "")
            .replace("        type: rfc3339\n", "")
            .replace("        format: \"%Y-%m-%d %H:%M\"\n", "")
            .replace("        initial: \"1970-01-01 00:00\"\n", "");
        let err = Config::parse(&text).unwrap_err();
        assert!(
            format!("{err}").contains("records.cursor") || matches!(err, ConfigError::Parse(_)),
            "expected a cursor error, got {err}"
        );
    }

    #[test]
    fn a_cursor_placeholder_without_an_initial_mark_is_refused() {
        let text = QUERY_LANGUAGE.replace("        initial: \"1970-01-01 00:00\"\n", "");
        let err = Config::parse(&text).unwrap_err();
        assert!(
            matches!(err, ConfigError::Cursor { .. }) && format!("{err}").contains("initial"),
            "expected an initial-mark error, got {err}"
        );
    }

    #[test]
    fn declaring_both_ways_of_sending_the_mark_is_refused() {
        let text = QUERY_LANGUAGE.replace(
            "        initial: \"1970-01-01 00:00\"",
            "        initial: \"1970-01-01 00:00\"\n        param: since",
        );
        let err = Config::parse(&text).unwrap_err();
        assert!(
            matches!(err, ConfigError::Cursor { .. }),
            "expected a cursor conflict, got {err}"
        );
    }

    #[test]
    fn a_stream_without_a_placeholder_does_not_claim_to_interpolate() {
        let config = parse(MINIMAL);
        let stream = config.stream("issues").expect("stream exists");
        assert!(!stream.request.interpolates_cursor());
    }

    #[test]
    fn a_reserved_write_mode_says_so_rather_than_silently_upserting() {
        let text = MINIMAL.replace("mode: upsert", "mode: replace");
        let err = Config::parse(&text).unwrap_err();
        assert!(
            matches!(err, ConfigError::Unsupported { .. }),
            "expected an unsupported error, got {err}"
        );
    }

    #[test]
    fn an_inline_credential_is_linted() {
        let text = MINIMAL.replace("${GITHUB_TOKEN}", "ghp_hardcoded");
        let lints = parse(&text).lint();
        assert!(
            lints
                .iter()
                .any(|l| l.message.contains("written into the config")),
            "expected an inline-secret lint, got {lints:?}"
        );
    }

    #[test]
    fn an_env_credential_is_not_linted() {
        assert!(parse(MINIMAL).lint().is_empty());
    }

    #[test]
    fn base_url_can_come_from_the_environment() {
        // A private instance URL is not a credential, but it is not something to
        // write into a file that lives in a repo either. Without this the config
        // has to be generated by a script, which is how a twenty-project mirror
        // ends up with no reviewable config at all.
        let key = "SIPHON_TEST_BASE_URL";
        std::env::set_var(key, "https://example.invalid/api");

        let text = MINIMAL.replace(
            "base_url: https://api.github.com",
            &format!("base_url: ${{{key}}}"),
        );
        let config = parse(&text);
        assert_eq!(
            config.sources["github"].base_url.as_str(),
            "https://example.invalid/api"
        );

        std::env::remove_var(key);
    }

    #[test]
    fn an_unset_base_url_variable_names_itself() {
        let text = MINIMAL.replace(
            "base_url: https://api.github.com",
            "base_url: ${SIPHON_TEST_DEFINITELY_UNSET_URL}",
        );
        let err = Config::parse(&text).expect_err("an unset variable cannot yield a URL");
        assert!(
            err.to_string().contains("SIPHON_TEST_DEFINITELY_UNSET_URL"),
            "the error should name the variable, got: {err}"
        );
    }

    #[test]
    fn for_each_expands_one_block_into_a_stream_per_value() {
        // Twenty near-identical blocks differing in a single value is what
        // drives people to generate configs from a script. Write the shape once.
        let text = MINIMAL
            .replace(
                "  - name: issues\n",
                "  - name: issues_{{ project }}\n    for_each:\n      project: [ABC, DEF]\n",
            )
            .replace(
                "path: /repos/{{ owner }}/{{ repo }}/issues",
                "path: /repos/{{ owner }}/{{ repo }}/issues/{{ project }}",
            );

        let config = parse(&text);
        let names: Vec<&str> = config.streams.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["issues_ABC", "issues_DEF"]);

        // Each expansion carries its own value all the way into the request.
        let paths: Vec<&str> = config
            .streams
            .iter()
            .map(|s| s.request.path.as_str())
            .collect();
        assert_eq!(
            paths,
            vec![
                "/repos/rust-lang/rust/issues/ABC",
                "/repos/rust-lang/rust/issues/DEF"
            ]
        );
    }

    #[test]
    fn for_each_that_does_not_vary_the_name_is_refused() {
        // Every expansion would land on the same store key, silently mirroring
        // several projects on top of each other.
        let text = MINIMAL.replace(
            "  - name: issues\n",
            "  - name: issues\n    for_each:\n      project: [ABC, DEF]\n",
        );
        let err = Config::parse(&text).expect_err("duplicate names must be refused");
        assert!(matches!(err, ConfigError::DuplicateStream(_)), "{err:?}");
    }

    #[test]
    fn a_parameter_the_paginator_also_sets_is_linted() {
        // The config says `per_page: 100` and the pagination block says the page
        // size goes in `per_page` too. Only one of them reaches the API, so the
        // operator is reading a config that does not describe the request.
        let text = MINIMAL.replace(
            "      strategy: link_header\n      rel: next",
            "      strategy: page\n      size_param: per_page",
        );
        let lints = parse(&text).lint();
        assert!(
            lints.iter().any(|l| l.message.contains("per_page")),
            "expected a colliding-parameter lint, got {lints:?}"
        );
    }

    #[test]
    fn a_parameter_the_cursor_also_sets_is_linted() {
        // `cursor.param: since` already owns `since`; seeding it by hand means
        // the seed is dropped the moment a mark is stored.
        let text = MINIMAL.replace(
            "        state: all\n",
            "        state: all\n        since: 1970-01-01T00:00:00Z\n",
        );
        let lints = parse(&text).lint();
        assert!(
            lints.iter().any(|l| l.message.contains("since")),
            "expected a colliding-parameter lint, got {lints:?}"
        );
    }

    #[test]
    fn distinct_parameter_names_are_not_linted() {
        let lints = parse(MINIMAL).lint();
        assert!(
            !lints.iter().any(|l| l.message.contains("also set by")),
            "nothing collides in the minimal config, got {lints:?}"
        );
    }

    #[test]
    fn unsatisfiable_freshness_targets_are_linted() {
        // The ceiling affords 5000 x 0.8 = 4000 requests/hour. A 500ms target
        // needs 7200 even if every sync were a single request.
        let text = MINIMAL.replace("freshness: 15m", "freshness: 500ms");
        let lints = parse(&text).lint();
        assert!(
            lints.iter().any(|l| l.message.contains("requests/hour")),
            "expected a feasibility lint, got {lints:?}"
        );
    }

    #[test]
    fn satisfiable_freshness_targets_are_not_linted() {
        let lints = parse(MINIMAL).lint();
        assert!(!lints.iter().any(|l| l.message.contains("requests/hour")));
    }

    #[test]
    fn stop_on_seen_key_carries_a_warning() {
        let text = MINIMAL.replace(
            "      strategy: link_header\n      rel: next",
            "      strategy: link_header\n      rel: next\n      stop_on_seen_key: true",
        );
        let lints = parse(&text).lint();
        assert!(
            lints.iter().any(|l| l.message.contains("stop_on_seen_key")),
            "expected a stop_on_seen_key lint, got {lints:?}"
        );
    }

    #[test]
    fn backoff_is_exponential_capped_and_jittered() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(8),
        };
        // Full jitter: the value is somewhere in [0, ceiling].
        assert_eq!(policy.backoff(1, 1.0), Duration::from_secs(1));
        assert_eq!(policy.backoff(2, 1.0), Duration::from_secs(2));
        assert_eq!(policy.backoff(3, 1.0), Duration::from_secs(4));
        assert_eq!(policy.backoff(4, 1.0), Duration::from_secs(8));
        assert_eq!(policy.backoff(9, 1.0), Duration::from_secs(8));
        assert_eq!(policy.backoff(3, 0.0), Duration::ZERO);
        assert_eq!(policy.backoff(3, 0.5), Duration::from_secs(2));
    }

    #[test]
    fn missing_file_reports_the_path() {
        let err = Config::load("/nonexistent/siphon.yaml").unwrap_err();
        assert!(matches!(err, ConfigError::Read { .. }));
    }

    /// A minimal config with the given `store` block and one valid stream.
    fn with_store(store: &str) -> String {
        format!(
            "version: 1\n{store}\nsources:\n  api:\n    base_url: https://api.example.com\n\
             streams:\n  - name: items\n    source: api\n    freshness: 15m\n    request:\n\
             \x20     path: /items\n    pagination:\n      strategy: none\n    records:\n\
             \x20     path: $.items\n      primary_key: $.id\n"
        )
    }

    #[test]
    fn a_postgres_store_is_selectable_from_the_config() {
        let config = Config::parse(&with_store(
            "store:\n  type: postgres\n  url: ${DATABASE_URL}",
        ))
        .expect("config is valid");
        assert!(matches!(&config.store, StoreSpec::Postgres { .. }));
    }

    #[test]
    fn a_path_under_a_postgres_store_is_refused_rather_than_ignored() {
        // The likely cause is an author who changed `type` and left the rest
        // behind. Honouring the `type` and silently dropping the path would
        // point the mirror somewhere nobody is looking.
        let err = Config::parse(&with_store(
            "store:\n  type: postgres\n  path: ./data/mirror.db",
        ))
        .expect_err("a postgres store has no path");
        assert!(matches!(err, ConfigError::Store { .. }), "got {err:?}");
    }

    #[test]
    fn a_url_under_a_sqlite_store_is_refused_rather_than_ignored() {
        let err = Config::parse(&with_store(
            "store:\n  type: sqlite\n  url: postgres://localhost/siphon",
        ))
        .expect_err("a sqlite store has no url");
        assert!(matches!(err, ConfigError::Store { .. }), "got {err:?}");
    }

    #[test]
    fn an_inline_connection_string_is_reported_as_a_finding() {
        // It carries a password, so it earns the same finding as an inline
        // credential rather than loading silently.
        let config = Config::parse(&with_store(
            "store:\n  type: postgres\n  url: postgres://siphon:hunter2@localhost/siphon",
        ))
        .expect("an inline url still loads");
        let lints = config.lint();
        assert!(
            lints.iter().any(|lint| lint.subject == "store.url"),
            "expected a store.url finding, got {lints:?}"
        );
    }
}
