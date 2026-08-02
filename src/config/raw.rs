//! The YAML wire format.
//!
//! These types mirror the file exactly and enforce nothing beyond shape. Every
//! struct denies unknown fields, so a misspelled key is a startup error rather
//! than a setting that silently does nothing. [`super::Config`] is the validated
//! form; this is only what serde sees.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;

use crate::budget::BudgetConfig;
use crate::config::duration::HumanDuration;
use crate::config::secret::SecretRef;
use crate::extract::RecordsConfig;
use crate::governor::Priority;
use crate::pagination::PaginationConfig;

/// The whole config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// Config format version. Only `1` exists.
    pub version: u32,
    /// Where the mirror is written.
    pub store: RawStore,
    /// Values inherited by every source that does not override them.
    #[serde(default)]
    pub defaults: RawDefaults,
    /// APIs to pull from, by name.
    #[serde(default)]
    pub sources: BTreeMap<String, RawSource>,
    /// Endpoints to mirror.
    #[serde(default)]
    pub streams: Vec<RawStream>,
}

/// The `store` block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawStore {
    /// Which store backend. Only `sqlite` in v0.1.
    #[serde(rename = "type", default)]
    pub kind: StoreKind,
    /// Path to the database file.
    pub path: PathBuf,
}

/// Store backends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreKind {
    /// A local SQLite file.
    #[default]
    Sqlite,
}

/// The `defaults` block.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDefaults {
    /// Per-request timeout.
    #[serde(default)]
    pub timeout: Option<HumanDuration>,
    /// Retry behaviour for transient failures.
    #[serde(default)]
    pub retry: Option<RawRetry>,
}

/// The `retry` block.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRetry {
    /// How many attempts a single request gets, including the first.
    #[serde(default)]
    pub max_attempts: Option<u32>,
    /// Backoff before the second attempt.
    #[serde(default)]
    pub initial_backoff: Option<HumanDuration>,
    /// Ceiling the backoff doubles up to.
    #[serde(default)]
    pub max_backoff: Option<HumanDuration>,
}

/// One API.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSource {
    /// Scheme, host and any path prefix.
    pub base_url: String,
    /// How to authenticate.
    #[serde(default)]
    pub auth: Option<RawAuth>,
    /// Headers sent with every request to this source.
    #[serde(default)]
    pub headers: BTreeMap<String, Scalar>,
    /// How the rate-limit budget is measured and spent.
    #[serde(default)]
    pub budget: Option<BudgetConfig>,
    /// Whether to send `If-None-Match` / `If-Modified-Since`.
    #[serde(default)]
    pub conditional_requests: Option<bool>,
    /// Overrides `defaults.timeout`.
    #[serde(default)]
    pub timeout: Option<HumanDuration>,
    /// Overrides `defaults.retry`.
    #[serde(default)]
    pub retry: Option<RawRetry>,
}

/// The `auth` block.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RawAuth {
    /// No credentials.
    None,
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: SecretRef,
    },
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
        #[serde(default)]
        location: ApiKeyLocation,
    },
}

/// Where an API key is carried.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyLocation {
    /// In a request header.
    #[default]
    Header,
    /// In a query parameter.
    Query,
}

/// One endpoint to mirror.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawStream {
    /// Unique name; also the key in the store.
    ///
    /// May carry `{{ placeholders }}` when `for_each` is used, which is the only
    /// way the expansions get distinct store keys.
    pub name: String,
    /// Which source this stream pulls from.
    pub source: String,
    /// How stale this stream may get. Required unless `best_effort`.
    #[serde(default)]
    pub freshness: Option<HumanDuration>,
    /// What it is worth relative to the other streams.
    #[serde(default)]
    pub priority: Priority,
    /// Which way the walk runs.
    #[serde(default)]
    pub direction: Direction,
    /// The request to issue.
    pub request: RawRequest,
    /// Values for `{{ placeholders }}` in the request.
    #[serde(default)]
    pub vars: BTreeMap<String, Scalar>,
    /// Expands this block into one stream per value.
    ///
    /// A map of one variable to its values. Each expansion gets that variable
    /// added to its `vars`, so the same block serves twenty projects without
    /// twenty copies of it existing anywhere.
    #[serde(default)]
    pub for_each: BTreeMap<String, Vec<Scalar>>,
    /// How to walk pages.
    pub pagination: PaginationConfig,
    /// How to find records in the response.
    pub records: RecordsConfig,
    /// How records are written.
    #[serde(default)]
    pub write: RawWrite,
    /// Consecutive fatal failures on one page before it is quarantined.
    #[serde(default)]
    pub quarantine_after: Option<u32>,
}

/// Which way a stream walks the upstream collection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Incremental: resume from the stored high-water mark and advance it.
    #[default]
    Forward,
    /// Historical: walk backwards through the archive, ignoring the cursor.
    Backward,
}

/// The `request` block.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRequest {
    /// Path appended to the source's `base_url`, with `{{ vars }}` allowed.
    pub path: String,
    /// Query parameters.
    #[serde(default)]
    pub params: BTreeMap<String, Scalar>,
    /// Headers for this stream only.
    #[serde(default)]
    pub headers: BTreeMap<String, Scalar>,
}

/// The `write` block.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawWrite {
    /// How records land in the store.
    #[serde(default)]
    pub mode: WriteMode,
    /// Whether disappearing records are tombstoned.
    #[serde(default)]
    pub soft_delete: bool,
}

/// How records land in the store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Insert new records, update changed ones, keep the rest.
    #[default]
    Upsert,
    /// Keep every version seen.
    Append,
    /// Replace the stream's contents on every full sync.
    Replace,
}

impl WriteMode {
    /// A label for error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Append => "append",
            Self::Replace => "replace",
        }
    }
}

/// A YAML scalar accepted wherever a string is wanted.
///
/// `per_page: 100` is how anyone would write a page size, so numbers and
/// booleans are accepted and stringified rather than rejected for not being
/// quoted. Getting this wrong is the difference between "under a minute to a
/// working stream" (G6) and a puzzling type error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scalar(String);

impl Scalar {
    /// The value as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<Scalar> for String {
    fn from(value: Scalar) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarVisitor;

        impl Visitor<'_> for ScalarVisitor {
            type Value = Scalar;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string, number or boolean")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Scalar(v.to_owned()))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Scalar(v.to_string()))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Scalar(v.to_string()))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(Scalar(v.to_string()))
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(Scalar(v.to_string()))
            }
        }

        deserializer.deserialize_any(ScalarVisitor)
    }
}

/// Flattens a map of scalars into plain strings.
#[must_use]
pub fn scalars(map: &BTreeMap<String, Scalar>) -> BTreeMap<String, String> {
    map.iter()
        .map(|(k, v)| (k.clone(), v.as_str().to_owned()))
        .collect()
}
