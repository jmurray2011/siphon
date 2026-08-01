//! Turning a response body into records: the array, the primary key, the cursor.
//!
//! Extraction is JSONPath and nothing else. There is no expression language and
//! no transform step (NG3); reshaping happens in SQL downstream.

use serde::Deserialize;
use serde_json::Value;
use serde_json_path::JsonPath;
use sha2::{Digest, Sha256};

use crate::clock::Timestamp;

/// The `records` block of a stream, as written in YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordsConfig {
    /// JSONPath to the record array in the response body.
    pub path: String,
    /// JSONPath to the primary key, evaluated against each record.
    pub primary_key: String,
    /// Incremental cursor, if the API supports one.
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
}

/// The `cursor` block of a stream.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    /// JSONPath to the cursor value, evaluated against each record.
    pub field: String,
    /// How to interpret the value, which decides how high-water marks compare.
    #[serde(rename = "type")]
    pub kind: CursorKind,
    /// Query parameter the high-water mark is injected into on the next sync.
    #[serde(default)]
    pub param: Option<String>,
}

/// How a cursor value is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    /// An RFC 3339 timestamp.
    Rfc3339,
    /// Unix seconds.
    UnixSeconds,
    /// Unix milliseconds.
    UnixMillis,
    /// A monotonically increasing integer.
    Integer,
    /// An opaque string, compared lexicographically.
    String,
}

/// A validated `records` block.
#[derive(Debug, Clone)]
pub struct RecordSpec {
    records: JsonPath,
    primary_key: JsonPath,
    cursor: Option<CursorSpec>,
}

/// A validated cursor definition.
#[derive(Debug, Clone)]
pub struct CursorSpec {
    path: JsonPath,
    kind: CursorKind,
    param: Option<String>,
}

impl CursorSpec {
    /// Query parameter the high-water mark is injected into, if any.
    #[must_use]
    pub fn param(&self) -> Option<&str> {
        self.param.as_deref()
    }

    /// How values of this cursor are interpreted.
    #[must_use]
    pub const fn kind(&self) -> CursorKind {
        self.kind
    }
}

/// Why a `records` block could not be validated.
#[derive(Debug, thiserror::Error)]
pub enum RecordSpecError {
    /// One of the JSONPaths did not parse.
    #[error("records `{field}` is not a valid JSONPath: {source}")]
    BadPath {
        /// Which field held the bad path.
        field: &'static str,
        /// The parse failure.
        #[source]
        source: serde_json_path::ParseError,
    },
}

/// Why a response body could not be turned into records.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    /// A record had no value at the primary-key path.
    ///
    /// Fatal for the run rather than skipped: siphon never silently drops a
    /// record, and the dead-letter table that would hold it is a later version.
    #[error("record {index} has no value at the primary key path `{path}`: {snippet}")]
    MissingPrimaryKey {
        /// Position of the record within the page.
        index: usize,
        /// The configured primary-key path.
        path: String,
        /// A truncated rendering of the offending record.
        snippet: String,
    },
    /// The primary key resolved to an object or array.
    #[error("record {index} has a non-scalar primary key at `{path}`")]
    NonScalarPrimaryKey {
        /// Position of the record within the page.
        index: usize,
        /// The configured primary-key path.
        path: String,
    },
}

impl RecordSpec {
    /// Validates a YAML `records` block.
    ///
    /// # Errors
    ///
    /// Fails when any of the JSONPaths do not parse.
    pub fn from_config(cfg: &RecordsConfig) -> Result<Self, RecordSpecError> {
        Ok(Self {
            records: parse_path(&cfg.path, "path")?,
            primary_key: parse_path(&cfg.primary_key, "primary_key")?,
            cursor: cfg
                .cursor
                .as_ref()
                .map(|c| {
                    Ok::<_, RecordSpecError>(CursorSpec {
                        path: parse_path(&c.field, "cursor.field")?,
                        kind: c.kind,
                        param: c.param.clone(),
                    })
                })
                .transpose()?,
        })
    }

    /// The stream's cursor definition, if it declared one.
    #[must_use]
    pub const fn cursor(&self) -> Option<&CursorSpec> {
        self.cursor.as_ref()
    }

    /// Pulls records out of a response body.
    ///
    /// `fetched_at` is stamped onto every record so the raw layer records when
    /// siphon saw it, not when the upstream claims it changed.
    ///
    /// # Errors
    ///
    /// Fails if any record lacks a usable primary key.
    pub fn extract(
        &self,
        body: &Value,
        fetched_at: Timestamp,
    ) -> Result<Vec<Record>, ExtractError> {
        let nodes = self.records.query(body).all();
        let candidates: Vec<&Value> = match nodes.as_slice() {
            // `$` against an array body, or `$.data` against `{"data": [...]}`.
            [Value::Array(items)] => items.iter().collect(),
            // `$[*]` or `$.data[*]`, which select the elements directly.
            other => other.to_vec(),
        };

        let mut records = Vec::with_capacity(candidates.len());
        for (index, value) in candidates.into_iter().enumerate() {
            records.push(self.record_from(index, value, fetched_at)?);
        }
        Ok(records)
    }

    fn record_from(
        &self,
        index: usize,
        value: &Value,
        fetched_at: Timestamp,
    ) -> Result<Record, ExtractError> {
        let pk_node = self.primary_key.query(value).first().ok_or_else(|| {
            ExtractError::MissingPrimaryKey {
                index,
                path: self.primary_key.to_string(),
                snippet: snippet(value),
            }
        })?;

        let pk = match pk_node {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => {
                return Err(ExtractError::MissingPrimaryKey {
                    index,
                    path: self.primary_key.to_string(),
                    snippet: snippet(value),
                })
            }
            Value::Array(_) | Value::Object(_) => {
                return Err(ExtractError::NonScalarPrimaryKey {
                    index,
                    path: self.primary_key.to_string(),
                })
            }
        };

        let cursor = self.cursor.as_ref().and_then(|spec| {
            spec.path
                .query(value)
                .first()
                .and_then(|node| CursorValue::from_json(node, spec.kind))
        });

        Ok(Record {
            pk,
            payload: value.clone(),
            cursor,
            fetched_at,
        })
    }
}

fn parse_path(raw: &str, field: &'static str) -> Result<JsonPath, RecordSpecError> {
    JsonPath::parse(raw).map_err(|source| RecordSpecError::BadPath { field, source })
}

fn snippet(value: &Value) -> String {
    let mut rendered = value.to_string();
    if rendered.len() > 200 {
        rendered.truncate(197);
        rendered.push_str("...");
    }
    rendered
}

/// One extracted record, ready for the raw layer.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pk: String,
    payload: Value,
    cursor: Option<CursorValue>,
    fetched_at: Timestamp,
}

impl Record {
    /// The record's primary key, as text.
    #[must_use]
    pub fn pk(&self) -> &str {
        &self.pk
    }

    /// The verbatim record body.
    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }

    /// The record's cursor value, if the stream declared one.
    #[must_use]
    pub const fn cursor(&self) -> Option<&CursorValue> {
        self.cursor.as_ref()
    }

    /// When siphon fetched this record.
    #[must_use]
    pub const fn fetched_at(&self) -> Timestamp {
        self.fetched_at
    }

    /// The payload as canonical JSON text, which is what the raw layer stores.
    #[must_use]
    pub fn payload_json(&self) -> String {
        self.payload.to_string()
    }

    /// SHA-256 of the canonical payload.
    ///
    /// `serde_json::Map` is ordered, so the same record always hashes the same
    /// way. This is what distinguishes a record that was re-seen from one that
    /// genuinely changed, which in turn is what makes the change-rate signal in
    /// the run log meaningful.
    #[must_use]
    pub fn payload_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.payload_json().as_bytes());
        hasher.finalize().into()
    }
}

/// A cursor value that can be compared to find a high-water mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorValue {
    raw: String,
    ord: CursorOrd,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CursorOrd {
    Num(i64),
    Text(String),
}

impl CursorValue {
    /// Parses a cursor out of a JSON node, returning `None` for null or a value
    /// that does not match the declared type.
    #[must_use]
    pub fn from_json(node: &Value, kind: CursorKind) -> Option<Self> {
        let raw = match node {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return None,
        };
        Self::parse(&raw, kind)
    }

    /// Parses a cursor from its stored text form.
    #[must_use]
    pub fn parse(raw: &str, kind: CursorKind) -> Option<Self> {
        let ord = match kind {
            CursorKind::Rfc3339 => CursorOrd::Num(
                chrono::DateTime::parse_from_rfc3339(raw)
                    .ok()?
                    .timestamp_millis(),
            ),
            CursorKind::UnixSeconds => CursorOrd::Num(raw.parse::<i64>().ok()?.checked_mul(1_000)?),
            CursorKind::UnixMillis | CursorKind::Integer => CursorOrd::Num(raw.parse().ok()?),
            CursorKind::String => CursorOrd::Text(raw.to_owned()),
        };
        Some(Self {
            raw: raw.to_owned(),
            ord,
        })
    }

    /// The value as it appeared upstream, which is what gets sent back as a
    /// query parameter and what gets stored.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns whichever of the two is further along.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        if other.ord > self.ord {
            other
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(path: &str, pk: &str, cursor: Option<(&str, CursorKind)>) -> RecordSpec {
        RecordSpec::from_config(&RecordsConfig {
            path: path.to_owned(),
            primary_key: pk.to_owned(),
            cursor: cursor.map(|(field, kind)| CursorConfig {
                field: field.to_owned(),
                kind,
                param: Some("since".to_owned()),
            }),
        })
        .unwrap()
    }

    fn at() -> Timestamp {
        Timestamp::from_secs(1_700_000_000)
    }

    #[test]
    fn extracts_a_bare_array_body() {
        let s = spec("$", "$.id", None);
        let body = json!([{ "id": 1 }, { "id": 2 }]);
        let records = s.extract(&body, at()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].pk(), "1");
        assert_eq!(records[1].pk(), "2");
    }

    #[test]
    fn extracts_a_nested_array() {
        let s = spec("$.data", "$.uuid", None);
        let body = json!({ "data": [{ "uuid": "a" }], "meta": {} });
        let records = s.extract(&body, at()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].pk(), "a");
    }

    #[test]
    fn extracts_via_a_wildcard_selector() {
        let s = spec("$.data[*]", "$.id", None);
        let body = json!({ "data": [{ "id": 7 }, { "id": 8 }] });
        let records = s.extract(&body, at()).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn empty_array_yields_no_records() {
        let s = spec("$", "$.id", None);
        let body = json!([]);
        assert!(s.extract(&body, at()).unwrap().is_empty());
    }

    #[test]
    fn missing_array_yields_no_records() {
        let s = spec("$.data", "$.id", None);
        let body = json!({ "other": 1 });
        assert!(s.extract(&body, at()).unwrap().is_empty());
    }

    #[test]
    fn missing_primary_key_is_fatal_not_silent() {
        let s = spec("$", "$.id", None);
        let body = json!([{ "id": 1 }, { "name": "no id here" }]);
        let err = s.extract(&body, at()).unwrap_err();
        match err {
            ExtractError::MissingPrimaryKey { index, .. } => assert_eq!(index, 1),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn null_primary_key_is_fatal() {
        let s = spec("$", "$.id", None);
        let body = json!([{ "id": null }]);
        assert!(matches!(
            s.extract(&body, at()).unwrap_err(),
            ExtractError::MissingPrimaryKey { .. }
        ));
    }

    #[test]
    fn object_primary_key_is_rejected() {
        let s = spec("$", "$.id", None);
        let body = json!([{ "id": { "nested": 1 } }]);
        assert!(matches!(
            s.extract(&body, at()).unwrap_err(),
            ExtractError::NonScalarPrimaryKey { .. }
        ));
    }

    #[test]
    fn extracts_rfc3339_cursor() {
        let s = spec("$", "$.id", Some(("$.updated_at", CursorKind::Rfc3339)));
        let body = json!([{ "id": 1, "updated_at": "2024-03-01T10:00:00Z" }]);
        let records = s.extract(&body, at()).unwrap();
        assert_eq!(
            records[0].cursor().map(CursorValue::as_str),
            Some("2024-03-01T10:00:00Z")
        );
    }

    #[test]
    fn absent_cursor_is_not_an_error() {
        let s = spec("$", "$.id", Some(("$.updated_at", CursorKind::Rfc3339)));
        let body = json!([{ "id": 1 }]);
        let records = s.extract(&body, at()).unwrap();
        assert!(records[0].cursor().is_none());
    }

    #[test]
    fn unparseable_cursor_is_ignored_rather_than_poisoning_the_high_water_mark() {
        let s = spec("$", "$.id", Some(("$.updated_at", CursorKind::Rfc3339)));
        let body = json!([{ "id": 1, "updated_at": "not a date" }]);
        let records = s.extract(&body, at()).unwrap();
        assert!(records[0].cursor().is_none());
    }

    #[test]
    fn cursor_max_compares_by_type_not_by_text() {
        let a = CursorValue::parse("9", CursorKind::Integer).unwrap();
        let b = CursorValue::parse("10", CursorKind::Integer).unwrap();
        assert_eq!(a.clone().max(b.clone()).as_str(), "10");
        assert_eq!(b.max(a).as_str(), "10");

        let x = CursorValue::parse("2024-01-02T00:00:00Z", CursorKind::Rfc3339).unwrap();
        let y = CursorValue::parse("2024-01-10T00:00:00+00:00", CursorKind::Rfc3339).unwrap();
        assert_eq!(x.max(y).as_str(), "2024-01-10T00:00:00+00:00");
    }

    #[test]
    fn rfc3339_cursor_compares_across_offsets() {
        let utc = CursorValue::parse("2024-01-01T12:00:00Z", CursorKind::Rfc3339).unwrap();
        let offset = CursorValue::parse("2024-01-01T08:00:00-05:00", CursorKind::Rfc3339).unwrap();
        // 08:00-05:00 is 13:00Z, so it is the later of the two.
        assert_eq!(utc.max(offset).as_str(), "2024-01-01T08:00:00-05:00");
    }

    #[test]
    fn payload_hash_is_stable_and_change_sensitive() {
        let s = spec("$", "$.id", None);
        let first = s.extract(&json!([{ "id": 1, "t": "a" }]), at()).unwrap();
        let same = s.extract(&json!([{ "t": "a", "id": 1 }]), at()).unwrap();
        let changed = s.extract(&json!([{ "id": 1, "t": "b" }]), at()).unwrap();

        assert_eq!(first[0].payload_hash(), same[0].payload_hash());
        assert_ne!(first[0].payload_hash(), changed[0].payload_hash());
    }

    #[test]
    fn bad_jsonpath_is_rejected_at_construction() {
        let err = RecordSpec::from_config(&RecordsConfig {
            path: "$[".to_owned(),
            primary_key: "$.id".to_owned(),
            cursor: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            RecordSpecError::BadPath { field: "path", .. }
        ));
    }
}
