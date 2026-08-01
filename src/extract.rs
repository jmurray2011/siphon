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
    ///
    /// Mutually exclusive with a `{{ cursor }}` placeholder in `request.params`,
    /// which is what an API with a query language needs instead.
    #[serde(default)]
    pub param: Option<String>,
    /// How to write the high-water mark back, as a `strftime` pattern.
    ///
    /// APIs routinely accept a narrower timestamp format than they emit. Jira
    /// is the canonical case: it returns `2021-05-04T22:10:32.221+0100` but its
    /// query language accepts only `2021-05-04 22:10` - and feeding it the
    /// wider form is *accepted* and silently matches nothing, so getting this
    /// wrong looks exactly like a healthy stream with no new data.
    #[serde(default)]
    pub format: Option<String>,
    /// The mark to use before anything has been stored.
    ///
    /// Required when `request.params` interpolates `{{ cursor }}`, because
    /// there is no sensible way to render a placeholder for a mark that does
    /// not exist yet.
    #[serde(default)]
    pub initial: Option<String>,
}

/// How a cursor value is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    /// An RFC 3339 timestamp.
    ///
    /// The basic-format offset that Java-based APIs emit (`+0100` rather than
    /// `+01:00`) is accepted too. It is not strictly RFC 3339, but rejecting it
    /// means the high-water mark silently never advances, and a mirror that
    /// re-walks its whole history forever while reporting success is a worse
    /// outcome than a liberal parser.
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

impl CursorKind {
    /// True when values of this kind denote an instant, and so can be reformatted.
    #[must_use]
    pub const fn is_timestamp(self) -> bool {
        matches!(self, Self::Rfc3339 | Self::UnixSeconds | Self::UnixMillis)
    }

    /// The label used in config and error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rfc3339 => "rfc3339",
            Self::UnixSeconds => "unix_seconds",
            Self::UnixMillis => "unix_millis",
            Self::Integer => "integer",
            Self::String => "string",
        }
    }
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
    format: Option<String>,
    initial: Option<String>,
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

    /// The `strftime` pattern the high-water mark is written back in, if the
    /// API wants something narrower than it emits.
    #[must_use]
    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }

    /// The mark to use before anything has been stored.
    #[must_use]
    pub fn initial(&self) -> Option<&str> {
        self.initial.as_deref()
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
    /// The `strftime` pattern in `cursor.format` is not valid.
    #[error("records `cursor.format` is not a valid strftime pattern: `{pattern}`")]
    BadFormat {
        /// What was written.
        pattern: String,
    },
    /// `cursor.format` was set on a cursor that is not a timestamp.
    #[error("records `cursor.format` only applies to timestamp cursors, not `{kind}`")]
    FormatNotApplicable {
        /// The cursor type it was set on.
        kind: &'static str,
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
                    if let Some(pattern) = &c.format {
                        if !c.kind.is_timestamp() {
                            return Err(RecordSpecError::FormatNotApplicable {
                                kind: c.kind.as_str(),
                            });
                        }
                        // chrono panics when formatting with a bad pattern, so
                        // the pattern is checked here rather than mid-run.
                        if chrono::format::StrftimeItems::new(pattern)
                            .any(|item| matches!(item, chrono::format::Item::Error))
                        {
                            return Err(RecordSpecError::BadFormat {
                                pattern: pattern.clone(),
                            });
                        }
                    }
                    Ok::<_, RecordSpecError>(CursorSpec {
                        path: parse_path(&c.field, "cursor.field")?,
                        kind: c.kind,
                        param: c.param.clone(),
                        format: c.format.clone(),
                        initial: c.initial.clone(),
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
            CursorKind::Rfc3339 => CursorOrd::Num(parse_timestamp(raw)?.timestamp_millis()),
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

    /// The mark as the API wants it written back.
    ///
    /// Without a `format` this is the value exactly as it arrived, which is
    /// what an API that round-trips its own cursor expects. With one, the
    /// instant is re-rendered - in the offset it arrived in, so a mark that
    /// meant 22:10 local does not come back meaning 21:10.
    #[must_use]
    pub fn render(&self, spec: &CursorSpec) -> String {
        let Some(pattern) = spec.format() else {
            return self.raw.clone();
        };
        match spec.kind() {
            CursorKind::Rfc3339 => parse_timestamp(&self.raw)
                .map(|at| at.format(pattern).to_string())
                .unwrap_or_else(|| self.raw.clone()),
            CursorKind::UnixSeconds | CursorKind::UnixMillis => {
                let millis = match self.ord {
                    CursorOrd::Num(millis) => millis,
                    CursorOrd::Text(_) => return self.raw.clone(),
                };
                chrono::DateTime::from_timestamp_millis(millis)
                    .map(|at| at.format(pattern).to_string())
                    .unwrap_or_else(|| self.raw.clone())
            }
            CursorKind::Integer | CursorKind::String => self.raw.clone(),
        }
    }
}

/// Parses a timestamp, accepting both RFC 3339 and the basic-format offsets
/// that Java-based APIs emit.
///
/// `2021-05-04T22:10:32.221+0100` is not RFC 3339 - the offset needs a colon -
/// but Jira, Confluence and a long tail of Java stacks send exactly that.
fn parse_timestamp(raw: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    if let Ok(at) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(at);
    }
    const FALLBACKS: [&str; 2] = ["%Y-%m-%dT%H:%M:%S%.f%z", "%Y-%m-%d %H:%M:%S%.f%z"];
    FALLBACKS
        .iter()
        .find_map(|format| chrono::DateTime::parse_from_str(raw, format).ok())
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
                format: None,
                initial: None,
            }),
        })
        .unwrap()
    }

    fn cursor_spec(kind: CursorKind, format: Option<&str>) -> CursorSpec {
        RecordSpec::from_config(&RecordsConfig {
            path: "$".to_owned(),
            primary_key: "$.id".to_owned(),
            cursor: Some(CursorConfig {
                field: "$.updated".to_owned(),
                kind,
                param: None,
                format: format.map(str::to_owned),
                initial: None,
            }),
        })
        .unwrap()
        .cursor()
        .cloned()
        .expect("cursor was configured")
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
    fn a_basic_format_offset_still_parses() {
        // Jira, Confluence and a long tail of Java stacks emit `+0100` rather
        // than `+01:00`. Rejecting it means the high-water mark never advances
        // and the mirror re-walks its whole history forever, reporting success.
        let jira = CursorValue::parse("2021-05-04T22:10:32.221+0100", CursorKind::Rfc3339)
            .expect("a basic-format offset should parse");
        let rfc = CursorValue::parse("2021-05-04T22:10:32.221+01:00", CursorKind::Rfc3339)
            .expect("rfc 3339 should parse");

        // The two spellings denote the same instant, so neither is "later" and
        // a tie leaves the stored mark alone rather than churning it.
        assert_eq!(jira.ord, rfc.ord);
        assert_eq!(jira.clone().max(rfc).as_str(), jira.as_str());

        // And it orders correctly against a genuinely later mark.
        let later =
            CursorValue::parse("2021-05-04T23:10:32.221+0100", CursorKind::Rfc3339).unwrap();
        assert_eq!(jira.max(later.clone()).as_str(), later.as_str());
    }

    #[test]
    fn a_space_separated_timestamp_parses_too() {
        assert!(CursorValue::parse("2021-05-04 22:10:32+0100", CursorKind::Rfc3339).is_some());
    }

    #[test]
    fn genuinely_unparseable_timestamps_are_still_rejected() {
        for raw in ["not a date", "", "2021-13-45T99:99:99Z"] {
            assert!(
                CursorValue::parse(raw, CursorKind::Rfc3339).is_none(),
                "`{raw}` should not parse"
            );
        }
    }

    #[test]
    fn a_cursor_renders_verbatim_when_no_format_is_given() {
        let spec = cursor_spec(CursorKind::Rfc3339, None);
        let value =
            CursorValue::parse("2021-05-04T22:10:32.221+0100", CursorKind::Rfc3339).unwrap();
        assert_eq!(value.render(&spec), "2021-05-04T22:10:32.221+0100");
    }

    #[test]
    fn a_format_narrows_the_mark_to_what_the_api_accepts() {
        // The exact shape Jira's query language wants.
        let spec = cursor_spec(CursorKind::Rfc3339, Some("%Y-%m-%d %H:%M"));
        let value =
            CursorValue::parse("2021-05-04T22:10:32.221+0100", CursorKind::Rfc3339).unwrap();
        assert_eq!(value.render(&spec), "2021-05-04 22:10");
    }

    #[test]
    fn rendering_keeps_the_offset_the_mark_arrived_in() {
        // Re-rendering in UTC would turn 22:10+0100 into 21:10 and quietly
        // re-fetch an extra hour of history on every run.
        let spec = cursor_spec(CursorKind::Rfc3339, Some("%Y-%m-%d %H:%M"));
        let value =
            CursorValue::parse("2021-05-04T22:10:32.221+0100", CursorKind::Rfc3339).unwrap();
        assert_eq!(value.render(&spec), "2021-05-04 22:10");

        let utc = CursorValue::parse("2021-05-04T22:10:32.221Z", CursorKind::Rfc3339).unwrap();
        assert_eq!(utc.render(&spec), "2021-05-04 22:10");
    }

    #[test]
    fn unix_cursors_can_be_reformatted_as_dates() {
        let spec = cursor_spec(CursorKind::UnixSeconds, Some("%Y-%m-%d"));
        let value = CursorValue::parse("1620166232", CursorKind::UnixSeconds).unwrap();
        assert_eq!(value.render(&spec), "2021-05-04");
    }

    #[test]
    fn a_format_on_a_non_timestamp_cursor_is_refused() {
        let err = RecordSpec::from_config(&RecordsConfig {
            path: "$".to_owned(),
            primary_key: "$.id".to_owned(),
            cursor: Some(CursorConfig {
                field: "$.seq".to_owned(),
                kind: CursorKind::Integer,
                param: None,
                format: Some("%Y-%m-%d".to_owned()),
                initial: None,
            }),
        })
        .unwrap_err();
        assert!(matches!(
            err,
            RecordSpecError::FormatNotApplicable { kind: "integer" }
        ));
    }

    #[test]
    fn a_bad_strftime_pattern_is_caught_at_load_not_mid_run() {
        // chrono panics when formatting with a bad pattern, so this has to fail
        // before a run rather than during one.
        let err = RecordSpec::from_config(&RecordsConfig {
            path: "$".to_owned(),
            primary_key: "$.id".to_owned(),
            cursor: Some(CursorConfig {
                field: "$.updated".to_owned(),
                kind: CursorKind::Rfc3339,
                param: None,
                format: Some("%Q".to_owned()),
                initial: None,
            }),
        })
        .unwrap_err();
        assert!(matches!(err, RecordSpecError::BadFormat { .. }));
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
