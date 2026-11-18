//! Reading a rate-limit budget out of response headers.
//!
//! Three families are recognised without configuration - `X-RateLimit-*`, the
//! IETF `RateLimit-*` draft fields, and `X-Rate-Limit-*` - plus `Retry-After`
//! in both delta-seconds and HTTP-date forms. An explicit `observe` block in the
//! config always wins, because an API that uses its own names is exactly the
//! case auto-detection cannot cover.

use std::time::Duration;

use serde::Deserialize;

use crate::clock::Timestamp;

/// Anything that can be asked for a header by name, case-insensitively.
///
/// Keeps this module free of the HTTP client: the adapter implements it for
/// `reqwest`'s header map, and tests implement it over a list of pairs.
pub trait HeaderSource {
    /// Returns the value of `name`, which is always lowercase ASCII.
    fn header(&self, name: &str) -> Option<&str>;
}

impl HeaderSource for Vec<(String, String)> {
    fn header(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// How the reset header expresses its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetFormat {
    /// Absolute unix seconds.
    UnixSeconds,
    /// Absolute unix milliseconds.
    UnixMillis,
    /// Seconds from now until the window resets.
    DeltaSeconds,
    /// An HTTP-date.
    HttpDate,
}

/// The `observe` block of a source's budget, as written in YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveConfig {
    /// Header carrying requests remaining in the window.
    pub remaining_header: String,
    /// Header carrying the window's total allowance.
    #[serde(default)]
    pub limit_header: Option<String>,
    /// Header carrying when the window resets.
    #[serde(default)]
    pub reset_header: Option<String>,
    /// How to read the reset header. Defaults to sniffing the value.
    #[serde(default)]
    pub reset_format: Option<ResetFormat>,
}

/// A validated `observe` block.
#[derive(Debug, Clone)]
pub struct ObserveSpec {
    remaining_header: String,
    limit_header: Option<String>,
    reset_header: Option<String>,
    reset_format: Option<ResetFormat>,
}

impl From<&ObserveConfig> for ObserveSpec {
    fn from(cfg: &ObserveConfig) -> Self {
        Self {
            remaining_header: cfg.remaining_header.clone(),
            limit_header: cfg.limit_header.clone(),
            reset_header: cfg.reset_header.clone(),
            reset_format: cfg.reset_format,
        }
    }
}

/// What one response said about the budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LimitHeaders {
    /// The window's total allowance.
    pub limit: Option<u64>,
    /// Requests left in the current window.
    pub remaining: Option<u64>,
    /// When the window resets, in server time.
    pub reset_at: Option<Timestamp>,
    /// The server's own clock, from the `Date` header.
    pub server_date: Option<Timestamp>,
    /// How long the server asked us to wait.
    pub retry_after: Option<Duration>,
}

impl LimitHeaders {
    /// True when the response carried any rate-limit signal at all.
    ///
    /// This is what separates "429 because of the rate limit" from "403 because
    /// the token is wrong", which are handled very differently.
    #[must_use]
    pub const fn is_informative(&self) -> bool {
        self.limit.is_some()
            || self.remaining.is_some()
            || self.reset_at.is_some()
            || self.retry_after.is_some()
    }
}

const REMAINING_NAMES: [&str; 3] = [
    "x-ratelimit-remaining",
    "ratelimit-remaining",
    "x-rate-limit-remaining",
];
const LIMIT_NAMES: [&str; 3] = ["x-ratelimit-limit", "ratelimit-limit", "x-rate-limit-limit"];
const RESET_NAMES: [&str; 3] = ["x-ratelimit-reset", "ratelimit-reset", "x-rate-limit-reset"];

/// Reads whatever budget signal a response carries.
///
/// `local_now` is only used to resolve relative forms (`Retry-After: 30`,
/// `RateLimit-Reset: 60`) into absolute instants; absolute forms are returned in
/// server time and corrected for skew by the caller.
#[must_use]
pub fn read<H: HeaderSource + ?Sized>(
    headers: &H,
    observe: Option<&ObserveSpec>,
    local_now: Timestamp,
) -> LimitHeaders {
    let server_date = headers.header("date").and_then(parse_http_date);

    let (limit, remaining, reset_raw, declared_format) = match observe {
        Some(spec) => (
            spec.limit_header
                .as_deref()
                .and_then(|n| headers.header(n))
                .and_then(parse_count),
            headers.header(&spec.remaining_header).and_then(parse_count),
            spec.reset_header.as_deref().and_then(|n| headers.header(n)),
            spec.reset_format,
        ),
        None => (
            first_of(headers, &LIMIT_NAMES).and_then(parse_count),
            first_of(headers, &REMAINING_NAMES).and_then(parse_count),
            first_of(headers, &RESET_NAMES),
            None,
        ),
    };

    // Relative resets are anchored to the server's clock when it gave us one, so
    // that the caller's skew correction cancels out rather than double-counting.
    let anchor = server_date.unwrap_or(local_now);
    let reset_at = reset_raw.and_then(|raw| parse_reset(raw, declared_format, anchor));

    LimitHeaders {
        limit,
        remaining,
        reset_at,
        server_date,
        retry_after: headers
            .header("retry-after")
            .and_then(|raw| parse_retry_after(raw, anchor)),
    }
}

fn first_of<'h, H: HeaderSource + ?Sized>(headers: &'h H, names: &[&str]) -> Option<&'h str> {
    names.iter().find_map(|name| headers.header(name))
}

/// Counts are integers, but some APIs send `100.0` or pad with spaces, and the
/// IETF draft allows a quoted parameter list. Take the leading integer.
fn parse_count(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    let digits: String = trimmed.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn parse_reset(raw: &str, declared: Option<ResetFormat>, anchor: Timestamp) -> Option<Timestamp> {
    let trimmed = raw.trim();
    match declared {
        Some(ResetFormat::UnixSeconds) => trimmed.parse::<i64>().ok().map(Timestamp::from_secs),
        Some(ResetFormat::UnixMillis) => trimmed.parse::<i64>().ok().map(Timestamp::from_millis),
        Some(ResetFormat::DeltaSeconds) => trimmed
            .parse::<i64>()
            .ok()
            .map(|d| anchor.shifted(d.saturating_mul(1_000))),
        Some(ResetFormat::HttpDate) => parse_http_date(trimmed),
        None => sniff_reset(trimmed, anchor),
    }
}

/// Guesses the reset format from the magnitude of the value.
///
/// A window that resets within an hour is a delta; a value in the 1e9 range is
/// unix seconds; 1e12 is unix milliseconds. The bands do not overlap for any
/// value siphon will meet in this decade, and an operator whose API is unusual
/// enough to break the guess can declare `reset_format` explicitly.
fn sniff_reset(raw: &str, anchor: Timestamp) -> Option<Timestamp> {
    if let Ok(value) = raw.parse::<i64>() {
        return Some(match value {
            v if v >= 100_000_000_000 => Timestamp::from_millis(v),
            v if v >= 1_000_000_000 => Timestamp::from_secs(v),
            v => anchor.shifted(v.saturating_mul(1_000)),
        });
    }
    parse_http_date(raw)
}

fn parse_retry_after(raw: &str, anchor: Timestamp) -> Option<Duration> {
    let trimmed = raw.trim();
    if let Ok(secs) = trimmed.parse::<i64>() {
        return Some(Duration::from_secs(u64::try_from(secs).unwrap_or(0)));
    }
    let at = parse_http_date(trimmed)?;
    Some(at.saturating_since(anchor))
}

/// Parses the three date formats HTTP allows, preferring IMF-fixdate.
fn parse_http_date(raw: &str) -> Option<Timestamp> {
    let trimmed = raw.trim();
    const FORMATS: [&str; 3] = [
        "%a, %d %b %Y %H:%M:%S GMT",
        "%A, %d-%b-%y %H:%M:%S GMT",
        "%a %b %e %H:%M:%S %Y",
    ];
    let at = FORMATS
        .iter()
        .find_map(|format| {
            jiff::fmt::strtime::parse(format, trimmed)
                .ok()?
                .to_datetime()
                .ok()?
                .to_zoned_datetime(jiff::tz::TimeZone::UTC)
                .ok()
        })
        .or_else(|| jiff::fmt::rfc2822::parse(trimmed).ok())?;
    Some(Timestamp::from_millis(at.timestamp().as_millisecond()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn now() -> Timestamp {
        Timestamp::from_secs(1_700_000_000)
    }

    #[test]
    fn reads_github_style_headers() {
        let h = headers(&[
            ("X-RateLimit-Limit", "5000"),
            ("X-RateLimit-Remaining", "4987"),
            ("X-RateLimit-Reset", "1700003600"),
        ]);
        let read = read(&h, None, now());
        assert_eq!(read.limit, Some(5000));
        assert_eq!(read.remaining, Some(4987));
        assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_700_003_600)));
        assert!(read.is_informative());
    }

    #[test]
    fn reads_ietf_draft_headers_with_a_delta_reset() {
        let h = headers(&[
            ("RateLimit-Limit", "100"),
            ("RateLimit-Remaining", "37"),
            ("RateLimit-Reset", "60"),
        ]);
        let read = read(&h, None, now());
        assert_eq!(read.remaining, Some(37));
        assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_700_000_060)));
    }

    #[test]
    fn explicit_observe_config_wins_over_sniffing() {
        let h = headers(&[
            ("X-RateLimit-Remaining", "4987"),
            ("X-Quota-Left", "12"),
            ("X-Quota-Reset", "30"),
        ]);
        let spec = ObserveSpec::from(&ObserveConfig {
            remaining_header: "X-Quota-Left".to_owned(),
            limit_header: None,
            reset_header: Some("X-Quota-Reset".to_owned()),
            reset_format: Some(ResetFormat::DeltaSeconds),
        });
        let read = read(&h, Some(&spec), now());
        assert_eq!(read.remaining, Some(12));
        assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_700_000_030)));
    }

    #[test]
    fn relative_reset_anchors_to_server_date_not_local_time() {
        // Local clock is 10 minutes behind the server. A 60s delta reset must
        // land 60s after the server's clock, not 60s after ours.
        let h = headers(&[
            ("Date", "Tue, 14 Nov 2023 22:13:20 GMT"),
            ("RateLimit-Reset", "60"),
        ]);
        let local = Timestamp::from_secs(1_700_000_000 - 600);
        let read = read(&h, None, local);
        assert_eq!(read.server_date, Some(Timestamp::from_secs(1_700_000_000)));
        assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_700_000_060)));
    }

    #[test]
    fn parses_retry_after_in_both_forms() {
        let delta = headers(&[("Retry-After", "30")]);
        assert_eq!(
            read(&delta, None, now()).retry_after,
            Some(Duration::from_secs(30))
        );

        let date = headers(&[
            ("Date", "Tue, 14 Nov 2023 22:13:20 GMT"),
            ("Retry-After", "Tue, 14 Nov 2023 22:14:20 GMT"),
        ]);
        assert_eq!(
            read(&date, None, now()).retry_after,
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn parses_every_http_date_form() {
        let at = Some(Timestamp::from_millis(784_111_777_000));
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), at);
        assert_eq!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT"), at);
        assert_eq!(parse_http_date("Sun Nov  6 08:49:37 1994"), at);
        assert_eq!(
            parse_http_date("Tue, 14 Nov 2023 23:13:20 +0100"),
            Some(Timestamp::from_secs(1_700_000_000))
        );
        assert_eq!(parse_http_date("not a date"), None);
    }

    #[test]
    fn retry_after_in_the_past_is_zero_not_negative() {
        let h = headers(&[
            ("Date", "Tue, 14 Nov 2023 22:13:20 GMT"),
            ("Retry-After", "Tue, 14 Nov 2023 22:00:00 GMT"),
        ]);
        assert_eq!(read(&h, None, now()).retry_after, Some(Duration::ZERO));
    }

    #[test]
    fn sniffs_absolute_versus_relative_resets() {
        let anchor = now();
        assert_eq!(
            sniff_reset("1700003600", anchor),
            Some(Timestamp::from_secs(1_700_003_600))
        );
        assert_eq!(
            sniff_reset("1700003600000", anchor),
            Some(Timestamp::from_millis(1_700_003_600_000))
        );
        assert_eq!(
            sniff_reset("60", anchor),
            Some(Timestamp::from_secs(1_700_000_060))
        );
    }

    #[test]
    fn tolerates_decimal_and_padded_counts() {
        let h = headers(&[("X-RateLimit-Remaining", " 42.0 ")]);
        assert_eq!(read(&h, None, now()).remaining, Some(42));
    }

    #[test]
    fn absent_headers_are_uninformative_rather_than_zero() {
        let h = headers(&[("Content-Type", "application/json")]);
        let read = read(&h, None, now());
        assert_eq!(read.remaining, None);
        assert_eq!(read.limit, None);
        assert!(!read.is_informative());
    }

    #[test]
    fn garbage_headers_do_not_produce_a_budget() {
        let h = headers(&[
            ("X-RateLimit-Remaining", "unknown"),
            ("X-RateLimit-Reset", "soon"),
            ("Date", "not a date"),
        ]);
        let read = read(&h, None, now());
        assert_eq!(read.remaining, None);
        assert_eq!(read.reset_at, None);
        assert_eq!(read.server_date, None);
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let h = headers(&[("x-ratelimit-remaining", "5")]);
        assert_eq!(read(&h, None, now()).remaining, Some(5));
    }
}
