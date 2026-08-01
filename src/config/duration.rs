//! Human-written durations: `30s`, `15m`, `1h`, `1h30m`, `500ms`.
//!
//! Hand-rolled rather than pulled from a crate because the config format is the
//! product: the error messages matter, and a bare number has to be rejected
//! rather than silently interpreted as some unit the operator didn't intend.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::{self, Deserialize, Deserializer};

/// A [`Duration`] written the way an operator writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanDuration(Duration);

impl HumanDuration {
    /// Wraps a duration.
    #[must_use]
    pub const fn new(d: Duration) -> Self {
        Self(d)
    }

    /// The wrapped duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl From<HumanDuration> for Duration {
    fn from(value: HumanDuration) -> Self {
        value.0
    }
}

/// Why a duration string could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurationParseError {
    /// The string was empty or only whitespace.
    #[error("empty duration")]
    Empty,
    /// A number was written with no unit suffix.
    #[error("`{0}` has no unit: write a duration like `30s`, `15m`, `1h` or `2d`")]
    MissingUnit(String),
    /// The unit suffix is not one siphon knows.
    #[error("unknown duration unit `{unit}` in `{input}`: use ms, s, m, h or d")]
    UnknownUnit {
        /// The offending suffix.
        unit: String,
        /// The whole string it appeared in.
        input: String,
    },
    /// A component was not a number.
    #[error("`{0}` is not a valid duration")]
    Malformed(String),
    /// The total exceeded what a [`Duration`] can hold.
    #[error("`{0}` is too large")]
    Overflow(String),
}

impl FromStr for HumanDuration {
    type Err = DurationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.trim();
        if input.is_empty() {
            return Err(DurationParseError::Empty);
        }

        let mut total = Duration::ZERO;
        let mut rest = input;
        let mut components = 0usize;

        while !rest.is_empty() {
            let digits_end = rest
                .find(|c: char| !c.is_ascii_digit())
                .ok_or_else(|| DurationParseError::MissingUnit(input.to_owned()))?;
            if digits_end == 0 {
                return Err(DurationParseError::Malformed(input.to_owned()));
            }
            let (digits, tail) = rest.split_at(digits_end);
            let unit_end = tail
                .find(|c: char| c.is_ascii_digit())
                .unwrap_or(tail.len());
            let (unit, remainder) = tail.split_at(unit_end);

            let value: u64 = digits
                .parse()
                .map_err(|_| DurationParseError::Overflow(input.to_owned()))?;
            let millis = match unit.trim() {
                "ms" => Some(value),
                "s" => value.checked_mul(1_000),
                "m" => value.checked_mul(60_000),
                "h" => value.checked_mul(3_600_000),
                "d" => value.checked_mul(86_400_000),
                other => {
                    return Err(DurationParseError::UnknownUnit {
                        unit: other.to_owned(),
                        input: input.to_owned(),
                    })
                }
            }
            .ok_or_else(|| DurationParseError::Overflow(input.to_owned()))?;

            total = total
                .checked_add(Duration::from_millis(millis))
                .ok_or_else(|| DurationParseError::Overflow(input.to_owned()))?;
            components += 1;
            rest = remainder;
        }

        if components == 0 {
            return Err(DurationParseError::Malformed(input.to_owned()));
        }
        Ok(Self(total))
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_duration(self.0))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(de::Error::custom)
    }
}

/// Renders a duration in the compact form used by `siphon status`: `4m12s`, `1h03m`.
#[must_use]
pub fn format_duration(d: Duration) -> String {
    let total = d.as_secs();
    if total == 0 {
        let millis = d.subsec_millis();
        return if millis == 0 {
            "0s".to_owned()
        } else {
            format!("{millis}ms")
        };
    }
    let days = total / 86_400;
    let hours = (total % 86_400) / 3_600;
    let mins = (total % 3_600) / 60;
    let secs = total % 60;

    if days > 0 {
        format!("{days}d{hours:02}h")
    } else if hours > 0 {
        format!("{hours}h{mins:02}m")
    } else if mins > 0 {
        format!("{mins}m{secs:02}s")
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Duration, DurationParseError> {
        s.parse::<HumanDuration>().map(HumanDuration::get)
    }

    #[test]
    fn parses_single_units() {
        assert_eq!(parse("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse("15m"), Ok(Duration::from_secs(900)));
        assert_eq!(parse("1h"), Ok(Duration::from_secs(3_600)));
        assert_eq!(parse("2d"), Ok(Duration::from_secs(172_800)));
    }

    #[test]
    fn parses_compound_units() {
        assert_eq!(parse("1h30m"), Ok(Duration::from_secs(5_400)));
        assert_eq!(parse("1d2h3m4s"), Ok(Duration::from_secs(93_784)));
    }

    #[test]
    fn rejects_bare_number() {
        assert_eq!(
            parse("15"),
            Err(DurationParseError::MissingUnit("15".to_owned()))
        );
    }

    #[test]
    fn rejects_unknown_unit() {
        assert_eq!(
            parse("15w"),
            Err(DurationParseError::UnknownUnit {
                unit: "w".to_owned(),
                input: "15w".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_empty_and_malformed() {
        assert_eq!(parse("  "), Err(DurationParseError::Empty));
        assert_eq!(
            parse("m15"),
            Err(DurationParseError::Malformed("m15".to_owned()))
        );
    }

    #[test]
    fn formats_compactly() {
        assert_eq!(format_duration(Duration::from_secs(252)), "4m12s");
        assert_eq!(format_duration(Duration::from_secs(3_780)), "1h03m");
        assert_eq!(format_duration(Duration::from_secs(90_000)), "1d01h");
        assert_eq!(format_duration(Duration::from_secs(42)), "42s");
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }
}
