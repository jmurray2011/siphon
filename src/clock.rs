//! Time as a value, so every scheduling decision is testable without sleeping.
//!
//! Everything siphon persists or compares is a [`Timestamp`]: unix milliseconds,
//! which is also the on-disk representation. Wall-clock access goes through
//! [`Clock`] so tests can drive the governor deterministically.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A point in time as unix milliseconds.
///
/// Negative values (pre-1970) are representable but never produced by siphon;
/// they can only arrive from a corrupt database or a badly wrong server clock,
/// and the arithmetic below saturates rather than panicking on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The unix epoch.
    pub const EPOCH: Self = Self(0);

    /// Builds a timestamp from unix milliseconds.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Builds a timestamp from unix seconds, saturating on overflow.
    #[must_use]
    pub const fn from_secs(secs: i64) -> Self {
        Self(secs.saturating_mul(1_000))
    }

    /// Unix milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// Unix seconds, truncated toward negative infinity.
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0.div_euclid(1_000)
    }

    /// Time elapsed from `earlier` to `self`, or zero if `self` is not later.
    #[must_use]
    pub fn saturating_since(self, earlier: Self) -> Duration {
        let delta = self.0.saturating_sub(earlier.0);
        Duration::from_millis(u64::try_from(delta).unwrap_or(0))
    }

    /// Signed difference `self - other` in milliseconds.
    #[must_use]
    pub const fn diff_millis(self, other: Self) -> i64 {
        self.0.saturating_sub(other.0)
    }

    /// This instant advanced by `d`, saturating at [`i64::MAX`].
    #[must_use]
    pub fn saturating_add(self, d: Duration) -> Self {
        let millis = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_add(millis))
    }

    /// This instant moved back by `d`, saturating at [`i64::MIN`].
    #[must_use]
    pub fn saturating_sub(self, d: Duration) -> Self {
        let millis = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_sub(millis))
    }

    /// This instant shifted by a signed millisecond offset.
    #[must_use]
    pub const fn shifted(self, millis: i64) -> Self {
        Self(self.0.saturating_add(millis))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match chrono::DateTime::from_timestamp_millis(self.0) {
            Some(dt) => write!(f, "{}", dt.to_rfc3339()),
            None => write!(f, "{}ms", self.0),
        }
    }
}

/// Source of the current time.
///
/// This exists so the governor, the budget pacer and the SLO report can be
/// tested at arbitrary points in time without sleeping or flaking.
pub trait Clock: Send + Sync + 'static {
    /// The current instant.
    fn now(&self) -> Timestamp;
}

/// The real system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        Timestamp::from_millis(millis)
    }
}

/// A clock that only moves when a test moves it.
#[derive(Debug)]
pub struct TestClock(AtomicI64);

impl TestClock {
    /// Starts a test clock at `start`.
    #[must_use]
    pub fn new(start: Timestamp) -> Self {
        Self(AtomicI64::new(start.as_millis()))
    }

    /// Moves the clock forward by `d`.
    pub fn advance(&self, d: Duration) {
        let millis = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.0.fetch_add(millis, Ordering::SeqCst);
    }

    /// Jumps the clock to an absolute instant.
    pub fn set(&self, at: Timestamp) {
        self.0.store(at.as_millis(), Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturating_since_floors_at_zero() {
        let a = Timestamp::from_secs(100);
        let b = Timestamp::from_secs(150);
        assert_eq!(b.saturating_since(a), Duration::from_secs(50));
        assert_eq!(a.saturating_since(b), Duration::ZERO);
    }

    #[test]
    fn as_secs_floors_toward_negative_infinity() {
        assert_eq!(Timestamp::from_millis(1_500).as_secs(), 1);
        assert_eq!(Timestamp::from_millis(-1_500).as_secs(), -2);
    }

    #[test]
    fn diff_millis_is_signed() {
        let a = Timestamp::from_secs(10);
        let b = Timestamp::from_secs(4);
        assert_eq!(a.diff_millis(b), 6_000);
        assert_eq!(b.diff_millis(a), -6_000);
    }

    #[test]
    fn test_clock_advances_only_when_told() {
        let clock = TestClock::new(Timestamp::from_secs(1_000));
        assert_eq!(clock.now(), Timestamp::from_secs(1_000));
        clock.advance(Duration::from_secs(30));
        assert_eq!(clock.now(), Timestamp::from_secs(1_030));
    }
}
