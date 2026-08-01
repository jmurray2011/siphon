//! siphon mirrors HTTP APIs into a local database, scheduling its own polling
//! against a continuously measured rate-limit budget.
//!
//! The crate is split into a deterministic core and a thin I/O edge. The core
//! has no sockets, no filesystem and no wall clock except through
//! [`clock::Clock`], which is what lets scheduling behaviour be tested by
//! simulation rather than by sleeping.
//!
//! The governor ([`governor`]) is the piece that distinguishes siphon: it treats
//! the remaining rate-limit budget as a resource to allocate across streams that
//! have declared how fresh they need to be, rather than as a ceiling to avoid.

pub mod budget;
pub mod clock;
pub mod extract;
pub mod governor;
pub mod pagination;
