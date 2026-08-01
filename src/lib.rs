//! siphon mirrors HTTP APIs into a local database, scheduling its own polling
//! against a continuously measured rate-limit budget.
//!
//! The crate is split into a deterministic core and a thin I/O edge. The core
//! has no sockets, no filesystem and no wall clock except through
//! [`clock::Clock`], which is what lets scheduling behaviour be tested by
//! simulation rather than by sleeping.

pub mod clock;
