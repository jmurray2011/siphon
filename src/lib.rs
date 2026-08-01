//! siphon mirrors HTTP APIs into a local database, scheduling its own polling
//! against a continuously measured rate-limit budget.
//!
//! The crate is split into a deterministic core and a thin I/O edge:
//!
//! - [`clock`], [`config`], [`pagination`], [`budget`], [`governor`], [`extract`]
//!   are pure: no sockets, no filesystem, no wall clock except through [`clock::Clock`].
//! - [`http`], [`store`] are adapters. [`sync`] and [`cli`] wire them together.
//!
//! The governor ([`governor`]) is the piece that distinguishes siphon: it treats
//! the remaining rate-limit budget as a resource to allocate across streams that
//! have declared how fresh they need to be, rather than as a ceiling to avoid.

pub mod budget;
pub mod cli;
pub mod clock;
pub mod config;
pub mod extract;
pub mod governor;
pub mod http;
pub mod pagination;
pub mod store;
pub mod sync;
