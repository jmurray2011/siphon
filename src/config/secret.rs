//! Secret references, resolved at load time and never echoed back.
//!
//! A config value carries a *reference* to a secret (`${GITHUB_TOKEN}`,
//! `file:/run/secrets/token`), not the secret. Anything that isn't a recognised
//! reference is treated as an inline literal, which loads but is reported by
//! `siphon validate` as a finding: the value works, and it is also sitting in a
//! file the operator probably commits.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Deserializer};

/// Where a secret comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretRef {
    /// `${VAR}` - read from the process environment.
    Env(String),
    /// `file:/path` - read from a file, trailing newline trimmed.
    File(PathBuf),
    /// Anything else - the value itself, written into the config.
    Inline(String),
}

impl SecretRef {
    /// True if the secret is written into the config file verbatim.
    #[must_use]
    pub const fn is_inline(&self) -> bool {
        matches!(self, Self::Inline(_))
    }

    /// A short description of the source, safe to log.
    #[must_use]
    pub fn origin(&self) -> String {
        match self {
            Self::Env(name) => format!("${{{name}}}"),
            Self::File(path) => format!("file:{}", path.display()),
            Self::Inline(_) => "inline literal".to_owned(),
        }
    }

    /// Reads the secret from its source.
    ///
    /// # Errors
    ///
    /// Fails if the environment variable is unset or the file is unreadable.
    /// Both are fatal: starting a run with a half-resolved credential means
    /// discovering it as a 401 several requests later.
    pub fn resolve(&self) -> Result<Secret, SecretError> {
        match self {
            Self::Env(name) => std::env::var(name)
                .map(Secret::new)
                .map_err(|_| SecretError::EnvMissing(name.clone())),
            Self::File(path) => std::fs::read_to_string(path)
                .map(|raw| Secret::new(raw.trim_end_matches(['\n', '\r']).to_owned()))
                .map_err(|source| SecretError::FileUnreadable {
                    path: path.clone(),
                    source,
                }),
            Self::Inline(value) => Ok(Secret::new(value.clone())),
        }
    }
}

impl FromStr for SecretRef {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s.strip_prefix("${").and_then(|r| r.strip_suffix('}')) {
            return Ok(Self::Env(inner.to_owned()));
        }
        if let Some(path) = s.strip_prefix("file:") {
            return Ok(Self::File(PathBuf::from(path)));
        }
        Ok(Self::Inline(s.to_owned()))
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(raw.parse().unwrap_or(Self::Inline(raw)))
    }
}

/// A resolved secret. [`Debug`] and [`Display`](fmt::Display) are redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a resolved secret value.
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self(value)
    }

    /// The secret itself. Call this only where it goes onto the wire.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Why a secret could not be resolved.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// The referenced environment variable is not set.
    #[error("environment variable `{0}` is not set")]
    EnvMissing(String),
    /// The referenced file could not be read.
    #[error("cannot read secret from `{path}`")]
    FileUnreadable {
        /// The path that was tried.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_env_reference() {
        assert_eq!(
            "${GITHUB_TOKEN}".parse::<SecretRef>().unwrap(),
            SecretRef::Env("GITHUB_TOKEN".to_owned())
        );
    }

    #[test]
    fn recognises_file_reference() {
        assert_eq!(
            "file:/run/secrets/tok".parse::<SecretRef>().unwrap(),
            SecretRef::File(PathBuf::from("/run/secrets/tok"))
        );
    }

    #[test]
    fn anything_else_is_inline() {
        let parsed: SecretRef = "ghp_literal".parse().unwrap();
        assert_eq!(parsed, SecretRef::Inline("ghp_literal".to_owned()));
        assert!(parsed.is_inline());
    }

    #[test]
    fn unclosed_env_reference_is_not_treated_as_env() {
        assert_eq!(
            "${UNCLOSED".parse::<SecretRef>().unwrap(),
            SecretRef::Inline("${UNCLOSED".to_owned())
        );
    }

    #[test]
    fn file_secret_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tok");
        std::fs::write(&path, "s3cret\n").unwrap();
        let resolved = SecretRef::File(path).resolve().unwrap();
        assert_eq!(resolved.expose(), "s3cret");
    }

    #[test]
    fn missing_env_is_an_error() {
        let err = SecretRef::Env("SIPHON_TEST_DEFINITELY_UNSET".to_owned())
            .resolve()
            .unwrap_err();
        assert!(matches!(err, SecretError::EnvMissing(_)));
    }

    #[test]
    fn debug_does_not_leak() {
        let secret = Secret::new("hunter2".to_owned());
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{secret}"), "<redacted>");
    }
}
