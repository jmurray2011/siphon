//! RFC 8288 `Link` header parsing, reduced to what pagination needs.

/// Returns the URI of the first link whose `rel` matches `rel`, case-insensitively.
///
/// The header is a comma-separated list of `<uri>; param=value` entries. Only
/// the `rel` parameter is interpreted; everything else is skipped. A `rel` value
/// may itself be a space-separated list (`rel="next prefetch"`), so each token
/// is compared individually.
#[must_use]
pub fn find_rel(header: &str, rel: &str) -> Option<String> {
    for entry in split_entries(header) {
        let entry = entry.trim();
        let Some(close) = entry.find('>') else {
            continue;
        };
        if !entry.starts_with('<') {
            continue;
        }
        let uri = &entry[1..close];
        let params = &entry[close + 1..];
        if params_declare_rel(params, rel) {
            return Some(uri.trim().to_owned());
        }
    }
    None
}

/// Splits on commas that are not inside `<...>` or a quoted string.
fn split_entries(header: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut start = 0usize;
    let mut in_uri = false;
    let mut in_quotes = false;

    for (idx, ch) in header.char_indices() {
        match ch {
            '<' if !in_quotes => in_uri = true,
            '>' if !in_quotes => in_uri = false,
            '"' => in_quotes = !in_quotes,
            ',' if !in_uri && !in_quotes => {
                entries.push(&header[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    entries.push(&header[start..]);
    entries
}

fn params_declare_rel(params: &str, wanted: &str) -> bool {
    params.split(';').any(|param| {
        let mut parts = param.splitn(2, '=');
        let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
            return false;
        };
        if !key.trim().eq_ignore_ascii_case("rel") {
            return false;
        }
        let value = value.trim().trim_matches('"');
        value
            .split_whitespace()
            .any(|token| token.eq_ignore_ascii_case(wanted))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GITHUB: &str = r#"<https://api.github.com/repositories/1/issues?page=2>; rel="next", <https://api.github.com/repositories/1/issues?page=34>; rel="last""#;

    #[test]
    fn finds_next() {
        assert_eq!(
            find_rel(GITHUB, "next").as_deref(),
            Some("https://api.github.com/repositories/1/issues?page=2")
        );
    }

    #[test]
    fn finds_last() {
        assert_eq!(
            find_rel(GITHUB, "last").as_deref(),
            Some("https://api.github.com/repositories/1/issues?page=34")
        );
    }

    #[test]
    fn missing_rel_is_none() {
        assert_eq!(find_rel(GITHUB, "prev"), None);
    }

    #[test]
    fn handles_unquoted_rel() {
        assert_eq!(
            find_rel("<https://x/2>; rel=next", "next").as_deref(),
            Some("https://x/2")
        );
    }

    #[test]
    fn handles_multi_token_rel() {
        assert_eq!(
            find_rel(r#"<https://x/2>; rel="prefetch next""#, "next").as_deref(),
            Some("https://x/2")
        );
    }

    #[test]
    fn ignores_commas_inside_the_uri_and_quotes() {
        let header = r#"<https://x/2?ids=1,2,3>; rel="next"; title="a, b""#;
        assert_eq!(
            find_rel(header, "next").as_deref(),
            Some("https://x/2?ids=1,2,3")
        );
    }

    #[test]
    fn rel_match_is_case_insensitive() {
        assert_eq!(
            find_rel(r#"<https://x/2>; REL="Next""#, "next").as_deref(),
            Some("https://x/2")
        );
    }

    #[test]
    fn garbage_yields_none_rather_than_panicking() {
        assert_eq!(find_rel("not a link header", "next"), None);
        assert_eq!(find_rel("", "next"), None);
        assert_eq!(find_rel("<unterminated; rel=\"next\"", "next"), None);
    }
}
