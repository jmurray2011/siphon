//! `{{ var }}` substitution in request paths and parameters.
//!
//! Deliberately not an expression language (NG3). It substitutes declared
//! variables and fails loudly on anything else, so a typo in a path is a config
//! error rather than a 404 discovered at runtime.

use std::collections::BTreeMap;

/// Why a template could not be rendered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    /// The template referenced a variable that was not declared.
    #[error("`{{{{ {name} }}}}` is not declared in `vars`")]
    UnknownVar {
        /// The variable that was referenced.
        name: String,
    },
    /// A `{{` was never closed.
    #[error("unclosed `{{{{` in `{template}`")]
    Unclosed {
        /// The offending template.
        template: String,
    },
}

/// The built-in placeholder that carries the incremental high-water mark.
///
/// Reserved rather than user-declarable: it is resolved per request, not at
/// load, because its value is whatever the last run finished on.
pub const CURSOR: &str = "cursor";

/// Substitutes `{{ name }}` placeholders from `vars`.
///
/// # Errors
///
/// Fails on an undeclared variable or an unclosed placeholder.
pub fn render(template: &str, vars: &BTreeMap<String, String>) -> Result<String, TemplateError> {
    render_deferring(template, vars, &[])
}

/// Substitutes `{{ name }}` placeholders, leaving `deferred` ones intact.
///
/// This is what lets `{{ cursor }}` survive config load and be resolved per
/// request, while a misspelled `{{ ownr }}` is still a startup error.
///
/// # Errors
///
/// Fails on an undeclared, non-deferred variable or an unclosed placeholder.
pub fn render_deferring(
    template: &str,
    vars: &BTreeMap<String, String>,
    deferred: &[&str],
) -> Result<String, TemplateError> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let close = after.find("}}").ok_or_else(|| TemplateError::Unclosed {
            template: template.to_owned(),
        })?;
        let name = after[..close].trim();

        if deferred.contains(&name) {
            // Re-emitted in a normalised spelling so the later pass only has
            // one form to match.
            out.push_str("{{ ");
            out.push_str(name);
            out.push_str(" }}");
        } else {
            let value = vars.get(name).ok_or_else(|| TemplateError::UnknownVar {
                name: name.to_owned(),
            })?;
            out.push_str(value);
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Lists the variables a template references, whether or not they are declared.
#[must_use]
pub fn referenced(template: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find("{{") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else { break };
        names.push(after[..close].trim().to_owned());
        rest = &after[close + 2..];
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn substitutes_declared_vars() {
        let v = vars(&[("owner", "rust-lang"), ("repo", "rust")]);
        assert_eq!(
            render("/repos/{{ owner }}/{{ repo }}/issues", &v).unwrap(),
            "/repos/rust-lang/rust/issues"
        );
    }

    #[test]
    fn tolerates_missing_whitespace() {
        let v = vars(&[("id", "7")]);
        assert_eq!(render("/x/{{id}}", &v).unwrap(), "/x/7");
    }

    #[test]
    fn passes_through_a_template_with_no_placeholders() {
        assert_eq!(render("/issues", &vars(&[])).unwrap(), "/issues");
    }

    #[test]
    fn undeclared_var_is_an_error() {
        let err = render("/x/{{ nope }}", &vars(&[])).unwrap_err();
        assert_eq!(
            err,
            TemplateError::UnknownVar {
                name: "nope".to_owned()
            }
        );
    }

    #[test]
    fn unclosed_placeholder_is_an_error() {
        let err = render("/x/{{ oops", &vars(&[])).unwrap_err();
        assert!(matches!(err, TemplateError::Unclosed { .. }));
    }

    #[test]
    fn a_deferred_placeholder_survives_the_first_pass() {
        let v = vars(&[("project", "ABC")]);
        let rendered = render_deferring(
            "project = {{ project }} AND updated >= \"{{ cursor }}\"",
            &v,
            &[CURSOR],
        )
        .unwrap();
        assert_eq!(rendered, "project = ABC AND updated >= \"{{ cursor }}\"");

        // The second pass resolves what the first one left.
        let resolved = render(&rendered, &vars(&[("cursor", "2021-05-04 22:10")])).unwrap();
        assert_eq!(resolved, "project = ABC AND updated >= \"2021-05-04 22:10\"");
    }

    #[test]
    fn deferring_normalises_the_spelling() {
        let rendered =
            render_deferring("a={{cursor}} b={{  cursor  }}", &vars(&[]), &[CURSOR]).unwrap();
        assert_eq!(rendered, "a={{ cursor }} b={{ cursor }}");
    }

    #[test]
    fn deferring_still_catches_a_misspelled_var() {
        let err = render_deferring("{{ ownr }}", &vars(&[]), &[CURSOR]).unwrap_err();
        assert_eq!(
            err,
            TemplateError::UnknownVar {
                name: "ownr".to_owned()
            }
        );
    }

    #[test]
    fn lists_referenced_names() {
        assert_eq!(
            referenced("/repos/{{ owner }}/{{repo}}/issues"),
            vec!["owner".to_owned(), "repo".to_owned()]
        );
        assert!(referenced("/issues").is_empty());
    }
}
