//! The HTTP adapter: one client per source, one request at a time.
//!
//! This module issues a single attempt and classifies what came back. It does
//! not retry, does not pace, and does not decide what to fetch next - those
//! belong to [`crate::sync`] and [`crate::governor`], because the logic *is* the
//! product and hiding it inside a client would be the wrong place for it.

use std::collections::BTreeMap;
use std::sync::Arc;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::StatusCode;
use serde_json::Value;
use url::Url;

use crate::budget::{self, HeaderSource, LimitHeaders};
use crate::config::raw::ApiKeyLocation;
use crate::config::secret::SecretError;
use crate::config::{Auth, Source, Stream};
use crate::pagination::PageRequest;
use crate::store::Validator;

/// Why a request could not be built or issued.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// A credential could not be resolved.
    #[error("cannot resolve a credential for source `{source_name}`")]
    Secret {
        /// The source whose credential it is.
        source_name: String,
        /// The underlying failure.
        #[source]
        source: SecretError,
    },
    /// A header name or value in the config was not valid HTTP.
    #[error("source `{source_name}` has an invalid header `{header}`")]
    Header {
        /// The source that declared it.
        source_name: String,
        /// The offending header name.
        header: String,
    },
    /// The client could not be constructed.
    #[error("cannot build HTTP client for source `{source_name}`")]
    Client {
        /// The source it was being built for.
        source_name: String,
        /// The underlying failure.
        #[source]
        source: reqwest::Error,
    },
    /// A request URL could not be assembled.
    #[error("cannot build request URL for stream `{stream}`: {cause}")]
    Url {
        /// The stream whose request it was.
        stream: String,
        /// What went wrong.
        cause: String,
    },
}

/// How a response is treated. See the design's response classification table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseClass {
    /// 2xx: extract and write.
    Success,
    /// 304: the mirror is confirmed current; write nothing.
    NotModified,
    /// 429, or 403 carrying rate-limit headers.
    RateLimited,
    /// Transient. Worth another attempt against the retry budget.
    Retryable,
    /// Permanent for this request. Fails the page, never advances the checkpoint.
    Fatal,
}

/// What one attempt produced.
#[derive(Debug)]
pub struct Response {
    /// How to treat it.
    pub class: ResponseClass,
    /// The status code, or `None` for a transport failure.
    pub status: Option<u16>,
    /// The parsed body, when there was a JSON one.
    pub body: Option<Value>,
    /// The start of the body as text, for diagnostics and quarantine records.
    pub body_excerpt: String,
    /// The `Link` header, if any.
    pub link_header: Option<String>,
    /// Validators to store for the next conditional request.
    pub validator: Validator,
    /// Whatever the response said about the rate-limit budget.
    pub limits: LimitHeaders,
    /// A human-readable reason, set for failures.
    pub detail: Option<String>,
}

impl Response {
    /// The body, or JSON `null` when there was none.
    #[must_use]
    pub fn body_or_null(&self) -> Value {
        self.body.clone().unwrap_or(Value::Null)
    }
}

impl HeaderSource for HeaderMap {
    fn header(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|value| value.to_str().ok())
    }
}

/// An HTTP client bound to one source, with its credentials already resolved.
#[derive(Debug)]
pub struct SourceClient {
    source: Arc<Source>,
    client: reqwest::Client,
    query_auth: Vec<(String, String)>,
}

impl SourceClient {
    /// Builds a client for a source, resolving its credentials.
    ///
    /// Credentials resolve here rather than at config load, so `siphon validate`
    /// can check a config on a machine that holds none. They still resolve
    /// before any request goes out: a missing token is a startup failure, not a
    /// 401 discovered three pages in.
    ///
    /// # Errors
    ///
    /// Fails if a credential cannot be read or a configured header is not valid
    /// HTTP.
    pub fn new(source: Arc<Source>) -> Result<Self, HttpError> {
        let mut headers = HeaderMap::new();
        for (name, value) in &source.headers {
            insert_header(&mut headers, &source.name, name, value)?;
        }

        let mut query_auth = Vec::new();
        match &source.auth {
            Auth::None => {}
            Auth::Bearer(token) => {
                let token = token.resolve().map_err(|e| HttpError::Secret {
                    source_name: source.name.clone(),
                    source: e,
                })?;
                insert_header(
                    &mut headers,
                    &source.name,
                    "authorization",
                    &format!("Bearer {}", token.expose()),
                )?;
            }
            Auth::Basic { username, password } => {
                let user = username.resolve().map_err(|e| HttpError::Secret {
                    source_name: source.name.clone(),
                    source: e,
                })?;
                let pass = password.resolve().map_err(|e| HttpError::Secret {
                    source_name: source.name.clone(),
                    source: e,
                })?;
                // Encoding is reqwest's job; doing it by hand would mean either
                // a base64 dependency or a hand-rolled encoder, for no gain.
                let encoded = reqwest::Client::new()
                    .get("http://placeholder.invalid")
                    .basic_auth(user.expose(), Some(pass.expose()))
                    .build()
                    .ok()
                    .and_then(|req| {
                        req.headers()
                            .get(reqwest::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok().map(str::to_owned))
                    })
                    .ok_or_else(|| HttpError::Header {
                        source_name: source.name.clone(),
                        header: "authorization".to_owned(),
                    })?;
                insert_header(&mut headers, &source.name, "authorization", &encoded)?;
            }
            Auth::ApiKey {
                name,
                value,
                location,
            } => {
                let key = value.resolve().map_err(|e| HttpError::Secret {
                    source_name: source.name.clone(),
                    source: e,
                })?;
                match location {
                    ApiKeyLocation::Header => {
                        insert_header(&mut headers, &source.name, name, key.expose())?;
                    }
                    ApiKeyLocation::Query => {
                        query_auth.push((name.clone(), key.expose().to_owned()));
                    }
                }
            }
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(source.timeout)
            .user_agent(concat!("siphon/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| HttpError::Client {
                source_name: source.name.clone(),
                source: e,
            })?;

        Ok(Self {
            source,
            client,
            query_auth,
        })
    }

    /// The source this client talks to.
    #[must_use]
    pub fn source(&self) -> &Source {
        &self.source
    }

    /// Assembles the URL for one page of a stream.
    ///
    /// # Errors
    ///
    /// Fails if the assembled URL is not valid.
    pub fn url_for(
        &self,
        stream: &Stream,
        page: &PageRequest,
        cursor: Option<&str>,
    ) -> Result<Url, HttpError> {
        // A `Link` header or a body-supplied next URL is already complete: it
        // carries the upstream's own pagination parameters, and re-adding ours
        // would fight with them.
        if let Some(absolute) = &page.absolute_url {
            let mut url = Url::parse(absolute).map_err(|e| HttpError::Url {
                stream: stream.name.clone(),
                cause: e.to_string(),
            })?;
            self.apply_query_auth(&mut url);
            return Ok(url);
        }

        let mut url = self.source.base_url.clone();
        {
            let base_path = url.path().trim_end_matches('/').to_owned();
            let suffix = &stream.request.path;
            let joined = if suffix.starts_with('/') {
                format!("{base_path}{suffix}")
            } else {
                format!("{base_path}/{suffix}")
            };
            url.set_path(&joined);
        }

        {
            let interpolate: BTreeMap<String, String> = cursor
                .map(|value| {
                    [(crate::config::template::CURSOR.to_owned(), value.to_owned())].into()
                })
                .unwrap_or_default();

            // The mark goes in one place or the other, never both; the config
            // layer refuses a stream that declares both spellings.
            let cursor_param = if stream.request.interpolates_cursor() || cursor.is_none() {
                None
            } else {
                stream
                    .records
                    .cursor()
                    .and_then(super::extract::CursorSpec::param)
            };

            let mut query = url.query_pairs_mut();
            for (name, value) in &stream.request.params {
                // Whatever the paginator or the mark sets for this page wins
                // over a configured parameter of the same name. Emitting both
                // leaves the API to choose, and for `offset` the choice is
                // silently destructive: the API answers with one page size
                // while the paginator advances by another, so every page skips
                // or repeats records. `validate` lints the config that gets
                // here, but the request itself must be unambiguous regardless.
                if page.params.iter().any(|(taken, _)| taken == name)
                    || cursor_param == Some(name.as_str())
                {
                    continue;
                }
                // A parameter carrying `{{ cursor }}` gets the mark spliced in.
                // Everything else was fully resolved at config load.
                let resolved = if stream.request.interpolates_cursor() && !interpolate.is_empty() {
                    crate::config::template::render(value, &interpolate)
                        .unwrap_or_else(|_| value.clone())
                } else {
                    value.clone()
                };
                query.append_pair(name, &resolved);
            }
            for (name, value) in &page.params {
                query.append_pair(name, value);
            }
            if let (Some(param), Some(value)) = (cursor_param, cursor) {
                query.append_pair(param, value);
            }
        }
        self.apply_query_auth(&mut url);
        Ok(url)
    }

    fn apply_query_auth(&self, url: &mut Url) {
        if self.query_auth.is_empty() {
            return;
        }
        let mut query = url.query_pairs_mut();
        for (name, value) in &self.query_auth {
            query.append_pair(name, value);
        }
    }

    /// Issues one request and classifies the response.
    ///
    /// Never retries and never sleeps; a transport failure comes back as
    /// [`ResponseClass::Retryable`] for the caller to decide about.
    ///
    /// # Errors
    ///
    /// Fails only if a stream-level header in the config is not valid HTTP.
    /// Everything the network does is a [`Response`], not an error.
    pub async fn fetch(
        &self,
        url: &Url,
        stream_headers: &BTreeMap<String, String>,
        validator: Option<&Validator>,
        now: crate::clock::Timestamp,
    ) -> Result<Response, HttpError> {
        let mut request = self.client.get(url.clone());

        for (name, value) in stream_headers {
            let header = HeaderName::try_from(name.as_str()).map_err(|_| HttpError::Header {
                source_name: self.source.name.clone(),
                header: name.clone(),
            })?;
            let value = HeaderValue::from_str(value).map_err(|_| HttpError::Header {
                source_name: self.source.name.clone(),
                header: name.clone(),
            })?;
            request = request.header(header, value);
        }

        if self.source.conditional_requests {
            if let Some(validator) = validator {
                if let Some(etag) = &validator.etag {
                    request = request.header(reqwest::header::IF_NONE_MATCH, etag);
                }
                if let Some(last_modified) = &validator.last_modified {
                    request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
                }
            }
        }

        let observe = self.source.budget.observe();
        match request.send().await {
            Ok(response) => Ok(classify(response, observe, now).await),
            Err(error) => Ok(transport_failure(&error)),
        }
    }
}

fn insert_header(
    headers: &mut HeaderMap,
    source_name: &str,
    name: &str,
    value: &str,
) -> Result<(), HttpError> {
    let header = HeaderName::try_from(name).map_err(|_| HttpError::Header {
        source_name: source_name.to_owned(),
        header: name.to_owned(),
    })?;
    let mut value = HeaderValue::from_str(value).map_err(|_| HttpError::Header {
        source_name: source_name.to_owned(),
        header: name.to_owned(),
    })?;
    value.set_sensitive(header == reqwest::header::AUTHORIZATION);
    headers.insert(header, value);
    Ok(())
}

async fn classify(
    response: reqwest::Response,
    observe: Option<&budget::ObserveSpec>,
    now: crate::clock::Timestamp,
) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let limits = budget::headers::read(&headers, observe, now);

    let validator = Validator {
        etag: header_string(&headers, reqwest::header::ETAG),
        last_modified: header_string(&headers, reqwest::header::LAST_MODIFIED),
    };
    let link_header = headers.header("link").map(str::to_owned);

    if status == StatusCode::NOT_MODIFIED {
        return Response {
            class: ResponseClass::NotModified,
            status: Some(status.as_u16()),
            body: None,
            body_excerpt: String::new(),
            link_header,
            validator,
            limits,
            detail: None,
        };
    }

    // A 403 is the ambiguous one: GitHub uses it for rate limiting, everyone
    // else uses it for a bad token. The limit headers are what tells them apart,
    // and getting it wrong means either hammering a limit or treating a
    // temporary block as a permanently broken stream.
    let rate_limited = status == StatusCode::TOO_MANY_REQUESTS
        || (status == StatusCode::FORBIDDEN && limits.is_informative());

    let text = response.text().await.unwrap_or_default();
    let excerpt = excerpt_of(&text);

    if rate_limited {
        return Response {
            class: ResponseClass::RateLimited,
            status: Some(status.as_u16()),
            body: None,
            body_excerpt: excerpt,
            link_header,
            validator,
            limits,
            detail: Some(format!("rate limited with {status}")),
        };
    }

    if status.is_server_error() {
        return Response {
            class: ResponseClass::Retryable,
            status: Some(status.as_u16()),
            body: None,
            body_excerpt: excerpt,
            link_header,
            validator,
            limits,
            detail: Some(format!("upstream returned {status}")),
        };
    }

    if !status.is_success() {
        return Response {
            class: ResponseClass::Fatal,
            status: Some(status.as_u16()),
            body: None,
            body_excerpt: excerpt,
            link_header,
            validator,
            limits,
            detail: Some(format!("upstream returned {status}")),
        };
    }

    match serde_json::from_str::<Value>(&text) {
        Ok(body) => Response {
            class: ResponseClass::Success,
            status: Some(status.as_u16()),
            body: Some(body),
            body_excerpt: excerpt,
            link_header,
            validator,
            limits,
            detail: None,
        },
        Err(error) => Response {
            class: ResponseClass::Fatal,
            status: Some(status.as_u16()),
            body: None,
            body_excerpt: excerpt,
            link_header,
            validator,
            limits,
            detail: Some(format!("response body is not JSON: {error}")),
        },
    }
}

fn transport_failure(error: &reqwest::Error) -> Response {
    // Connection resets, timeouts and DNS blips are the transport equivalent of
    // a 503: worth another attempt against the retry budget.
    Response {
        class: ResponseClass::Retryable,
        status: None,
        body: None,
        body_excerpt: String::new(),
        link_header: None,
        validator: Validator::default(),
        limits: LimitHeaders::default(),
        detail: Some(error.to_string()),
    }
}

fn header_string(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn excerpt_of(text: &str) -> String {
    const LIMIT: usize = 2_000;
    if text.len() <= LIMIT {
        return text.to_owned();
    }
    let mut end = LIMIT;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    const CONFIG: &str = r#"
version: 1
store:
  type: sqlite
  path: ./x.db
sources:
  api:
    base_url: https://api.example.com/v2
    auth:
      type: api_key
      name: key
      value: secret-value
      location: query
streams:
  - name: items
    source: api
    freshness: 15m
    request:
      path: /items
      params:
        state: all
    pagination:
      strategy: offset
      page_size: 100
    records:
      path: $.data
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since
  - name: colliding
    source: api
    freshness: 15m
    request:
      path: /items
      params:
        limit: 50
        since: seed
    pagination:
      strategy: offset
      page_size: 100
    records:
      path: $.data
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since
"#;

    fn client_and_stream() -> (SourceClient, Arc<Stream>) {
        let config = Config::parse(CONFIG).expect("config is valid");
        let stream = Arc::clone(config.stream("items").expect("stream exists"));
        let source = Arc::clone(config.sources.get("api").expect("source exists"));
        (SourceClient::new(source).expect("client builds"), stream)
    }

    #[test]
    fn url_preserves_a_base_path_prefix() {
        let (client, stream) = client_and_stream();
        let page = PageRequest {
            params: vec![("limit".to_owned(), "100".to_owned())],
            absolute_url: None,
        };
        let url = client.url_for(&stream, &page, None).expect("url builds");
        assert_eq!(url.path(), "/v2/items");
    }

    #[test]
    fn url_merges_configured_and_pagination_params() {
        let (client, stream) = client_and_stream();
        let page = PageRequest {
            params: vec![
                ("limit".to_owned(), "100".to_owned()),
                ("offset".to_owned(), "200".to_owned()),
            ],
            absolute_url: None,
        };
        let url = client.url_for(&stream, &page, None).expect("url builds");
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("state").map(String::as_str), Some("all"));
        assert_eq!(query.get("limit").map(String::as_str), Some("100"));
        assert_eq!(query.get("offset").map(String::as_str), Some("200"));
    }

    #[test]
    fn pagination_and_cursor_params_override_configured_ones_of_the_same_name() {
        // A name can easily be declared twice: `pagelen` is both a natural
        // request parameter and the page-size parameter, and a `since` seed
        // reads like the obvious way to start a walk. Appending both leaves the
        // API to choose, and for `offset` that silently corrupts the walk - the
        // API answers with one page size while the paginator advances by
        // another, so records are skipped or repeated on every page.
        let config = Config::parse(CONFIG).expect("config is valid");
        let stream = Arc::clone(config.stream("colliding").expect("stream exists"));
        let source = Arc::clone(config.sources.get("api").expect("source exists"));
        let client = SourceClient::new(source).expect("client builds");

        let page = PageRequest {
            params: vec![
                ("limit".to_owned(), "100".to_owned()),
                ("offset".to_owned(), "200".to_owned()),
            ],
            absolute_url: None,
        };
        let url = client
            .url_for(&stream, &page, Some("2026-01-01T00:00:00Z"))
            .expect("url builds");

        let values = |wanted: &str| -> Vec<String> {
            url.query_pairs()
                .filter(|(name, _)| name == wanted)
                .map(|(_, value)| value.into_owned())
                .collect()
        };

        assert_eq!(
            values("limit"),
            vec!["100".to_owned()],
            "the paginator's page size must not be shadowed by a configured one"
        );
        assert_eq!(
            values("since"),
            vec!["2026-01-01T00:00:00Z".to_owned()],
            "the stored mark must not be shadowed by a configured seed"
        );
    }

    #[test]
    fn a_credential_error_does_not_restate_its_own_cause() {
        // Errors reach the operator through `{:#}`, which walks the source
        // chain. A variant that interpolates its own `#[source]` as well prints
        // the underlying failure twice, which reads like two separate problems.
        let error = HttpError::Secret {
            source_name: "api".to_owned(),
            source: SecretError::EnvMissing("TOKEN".to_owned()),
        };
        assert!(
            !format!("{error}").contains("is not set"),
            "the variant restates its cause: {error}"
        );
    }

    #[test]
    fn url_injects_the_cursor_into_the_configured_param() {
        let (client, stream) = client_and_stream();
        let page = PageRequest {
            params: Vec::new(),
            absolute_url: None,
        };
        let url = client
            .url_for(&stream, &page, Some("2024-01-01T00:00:00Z"))
            .expect("url builds");
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            query.get("since").map(String::as_str),
            Some("2024-01-01T00:00:00Z")
        );
    }

    #[test]
    fn an_absolute_next_url_is_used_verbatim() {
        let (client, stream) = client_and_stream();
        let page = PageRequest {
            params: Vec::new(),
            absolute_url: Some("https://api.example.com/v2/items?page=2&cursor=xyz".to_owned()),
        };
        let url = client
            .url_for(&stream, &page, Some("2024-01-01T00:00:00Z"))
            .expect("url builds");
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("page").map(String::as_str), Some("2"));
        assert_eq!(query.get("cursor").map(String::as_str), Some("xyz"));
        // The stream's own params are not re-added on top of the upstream's URL.
        assert_eq!(query.get("state"), None);
        assert_eq!(query.get("since"), None);
    }

    #[test]
    fn a_query_api_key_is_appended_to_every_url() {
        let (client, stream) = client_and_stream();
        let page = PageRequest {
            params: Vec::new(),
            absolute_url: None,
        };
        let url = client.url_for(&stream, &page, None).expect("url builds");
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("key").map(String::as_str), Some("secret-value"));

        // Including one handed to us by the upstream.
        let absolute = PageRequest {
            params: Vec::new(),
            absolute_url: Some("https://api.example.com/v2/items?page=2".to_owned()),
        };
        let url = client
            .url_for(&stream, &absolute, None)
            .expect("url builds");
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("key").map(String::as_str), Some("secret-value"));
    }

    #[test]
    fn a_missing_credential_fails_before_any_request() {
        let text = CONFIG.replace("value: secret-value", "value: ${SIPHON_TEST_UNSET_KEY}");
        let config = Config::parse(&text).expect("config is valid");
        let source = Arc::clone(config.sources.get("api").expect("source exists"));
        let err = SourceClient::new(source).expect_err("should fail");
        assert!(matches!(err, HttpError::Secret { .. }));
    }

    #[test]
    fn excerpts_do_not_split_a_character() {
        let text = "e".repeat(1_999) + "\u{00e9}\u{00e9}";
        let excerpt = excerpt_of(&text);
        assert!(excerpt.len() <= 2_000);
        assert!(excerpt.is_char_boundary(excerpt.len()));
    }
}
