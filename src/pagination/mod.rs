//! Pagination as named state machines rather than user-written code.
//!
//! A [`Paginator`] is a pure function of `(state, response) -> next state`. It
//! never touches the network, which is what makes mid-pagination resume cheap:
//! [`PageState`] round-trips through JSON into the checkpoint after every page,
//! so a crash on page 47 of 200 resumes at page 48.

pub mod link;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json_path::JsonPath;

/// Safety valve: how many pages a single run will walk before giving up.
const DEFAULT_MAX_PAGES: u32 = 10_000;

/// Which pagination state machine a stream uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    /// `limit` / `offset` query parameters.
    Offset,
    /// Page number, optionally with a page-size parameter.
    Page,
    /// Opaque cursor token lifted out of the response body.
    Cursor,
    /// RFC 8288 `Link` header with a `rel`.
    LinkHeader,
    /// A full URL for the next page, found in the response body.
    NextUrl,
    /// One request, no pagination.
    None,
}

/// The pagination block of a stream, as written in YAML.
///
/// This is deliberately flat: a tagged enum would either forbid the
/// cross-cutting options or repeat them per variant, and neither produces a
/// good error when a required field for the chosen strategy is missing.
/// [`Paginator::from_config`] is where the combination is checked.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaginationConfig {
    /// Which state machine to use.
    pub strategy: StrategyKind,

    /// `offset`: name of the page-size parameter. Default `limit`.
    #[serde(default)]
    pub limit_param: Option<String>,
    /// `offset`: name of the offset parameter. Default `offset`.
    #[serde(default)]
    pub offset_param: Option<String>,
    /// `offset` and `page`: how many records to ask for per request.
    #[serde(default)]
    pub page_size: Option<u64>,

    /// `page`: name of the page-number parameter. Default `page`.
    #[serde(default)]
    pub page_param: Option<String>,
    /// `page`: name of the page-size parameter, if the API takes one.
    #[serde(default)]
    pub size_param: Option<String>,
    /// `page`: number of the first page. Default 1.
    #[serde(default)]
    pub start_page: Option<u64>,
    /// `page`: JSONPath to a total-page count in the body, if the API reports one.
    #[serde(default)]
    pub total_pages_path: Option<String>,

    /// `cursor`: name of the query parameter carrying the token.
    #[serde(default)]
    pub cursor_param: Option<String>,
    /// `cursor`: JSONPath to the next token in the response body.
    #[serde(default)]
    pub cursor_path: Option<String>,

    /// `link_header`: which `rel` to follow. Default `next`.
    #[serde(default)]
    pub rel: Option<String>,

    /// `next_url`: JSONPath to a full URL in the response body.
    #[serde(default)]
    pub url_path: Option<String>,
    /// `next_url`: JSONPath to a boolean in the body; `true` ends the walk even
    /// when a next URL is present. Time-based exports keep handing out a next
    /// URL on their last page so a later run knows where to resume.
    #[serde(default)]
    pub end_path: Option<String>,

    /// Hard cap on pages per run. Default 10,000.
    #[serde(default)]
    pub max_pages: Option<u32>,
    /// Stop when a page yields no records. Default true.
    #[serde(default)]
    pub stop_on_empty: Option<bool>,
    /// Stop when a record already in the store is seen. Default false.
    #[serde(default)]
    pub stop_on_seen_key: Option<bool>,
}

impl StrategyKind {
    /// The name as written in YAML.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Offset => "offset",
            Self::Page => "page",
            Self::Cursor => "cursor",
            Self::LinkHeader => "link_header",
            Self::NextUrl => "next_url",
            Self::None => "none",
        }
    }
}

/// A validated pagination strategy with only the fields it actually uses.
#[derive(Debug, Clone)]
enum Strategy {
    Offset {
        limit_param: String,
        offset_param: String,
        page_size: u64,
    },
    Page {
        page_param: String,
        size_param: Option<String>,
        page_size: Option<u64>,
        start_page: u64,
        total_pages: Option<JsonPath>,
    },
    Cursor {
        cursor_param: String,
        cursor_path: JsonPath,
    },
    LinkHeader {
        rel: String,
    },
    NextUrl {
        url_path: JsonPath,
        end_path: Option<JsonPath>,
    },
    Single,
}

/// A pagination state machine, validated at construction.
#[derive(Debug, Clone)]
pub struct Paginator {
    strategy: Strategy,
    max_pages: u32,
    stop_on_empty: bool,
    stop_on_seen_key: bool,
}

/// Why a pagination block could not be turned into a [`Paginator`].
#[derive(Debug, thiserror::Error)]
pub enum PaginationConfigError {
    /// A field the chosen strategy needs was not supplied.
    #[error("pagination strategy `{strategy}` requires `{field}`")]
    MissingField {
        /// The strategy that was chosen.
        strategy: &'static str,
        /// The field it needs.
        field: &'static str,
    },
    /// A JSONPath in the block did not parse.
    #[error("pagination `{field}` is not a valid JSONPath: {source}")]
    BadPath {
        /// Which field held the bad path.
        field: &'static str,
        /// The parse failure.
        #[source]
        source: serde_json_path::ParseError,
    },
    /// A numeric field was out of range.
    #[error("pagination `{field}` must be greater than zero")]
    NotPositive {
        /// Which field was zero.
        field: &'static str,
    },
    /// A field was supplied that the chosen strategy does not use.
    #[error("pagination `{field}` does not apply to strategy `{strategy}`")]
    NotApplicable {
        /// The strategy that was chosen.
        strategy: &'static str,
        /// The field it does not use.
        field: &'static str,
    },
}

impl Paginator {
    /// Validates a YAML pagination block into a state machine.
    ///
    /// # Errors
    ///
    /// Fails when a field the strategy requires is missing, a JSONPath does not
    /// parse, or a size is zero.
    pub fn from_config(cfg: &PaginationConfig) -> Result<Self, PaginationConfigError> {
        if cfg.end_path.is_some() && cfg.strategy != StrategyKind::NextUrl {
            return Err(PaginationConfigError::NotApplicable {
                strategy: cfg.strategy.as_str(),
                field: "end_path",
            });
        }
        let strategy = match cfg.strategy {
            StrategyKind::Offset => {
                let page_size = cfg.page_size.ok_or(PaginationConfigError::MissingField {
                    strategy: "offset",
                    field: "page_size",
                })?;
                if page_size == 0 {
                    return Err(PaginationConfigError::NotPositive { field: "page_size" });
                }
                Strategy::Offset {
                    limit_param: cfg.limit_param.clone().unwrap_or_else(|| "limit".into()),
                    offset_param: cfg.offset_param.clone().unwrap_or_else(|| "offset".into()),
                    page_size,
                }
            }
            StrategyKind::Page => {
                if let Some(0) = cfg.page_size {
                    return Err(PaginationConfigError::NotPositive { field: "page_size" });
                }
                Strategy::Page {
                    page_param: cfg.page_param.clone().unwrap_or_else(|| "page".into()),
                    size_param: cfg.size_param.clone(),
                    page_size: cfg.page_size,
                    start_page: cfg.start_page.unwrap_or(1),
                    total_pages: parse_path(cfg.total_pages_path.as_deref(), "total_pages_path")?,
                }
            }
            StrategyKind::Cursor => Strategy::Cursor {
                cursor_param: cfg.cursor_param.clone().ok_or({
                    PaginationConfigError::MissingField {
                        strategy: "cursor",
                        field: "cursor_param",
                    }
                })?,
                cursor_path: parse_path(cfg.cursor_path.as_deref(), "cursor_path")?.ok_or({
                    PaginationConfigError::MissingField {
                        strategy: "cursor",
                        field: "cursor_path",
                    }
                })?,
            },
            StrategyKind::LinkHeader => Strategy::LinkHeader {
                rel: cfg.rel.clone().unwrap_or_else(|| "next".into()),
            },
            StrategyKind::NextUrl => Strategy::NextUrl {
                url_path: parse_path(cfg.url_path.as_deref(), "url_path")?.ok_or({
                    PaginationConfigError::MissingField {
                        strategy: "next_url",
                        field: "url_path",
                    }
                })?,
                end_path: parse_path(cfg.end_path.as_deref(), "end_path")?,
            },
            StrategyKind::None => Strategy::Single,
        };

        Ok(Self {
            strategy,
            max_pages: cfg.max_pages.unwrap_or(DEFAULT_MAX_PAGES).max(1),
            stop_on_empty: cfg.stop_on_empty.unwrap_or(true),
            stop_on_seen_key: cfg.stop_on_seen_key.unwrap_or(false),
        })
    }

    /// The state a fresh walk starts from.
    #[must_use]
    pub fn initial(&self) -> PageState {
        match &self.strategy {
            Strategy::Offset { .. } => PageState::Offset { offset: 0 },
            Strategy::Page { start_page, .. } => PageState::Page { page: *start_page },
            Strategy::Cursor { .. }
            | Strategy::LinkHeader { .. }
            | Strategy::NextUrl { .. }
            | Strategy::Single => PageState::Start,
        }
    }

    /// The query parameters this strategy sets for itself.
    ///
    /// A stream that also declares one of these under `request.params` is
    /// saying the same thing twice. The paginator's value is the one its own
    /// arithmetic depends on, so that is the one that reaches the API; this
    /// list is what lets `validate` say so before a walk goes wrong.
    #[must_use]
    pub fn reserved_params(&self) -> Vec<&str> {
        match &self.strategy {
            Strategy::Offset {
                limit_param,
                offset_param,
                ..
            } => vec![limit_param.as_str(), offset_param.as_str()],
            Strategy::Page {
                page_param,
                size_param,
                ..
            } => {
                let mut names = vec![page_param.as_str()];
                names.extend(size_param.as_deref());
                names
            }
            Strategy::Cursor { cursor_param, .. } => vec![cursor_param.as_str()],
            Strategy::LinkHeader { .. } | Strategy::NextUrl { .. } | Strategy::Single => Vec::new(),
        }
    }

    /// True when the stream stops as soon as it sees a record it already stored.
    #[must_use]
    pub const fn stops_on_seen_key(&self) -> bool {
        self.stop_on_seen_key
    }

    /// How this state turns into an actual request.
    #[must_use]
    pub fn request_for(&self, state: &PageState) -> PageRequest {
        let mut params = Vec::new();
        match (&self.strategy, state) {
            (
                Strategy::Offset {
                    limit_param,
                    offset_param,
                    page_size,
                },
                PageState::Offset { offset },
            ) => {
                params.push((limit_param.clone(), page_size.to_string()));
                params.push((offset_param.clone(), offset.to_string()));
            }
            (
                Strategy::Page {
                    page_param,
                    size_param,
                    page_size,
                    ..
                },
                PageState::Page { page },
            ) => {
                params.push((page_param.clone(), page.to_string()));
                if let (Some(name), Some(size)) = (size_param, page_size) {
                    params.push((name.clone(), size.to_string()));
                }
            }
            (Strategy::Cursor { cursor_param, .. }, PageState::Cursor { token }) => {
                params.push((cursor_param.clone(), token.clone()));
            }
            (_, PageState::Url { url }) => {
                return PageRequest {
                    params,
                    absolute_url: Some(url.clone()),
                }
            }
            _ => {}
        }
        PageRequest {
            params,
            absolute_url: None,
        }
    }

    /// The state after skipping a page that could not be fetched at all.
    ///
    /// Only positional strategies can skip: the next offset or page number is
    /// computable without the response. A cursor or `Link` walk cannot, because
    /// the only thing that knows where page 48 is, is page 47 - so a poisoned
    /// page there ends the run rather than being stepped over.
    #[must_use]
    pub fn skip(&self, state: &PageState) -> Option<PageState> {
        match (&self.strategy, state) {
            (Strategy::Offset { page_size, .. }, PageState::Offset { offset }) => {
                Some(PageState::Offset {
                    offset: offset.saturating_add(*page_size),
                })
            }
            (Strategy::Page { .. }, PageState::Page { page }) => Some(PageState::Page {
                page: page.saturating_add(1),
            }),
            _ => None,
        }
    }

    /// Decides what follows a page that was fetched and written.
    ///
    /// `pages_fetched` counts pages completed in this run, including this one.
    #[must_use]
    pub fn advance(
        &self,
        state: &PageState,
        page: &PageOutcome<'_>,
        pages_fetched: u32,
    ) -> Advance {
        if self.stop_on_empty && page.records == 0 {
            return Advance::Done(StopReason::EmptyPage);
        }
        if self.stop_on_seen_key && page.saw_known_key {
            return Advance::Done(StopReason::SeenKey);
        }
        if pages_fetched >= self.max_pages {
            return Advance::Done(StopReason::MaxPages);
        }

        match &self.strategy {
            Strategy::Single => Advance::Done(StopReason::SingleRequest),

            Strategy::Offset { page_size, .. } => {
                if page.records < usize::try_from(*page_size).unwrap_or(usize::MAX) {
                    return Advance::Done(StopReason::ShortPage);
                }
                let offset = match state {
                    PageState::Offset { offset } => *offset,
                    _ => 0,
                };
                Advance::Next(PageState::Offset {
                    offset: offset.saturating_add(*page_size),
                })
            }

            Strategy::Page {
                page_size,
                total_pages,
                start_page,
                ..
            } => {
                if let Some(size) = page_size {
                    if page.records < usize::try_from(*size).unwrap_or(usize::MAX) {
                        return Advance::Done(StopReason::ShortPage);
                    }
                }
                let current = match state {
                    PageState::Page { page } => *page,
                    _ => *start_page,
                };
                if let Some(path) = total_pages {
                    if let Some(total) = path.query(page.body).first().and_then(Value::as_u64) {
                        // Pages are numbered from `start_page`, so the last page
                        // is `start_page + total - 1`.
                        if current.saturating_add(1) >= start_page.saturating_add(total) {
                            return Advance::Done(StopReason::TotalPages);
                        }
                    }
                }
                Advance::Next(PageState::Page {
                    page: current.saturating_add(1),
                })
            }

            Strategy::Cursor { cursor_path, .. } => {
                match cursor_path.query(page.body).first().and_then(scalar_token) {
                    Some(token) => Advance::Next(PageState::Cursor { token }),
                    None => Advance::Done(StopReason::NoCursor),
                }
            }

            Strategy::LinkHeader { rel } => {
                match page.link_header.and_then(|h| link::find_rel(h, rel)) {
                    Some(url) => Advance::Next(PageState::Url { url }),
                    None => Advance::Done(StopReason::NoLink),
                }
            }

            Strategy::NextUrl { url_path, end_path } => {
                let ended = end_path
                    .as_ref()
                    .is_some_and(|path| path.query(page.body).first() == Some(&Value::Bool(true)));
                if ended {
                    return Advance::Done(StopReason::EndOfStream);
                }
                let next = url_path.query(page.body).first().and_then(|v| match v {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                });
                match next {
                    // An upstream that answers with the URL it was just asked for
                    // would otherwise be walked until `max_pages`.
                    Some(url) if matches!(state, PageState::Url { url: current } if *current == url) => {
                        Advance::Done(StopReason::RepeatedUrl)
                    }
                    Some(url) => Advance::Next(PageState::Url { url }),
                    None => Advance::Done(StopReason::NoNextUrl),
                }
            }
        }
    }
}

fn parse_path(
    raw: Option<&str>,
    field: &'static str,
) -> Result<Option<JsonPath>, PaginationConfigError> {
    raw.map(|p| {
        JsonPath::parse(p).map_err(|source| PaginationConfigError::BadPath { field, source })
    })
    .transpose()
}

/// Renders a JSON scalar as a token; objects, arrays and nulls have none.
fn scalar_token(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Where a walk currently is. Serialized into the checkpoint verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PageState {
    /// The first request, with no pagination parameters yet.
    Start,
    /// `offset` strategy.
    Offset {
        /// Records to skip.
        offset: u64,
    },
    /// `page` strategy.
    Page {
        /// Page number to request.
        page: u64,
    },
    /// `cursor` strategy.
    Cursor {
        /// Opaque token from the previous response.
        token: String,
    },
    /// A full URL supplied by the previous response.
    Url {
        /// The URL to fetch.
        url: String,
    },
}

impl PageState {
    /// A stable string form, used as the quarantine key and in logs.
    ///
    /// # Errors
    ///
    /// Fails only if the state contains a string that is not valid JSON, which
    /// cannot happen for values produced by a [`Paginator`].
    pub fn to_key(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// What the paginator needs to know about a page it just handled.
#[derive(Debug)]
pub struct PageOutcome<'a> {
    /// How many records the extractor found.
    pub records: usize,
    /// The parsed response body.
    pub body: &'a Value,
    /// The raw `Link` response header, if there was one.
    pub link_header: Option<&'a str>,
    /// Whether any record on the page was already in the store.
    pub saw_known_key: bool,
}

/// The request parameters a page state implies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    /// Query parameters to merge into the configured ones.
    pub params: Vec<(String, String)>,
    /// A complete URL that replaces the configured path, when the API supplied one.
    pub absolute_url: Option<String>,
}

/// The paginator's decision after a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Advance {
    /// Fetch this next.
    Next(PageState),
    /// The walk is complete.
    Done(StopReason),
}

/// Why a walk stopped. Recorded in the run log so a short sync is explainable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The page held no records.
    EmptyPage,
    /// The page held fewer records than the page size.
    ShortPage,
    /// The response carried no next cursor.
    NoCursor,
    /// The response carried no matching `Link` rel.
    NoLink,
    /// The response body carried no next URL.
    NoNextUrl,
    /// The response body flagged the end of the stream.
    EndOfStream,
    /// The response pointed back at the URL just fetched.
    RepeatedUrl,
    /// A record already in the store was seen.
    SeenKey,
    /// The reported total page count was reached.
    TotalPages,
    /// The `max_pages` safety valve tripped.
    MaxPages,
    /// The strategy is a single request.
    SingleRequest,
}

impl StopReason {
    /// A short label for logs and the run record.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmptyPage => "empty_page",
            Self::ShortPage => "short_page",
            Self::NoCursor => "no_cursor",
            Self::NoLink => "no_link",
            Self::NoNextUrl => "no_next_url",
            Self::EndOfStream => "end_of_stream",
            Self::RepeatedUrl => "repeated_url",
            Self::SeenKey => "seen_key",
            Self::TotalPages => "total_pages",
            Self::MaxPages => "max_pages",
            Self::SingleRequest => "single_request",
        }
    }

    /// True when the walk ended because a guard tripped rather than because the
    /// data ran out. These are the cases where the mirror is knowingly incomplete.
    #[must_use]
    pub const fn is_truncation(self) -> bool {
        matches!(self, Self::MaxPages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(strategy: StrategyKind) -> PaginationConfig {
        PaginationConfig {
            strategy,
            limit_param: None,
            offset_param: None,
            page_size: None,
            page_param: None,
            size_param: None,
            start_page: None,
            total_pages_path: None,
            cursor_param: None,
            cursor_path: None,
            rel: None,
            url_path: None,
            end_path: None,
            max_pages: None,
            stop_on_empty: None,
            stop_on_seen_key: None,
        }
    }

    fn outcome<'a>(records: usize, body: &'a Value, link: Option<&'a str>) -> PageOutcome<'a> {
        PageOutcome {
            records,
            body,
            link_header: link,
            saw_known_key: false,
        }
    }

    #[test]
    fn offset_requires_page_size() {
        let err = Paginator::from_config(&cfg(StrategyKind::Offset)).unwrap_err();
        assert!(matches!(
            err,
            PaginationConfigError::MissingField {
                strategy: "offset",
                field: "page_size"
            }
        ));
    }

    #[test]
    fn offset_walks_until_short_page() {
        let mut c = cfg(StrategyKind::Offset);
        c.page_size = Some(100);
        let p = Paginator::from_config(&c).unwrap();

        let state = p.initial();
        assert_eq!(state, PageState::Offset { offset: 0 });
        assert_eq!(
            p.request_for(&state).params,
            vec![
                ("limit".to_owned(), "100".to_owned()),
                ("offset".to_owned(), "0".to_owned())
            ]
        );

        let body = json!([]);
        let next = p.advance(&state, &outcome(100, &body, None), 1);
        assert_eq!(next, Advance::Next(PageState::Offset { offset: 100 }));

        let state = PageState::Offset { offset: 100 };
        let next = p.advance(&state, &outcome(42, &body, None), 2);
        assert_eq!(next, Advance::Done(StopReason::ShortPage));
    }

    #[test]
    fn offset_honours_custom_param_names() {
        let mut c = cfg(StrategyKind::Offset);
        c.page_size = Some(50);
        c.limit_param = Some("per_page".into());
        c.offset_param = Some("skip".into());
        let p = Paginator::from_config(&c).unwrap();
        assert_eq!(
            p.request_for(&p.initial()).params,
            vec![
                ("per_page".to_owned(), "50".to_owned()),
                ("skip".to_owned(), "0".to_owned())
            ]
        );
    }

    #[test]
    fn page_starts_at_one_and_increments() {
        let p = Paginator::from_config(&cfg(StrategyKind::Page)).unwrap();
        let state = p.initial();
        assert_eq!(state, PageState::Page { page: 1 });
        let body = json!({});
        assert_eq!(
            p.advance(&state, &outcome(10, &body, None), 1),
            Advance::Next(PageState::Page { page: 2 })
        );
    }

    #[test]
    fn page_stops_at_reported_total() {
        let mut c = cfg(StrategyKind::Page);
        c.total_pages_path = Some("$.total_pages".into());
        let p = Paginator::from_config(&c).unwrap();
        let body = json!({ "total_pages": 3 });

        assert_eq!(
            p.advance(&PageState::Page { page: 1 }, &outcome(10, &body, None), 1),
            Advance::Next(PageState::Page { page: 2 })
        );
        assert_eq!(
            p.advance(&PageState::Page { page: 3 }, &outcome(10, &body, None), 3),
            Advance::Done(StopReason::TotalPages)
        );
    }

    #[test]
    fn page_total_respects_zero_based_numbering() {
        let mut c = cfg(StrategyKind::Page);
        c.total_pages_path = Some("$.total_pages".into());
        c.start_page = Some(0);
        let p = Paginator::from_config(&c).unwrap();
        let body = json!({ "total_pages": 2 });

        assert_eq!(
            p.advance(&PageState::Page { page: 0 }, &outcome(10, &body, None), 1),
            Advance::Next(PageState::Page { page: 1 })
        );
        assert_eq!(
            p.advance(&PageState::Page { page: 1 }, &outcome(10, &body, None), 2),
            Advance::Done(StopReason::TotalPages)
        );
    }

    #[test]
    fn cursor_follows_body_token_then_stops() {
        let mut c = cfg(StrategyKind::Cursor);
        c.cursor_param = Some("after".into());
        c.cursor_path = Some("$.meta.next".into());
        let p = Paginator::from_config(&c).unwrap();

        let body = json!({ "meta": { "next": "abc123" } });
        let next = p.advance(&PageState::Start, &outcome(10, &body, None), 1);
        assert_eq!(
            next,
            Advance::Next(PageState::Cursor {
                token: "abc123".to_owned()
            })
        );

        let state = PageState::Cursor {
            token: "abc123".to_owned(),
        };
        assert_eq!(
            p.request_for(&state).params,
            vec![("after".to_owned(), "abc123".to_owned())]
        );

        let exhausted = json!({ "meta": { "next": null } });
        assert_eq!(
            p.advance(&state, &outcome(10, &exhausted, None), 2),
            Advance::Done(StopReason::NoCursor)
        );
    }

    #[test]
    fn cursor_treats_empty_string_as_exhausted() {
        let mut c = cfg(StrategyKind::Cursor);
        c.cursor_param = Some("after".into());
        c.cursor_path = Some("$.next".into());
        let p = Paginator::from_config(&c).unwrap();
        let body = json!({ "next": "" });
        assert_eq!(
            p.advance(&PageState::Start, &outcome(10, &body, None), 1),
            Advance::Done(StopReason::NoCursor)
        );
    }

    #[test]
    fn link_header_follows_next_as_absolute_url() {
        let p = Paginator::from_config(&cfg(StrategyKind::LinkHeader)).unwrap();
        let body = json!([1, 2]);
        let header = r#"<https://api.example.com/items?page=2>; rel="next""#;
        let next = p.advance(&PageState::Start, &outcome(2, &body, Some(header)), 1);
        assert_eq!(
            next,
            Advance::Next(PageState::Url {
                url: "https://api.example.com/items?page=2".to_owned()
            })
        );

        let state = PageState::Url {
            url: "https://api.example.com/items?page=2".to_owned(),
        };
        let req = p.request_for(&state);
        assert_eq!(
            req.absolute_url.as_deref(),
            Some("https://api.example.com/items?page=2")
        );
        assert!(req.params.is_empty());

        assert_eq!(
            p.advance(&state, &outcome(2, &body, None), 2),
            Advance::Done(StopReason::NoLink)
        );
    }

    #[test]
    fn empty_page_stops_every_strategy() {
        let mut c = cfg(StrategyKind::Offset);
        c.page_size = Some(100);
        let p = Paginator::from_config(&c).unwrap();
        let body = json!([]);
        assert_eq!(
            p.advance(&p.initial(), &outcome(0, &body, None), 1),
            Advance::Done(StopReason::EmptyPage)
        );
    }

    #[test]
    fn stop_on_empty_can_be_disabled() {
        let mut c = cfg(StrategyKind::Offset);
        c.page_size = Some(100);
        c.stop_on_empty = Some(false);
        let p = Paginator::from_config(&c).unwrap();
        let body = json!([]);
        // Still short, so it stops - but for the short-page reason, not emptiness.
        assert_eq!(
            p.advance(&p.initial(), &outcome(0, &body, None), 1),
            Advance::Done(StopReason::ShortPage)
        );
    }

    #[test]
    fn seen_key_stops_when_enabled() {
        let mut c = cfg(StrategyKind::Page);
        c.stop_on_seen_key = Some(true);
        let p = Paginator::from_config(&c).unwrap();
        let body = json!({});
        let page = PageOutcome {
            records: 10,
            body: &body,
            link_header: None,
            saw_known_key: true,
        };
        assert_eq!(
            p.advance(&p.initial(), &page, 1),
            Advance::Done(StopReason::SeenKey)
        );
    }

    #[test]
    fn max_pages_is_a_hard_stop() {
        let mut c = cfg(StrategyKind::Page);
        c.max_pages = Some(2);
        let p = Paginator::from_config(&c).unwrap();
        let body = json!({});
        assert_eq!(
            p.advance(&PageState::Page { page: 2 }, &outcome(10, &body, None), 2),
            Advance::Done(StopReason::MaxPages)
        );
        assert!(StopReason::MaxPages.is_truncation());
        assert!(!StopReason::ShortPage.is_truncation());
    }

    #[test]
    fn none_strategy_is_a_single_request() {
        let p = Paginator::from_config(&cfg(StrategyKind::None)).unwrap();
        assert_eq!(p.initial(), PageState::Start);
        let body = json!({});
        assert_eq!(
            p.advance(&PageState::Start, &outcome(5, &body, None), 1),
            Advance::Done(StopReason::SingleRequest)
        );
    }

    #[test]
    fn next_url_requires_url_path() {
        let err = Paginator::from_config(&cfg(StrategyKind::NextUrl)).unwrap_err();
        assert!(matches!(
            err,
            PaginationConfigError::MissingField {
                strategy: "next_url",
                field: "url_path"
            }
        ));
    }

    fn next_url(end_path: Option<&str>) -> Paginator {
        let mut c = cfg(StrategyKind::NextUrl);
        c.url_path = Some("$.next_page".into());
        c.end_path = end_path.map(Into::into);
        Paginator::from_config(&c).unwrap()
    }

    #[test]
    fn next_url_follows_the_body_url_as_an_absolute_request() {
        let p = next_url(None);
        assert_eq!(p.initial(), PageState::Start);
        assert!(p.reserved_params().is_empty());

        let body = json!({ "next_page": "https://api.example.com/items?start=2" });
        let next = p.advance(&PageState::Start, &outcome(10, &body, None), 1);
        assert_eq!(
            next,
            Advance::Next(PageState::Url {
                url: "https://api.example.com/items?start=2".to_owned()
            })
        );

        let state = PageState::Url {
            url: "https://api.example.com/items?start=2".to_owned(),
        };
        let req = p.request_for(&state);
        assert_eq!(
            req.absolute_url.as_deref(),
            Some("https://api.example.com/items?start=2")
        );
        assert!(req.params.is_empty());
    }

    #[test]
    fn next_url_stops_when_the_url_is_absent_null_or_empty() {
        let p = next_url(None);
        for body in [
            json!({}),
            json!({ "next_page": null }),
            json!({ "next_page": "" }),
        ] {
            assert_eq!(
                p.advance(&PageState::Start, &outcome(10, &body, None), 1),
                Advance::Done(StopReason::NoNextUrl)
            );
        }
    }

    #[test]
    fn next_url_stops_on_the_end_flag_even_with_a_url_present() {
        // Time-based exports keep handing out a next URL on the last page so the
        // next run knows where to resume; a flag in the body says this walk is done.
        let p = next_url(Some("$.end_of_stream"));
        let last = json!({
            "next_page": "https://api.example.com/items?start=9",
            "end_of_stream": true
        });
        assert_eq!(
            p.advance(&PageState::Start, &outcome(10, &last, None), 1),
            Advance::Done(StopReason::EndOfStream)
        );

        let more = json!({
            "next_page": "https://api.example.com/items?start=9",
            "end_of_stream": false
        });
        assert!(matches!(
            p.advance(&PageState::Start, &outcome(10, &more, None), 1),
            Advance::Next(PageState::Url { .. })
        ));
    }

    #[test]
    fn next_url_stops_rather_than_refetch_the_same_url() {
        let p = next_url(None);
        let url = "https://api.example.com/items?start=5".to_owned();
        let body = json!({ "next_page": url });
        assert_eq!(
            p.advance(
                &PageState::Url { url: url.clone() },
                &outcome(10, &body, None),
                2
            ),
            Advance::Done(StopReason::RepeatedUrl)
        );
    }

    #[test]
    fn next_url_rejects_a_bad_end_path() {
        let mut c = cfg(StrategyKind::NextUrl);
        c.url_path = Some("$.next_page".into());
        c.end_path = Some("not a path".into());
        assert!(matches!(
            Paginator::from_config(&c).unwrap_err(),
            PaginationConfigError::BadPath {
                field: "end_path",
                ..
            }
        ));
    }

    #[test]
    fn end_path_is_only_for_next_url() {
        let mut c = cfg(StrategyKind::Page);
        c.end_path = Some("$.done".into());
        assert!(matches!(
            Paginator::from_config(&c).unwrap_err(),
            PaginationConfigError::NotApplicable {
                strategy: "page",
                field: "end_path"
            }
        ));
    }

    #[test]
    fn a_next_url_walk_cannot_step_over_a_poisoned_page() {
        let p = next_url(None);
        assert_eq!(
            p.skip(&PageState::Url {
                url: "https://x/2".to_owned()
            }),
            None
        );
    }

    #[test]
    fn positional_strategies_can_step_over_a_poisoned_page() {
        let mut offset = cfg(StrategyKind::Offset);
        offset.page_size = Some(100);
        let p = Paginator::from_config(&offset).unwrap();
        assert_eq!(
            p.skip(&PageState::Offset { offset: 4_600 }),
            Some(PageState::Offset { offset: 4_700 })
        );

        let p = Paginator::from_config(&cfg(StrategyKind::Page)).unwrap();
        assert_eq!(
            p.skip(&PageState::Page { page: 47 }),
            Some(PageState::Page { page: 48 })
        );
    }

    #[test]
    fn cursor_and_link_walks_cannot_step_over_a_poisoned_page() {
        let mut cursor = cfg(StrategyKind::Cursor);
        cursor.cursor_param = Some("after".into());
        cursor.cursor_path = Some("$.next".into());
        let p = Paginator::from_config(&cursor).unwrap();
        assert_eq!(
            p.skip(&PageState::Cursor {
                token: "t".to_owned()
            }),
            None
        );

        let p = Paginator::from_config(&cfg(StrategyKind::LinkHeader)).unwrap();
        assert_eq!(
            p.skip(&PageState::Url {
                url: "https://x/2".to_owned()
            }),
            None
        );
        assert_eq!(p.skip(&PageState::Start), None);
    }

    #[test]
    fn page_state_round_trips_through_the_checkpoint() {
        for state in [
            PageState::Start,
            PageState::Offset { offset: 300 },
            PageState::Page { page: 7 },
            PageState::Cursor {
                token: "t0k".to_owned(),
            },
            PageState::Url {
                url: "https://x/2".to_owned(),
            },
        ] {
            let key = state.to_key().unwrap();
            let back: PageState = serde_json::from_str(&key).unwrap();
            assert_eq!(state, back);
        }
    }
}
