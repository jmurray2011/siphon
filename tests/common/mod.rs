//! A scriptable HTTP/1.1 server, so the end-to-end tests exercise the real
//! client, the real pagination and the real writes without a network.
//!
//! Hand-rolled rather than pulled from a mocking crate because these tests need
//! exact control over the things siphon is built around - `Link` headers, 304s,
//! `Retry-After`, rate-limit headers - and that is easier to state directly than
//! to configure.

// Integration tests are their own crates, so `allow-unwrap-in-tests` in
// clippy.toml does not reach helpers that sit outside a `#[test]` function.
// Panicking on a broken fixture is the correct behaviour here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One request the server received.
#[derive(Debug, Clone)]
pub struct Request {
    /// The request target, path only.
    pub path: String,
    /// Query parameters, decoded.
    pub query: BTreeMap<String, String>,
    /// Request headers, lowercased names.
    pub headers: BTreeMap<String, String>,
}

impl Request {
    /// The value of a query parameter.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query.get(name).map(String::as_str)
    }

    /// The value of a request header.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

/// A scripted response.
#[derive(Debug, Clone)]
pub struct Reply {
    /// The status code.
    pub status: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: String,
}

impl Reply {
    /// A 200 with a JSON body.
    pub fn json(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body: body.into(),
        }
    }

    /// A bare status with no body.
    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: String::new(),
        }
    }

    /// Adds a response header.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }
}

/// A running mock API.
pub struct MockApi {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Request>>>,
}

impl MockApi {
    /// Starts a server that answers every request with `handler`.
    pub async fn spawn<F>(handler: F) -> Self
    where
        F: Fn(&Request) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock api binds");
        let addr = listener.local_addr().expect("mock api has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));

        let handler = Arc::new(handler);
        let recorded = Arc::clone(&seen);

        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let handler = Arc::clone(&handler);
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    serve(socket, &*handler, &recorded).await;
                });
            }
        });

        Self { addr, seen }
    }

    /// The base URL to point a source at.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Every request the server has answered, in order.
    pub fn requests(&self) -> Vec<Request> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// How many requests the server has answered.
    pub fn request_count(&self) -> usize {
        self.requests().len()
    }
}

async fn serve<F>(mut socket: TcpStream, handler: &F, recorded: &Mutex<Vec<Request>>)
where
    F: Fn(&Request) -> Reply + Send + Sync,
{
    let Some(head) = read_head(&mut socket).await else {
        return;
    };
    let Some(request) = parse(&head) else {
        return;
    };

    recorded
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(request.clone());

    let reply = handler(&request);
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reason(reply.status),
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(&reply.body);

    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.flush().await;
    let _ = socket.shutdown().await;
}

async fn read_head(socket: &mut TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
            return String::from_utf8(buffer).ok();
        }
        if buffer.len() > 64 * 1024 {
            return None;
        }
    }
}

fn parse(head: &str) -> Option<Request> {
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let target = request_line.split_whitespace().nth(1)?;

    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let query = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect();

    let headers = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();

    Some(Request {
        path: path.to_owned(),
        query,
        headers,
    })
}

fn decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        304 => "Not Modified",
        403 => "Forbidden",
        404 => "Not Found",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
