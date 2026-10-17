//! Requests to upstream chain APIs: the pooled HTTP/1 client, how a client
//! request is rebuilt for an upstream, one attempt and its timeout, and the
//! bodies that carry an upstream answer to the client.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, DATE,
    HOST, TE, TRAILER, TRANSFER_ENCODING, UPGRADE, USER_AGENT,
};
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::time::timeout;

use super::directory::{ApiSlot, SlotGuard};
use super::RequestPermit;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The body of every chain API answer.
pub type ApiBody = UnsyncBoxBody<Bytes, BoxError>;

/// How long an idle pooled upstream connection is kept.
const POOL_IDLE: Duration = Duration::from_secs(30);

/// Request headers copied to the upstream. Everything else, `Expect`,
/// `Accept-Encoding` and hop-by-hop headers included, is dropped; the
/// router adds `Host` and `Content-Length` itself.
const FORWARDED: [HeaderName; 3] = [CONTENT_TYPE, ACCEPT, USER_AGENT];

/// Response headers never relayed: hop-by-hop ones and `Date` (nodeos
/// sends none, and the router adds none).
fn dropped_response_header(name: &HeaderName) -> bool {
    matches!(
        *name,
        CONNECTION | TE | TRAILER | TRANSFER_ENCODING | UPGRADE | DATE
    ) || matches!(
        name.as_str(),
        "keep-alive" | "proxy-connection" | "proxy-authenticate" | "proxy-authorization"
    )
}

/// The client request as it goes to every upstream: same method, target and
/// body, with only a few headers.
#[derive(Debug, Clone)]
pub struct Call {
    pub method: Method,
    /// The origin-form target, query string included.
    pub target: String,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Call {
    pub fn new(method: Method, target: &str, client_headers: &HeaderMap, body: Bytes) -> Call {
        let mut headers = HeaderMap::new();
        for name in FORWARDED {
            for value in client_headers.get_all(&name) {
                headers.append(name.clone(), value.clone());
            }
        }
        Call {
            method,
            target: target.to_string(),
            headers,
            body,
        }
    }

    /// The request for `slot`: `Host` is the API's configured authority,
    /// which nodeos validates by default, and the body is byte for byte the
    /// client's. A HEAD request asks the upstream to close the connection:
    /// nodeos writes a body even for HEAD, and those bytes must not reach
    /// the next request on a pooled connection.
    pub fn request_for(&self, slot: &ApiSlot) -> Result<Request<Full<Bytes>>, hyper::http::Error> {
        let mut builder = Request::builder()
            .method(self.method.clone())
            .uri(format!("{}{}", slot.base, self.target))
            .header(HOST, slot.authority.clone());
        for (name, value) in &self.headers {
            builder = builder.header(name, value);
        }
        builder = builder.header(CONTENT_LENGTH, self.body.len());
        if self.method == Method::HEAD {
            builder = builder.header(CONNECTION, HeaderValue::from_static("close"));
        }
        builder.body(Full::new(self.body.clone()))
    }
}

/// What one attempt on one upstream gave.
#[derive(Debug)]
pub enum Attempt {
    /// A response head; its body is still to be read.
    Response(Response<Incoming>),
    /// The connection could not be made: nothing was written.
    Connect,
    /// The connection failed after the request may have been written.
    Lost,
    /// No response head in time.
    Timeout,
}

/// The pooled HTTP/1 client of one listener's chain API.
#[derive(Clone, Debug)]
pub struct UpstreamClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl UpstreamClient {
    /// `connect_timeout` bounds each TCP connect; at most `max_idle` idle
    /// connections are kept per upstream, for 30 s.
    pub fn new(connect_timeout: Duration, max_idle: usize) -> UpstreamClient {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(connect_timeout));
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(POOL_IDLE)
            .pool_max_idle_per_host(max_idle)
            .pool_timer(TokioTimer::new())
            .timer(TokioTimer::new())
            .build(connector);
        UpstreamClient { client }
    }

    /// Send `request`; the response head must arrive within `head_timeout`.
    /// hyper-util retries on a new connection only a request it never
    /// wrote (a pooled connection that turned out closed).
    pub async fn send(&self, request: Request<Full<Bytes>>, head_timeout: Duration) -> Attempt {
        match timeout(head_timeout, self.client.request(request)).await {
            Err(_) => Attempt::Timeout,
            Ok(Ok(response)) => Attempt::Response(response),
            Ok(Err(e)) if e.is_connect() => Attempt::Connect,
            Ok(Err(_)) => Attempt::Lost,
        }
    }
}

/// An upstream answer, read whole: kept to be sent later, or classified.
#[derive(Debug, Clone)]
pub struct Buffered {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Buffered {
    /// The client's copy, holding `hold` until it is sent.
    pub fn into_response(self, hold: Hold) -> Response<ApiBody> {
        let body = Held::body(full(self.body), hold);
        let mut response = Response::new(body);
        *response.status_mut() =
            hyper::StatusCode::from_u16(self.status).unwrap_or(hyper::StatusCode::BAD_GATEWAY);
        *response.headers_mut() = self.headers;
        response
    }
}

/// The relayed head of an upstream answer: its status and every header
/// but hop-by-hop ones and `Date`. `Server` passes through. CORS headers
/// are left for the listener to strip or replace.
pub fn relay_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(upstream.len());
    // Names a `Connection` header lists are hop-by-hop too.
    let listed: Vec<String> = upstream
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    for (name, value) in upstream {
        if dropped_response_header(name) || listed.iter().any(|l| l == name.as_str()) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// An upstream answer streamed to the client.
pub fn streamed(response: Response<Incoming>, hold: Hold) -> Response<ApiBody> {
    let (parts, body) = response.into_parts();
    let body = Held::body(body.map_err(BoxError::from).boxed_unsync(), hold);
    let mut out = Response::new(body);
    *out.status_mut() = parts.status;
    *out.headers_mut() = relay_headers(&parts.headers);
    out
}

/// What [`peek`] read of a body.
pub enum Peeked {
    /// The whole body, at most the limit.
    Whole(Bytes),
    /// More than the limit: what was read, and the rest of the body.
    Partial {
        prefix: VecDeque<Bytes>,
        rest: Incoming,
    },
}

/// Read `body` whole if it is at most `limit` bytes.
pub async fn peek(mut body: Incoming, limit: usize) -> Result<Peeked, hyper::Error> {
    let mut whole = BytesMut::new();
    let mut prefix = VecDeque::new();
    let mut read = 0usize;
    while let Some(frame) = body.frame().await {
        let Ok(data) = frame?.into_data() else {
            continue;
        };
        read += data.len();
        prefix.push_back(data);
        if read > limit {
            return Ok(Peeked::Partial { prefix, rest: body });
        }
    }
    for chunk in prefix {
        whole.extend_from_slice(&chunk);
    }
    Ok(Peeked::Whole(whole.freeze()))
}

/// A body of already-read chunks followed by the rest of an upstream body.
pub struct Replay {
    prefix: VecDeque<Bytes>,
    rest: Incoming,
}

impl Replay {
    pub fn body(prefix: VecDeque<Bytes>, rest: Incoming) -> ApiBody {
        Replay { prefix, rest }.boxed_unsync()
    }
}

impl Body for Replay {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        if let Some(chunk) = self.prefix.pop_front() {
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        Pin::new(&mut self.rest)
            .poll_frame(cx)
            .map_err(BoxError::from)
    }
}

/// What a request holds until its answer is sent: the upstream slot and
/// the listener's in-flight permit.
#[derive(Default)]
pub struct Hold {
    _slot: Option<SlotGuard>,
    _request: Option<RequestPermit>,
}

impl Hold {
    pub fn new(request: Option<RequestPermit>, slot: Option<SlotGuard>) -> Hold {
        Hold {
            _slot: slot,
            _request: request,
        }
    }
}

/// A body that keeps its [`Hold`] until it ends or is dropped.
pub struct Held {
    inner: ApiBody,
    hold: Option<Hold>,
}

impl Held {
    pub fn body(inner: ApiBody, hold: Hold) -> ApiBody {
        Held {
            inner,
            hold: Some(hold),
        }
        .boxed_unsync()
    }
}

impl Body for Held {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(polled, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.hold = None;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// A complete body.
pub fn full(bytes: impl Into<Bytes>) -> ApiBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_end_to_end_headers_are_relayed() {
        let mut upstream = HeaderMap::new();
        for (name, value) in [
            ("content-type", "application/json"),
            ("content-length", "12"),
            ("server", "nodeos/v1.2.2"),
            ("date", "Sat, 17 Oct 2026 00:00:00 GMT"),
            ("connection", "keep-alive, x-private"),
            ("keep-alive", "timeout=5"),
            ("transfer-encoding", "chunked"),
            ("x-private", "1"),
            ("x-public", "2"),
            ("access-control-allow-origin", "*"),
        ] {
            upstream.append(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
        let relayed = relay_headers(&upstream);
        let names: Vec<&str> = relayed.keys().map(|n| n.as_str()).collect();
        assert_eq!(
            names,
            [
                "content-type",
                "content-length",
                "server",
                "x-public",
                "access-control-allow-origin"
            ]
        );
    }

    #[test]
    fn forwarded_request_headers_are_few() {
        let mut client = HeaderMap::new();
        for (name, value) in [
            ("content-type", "application/json"),
            ("accept", "*/*"),
            ("user-agent", "test"),
            ("expect", "100-continue"),
            ("accept-encoding", "gzip"),
            ("x-forwarded-for", "10.0.0.1"),
            ("connection", "keep-alive"),
            ("host", "client.example"),
        ] {
            client.append(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
        let call = Call::new(
            Method::HEAD,
            "/v1/chain/get_info",
            &client,
            Bytes::from_static(b"{}"),
        );
        let names: Vec<&str> = call.headers.keys().map(|n| n.as_str()).collect();
        assert_eq!(names, ["content-type", "accept", "user-agent"]);
        let api = crate::models::ApiUpstream {
            index: 0,
            name: "a".into(),
            endpoint: "127.0.0.1:1".into(),
            role: crate::models::Role::Live,
            api: crate::models::ApiEndpoint::parse("http://Node.Example:8888").unwrap(),
            priority: 0,
            weight: 1,
            range: None,
        };
        let dir = super::super::directory::ApiDirectory::new(&[api], 1);
        let request = call.request_for(dir.slot(0).unwrap()).unwrap();
        assert_eq!(request.uri(), "http://Node.Example:8888/v1/chain/get_info");
        assert_eq!(request.headers()[HOST], "Node.Example:8888");
        assert_eq!(request.headers()[CONTENT_LENGTH], "2");
        assert_eq!(request.headers()[CONNECTION], "close");
        assert_eq!(request.method(), Method::HEAD);
    }
}
