//! Observability: request IDs, the per-request log record and the response body meter.

use std::fmt::Write as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::header::HeaderMap;
use hyper::{Method, StatusCode};
use tracing::{info, warn};

use crate::error::BoxError;
use crate::proxy::RespBody;

/// Header used both to accept a caller supplied request ID and to echo it back.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Longest request ID accepted from a caller; anything longer is replaced.
const MAX_REQUEST_ID_LEN: usize = 128;

/// Reuse the incoming request ID when the caller supplied a usable one, otherwise generate one.
///
/// Unusable means empty, longer than [`MAX_REQUEST_ID_LEN`], non ASCII or containing control
/// characters — such a value would end up verbatim in every log line and response header.
pub fn request_id(headers: &HeaderMap) -> String {
    if let Some(value) = headers.get(REQUEST_ID_HEADER)
        && let Ok(raw) = value.to_str()
    {
        let trimmed = raw.trim();
        if !trimmed.is_empty()
            && trimmed.len() <= MAX_REQUEST_ID_LEN
            && trimmed.is_ascii()
            && !trimmed.chars().any(char::is_control)
        {
            return trimmed.to_string();
        }
    }
    generate_request_id()
}

/// Random UUID v4 shaped identifier, matching what people expect from `x-request-id`.
fn generate_request_id() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::getrandom(&mut bytes).is_err() {
        // Practically unreachable on Linux; fall back to a time based value so a request
        // never ends up without an ID.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        bytes.copy_from_slice(&nanos.to_le_bytes());
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let mut out = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Everything one request contributes to its log line. The line is emitted once the
/// response body is done, so the byte counts and the latency are the real ones.
pub struct RequestLog {
    pub request_id: String,
    pub method: Method,
    pub path: String,
    pub started: Instant,
    /// Caller description from `auth.keys`, `-` in the log when auth is disabled.
    pub caller: Option<String>,
    pub model: Option<String>,
    pub upstream: Option<String>,
    pub status: StatusCode,
    /// `true` for server-sent event responses.
    pub streaming: bool,
    pub request_bytes: u64,
    pub error: Option<String>,
}

impl RequestLog {
    /// Start a record for one request; everything else is filled in as it progresses.
    pub fn new(request_id: String, method: Method, path: String, started: Instant) -> Self {
        Self {
            request_id,
            method,
            path,
            started,
            caller: None,
            model: None,
            upstream: None,
            status: StatusCode::OK,
            streaming: false,
            request_bytes: 0,
            error: None,
        }
    }

    /// Emit the log line. `aborted` means the response body was dropped before the
    /// stream ended, which is what a disconnected client looks like.
    pub fn finish(self, response_bytes: u64, aborted: bool) {
        let Self {
            request_id,
            method,
            path,
            started,
            caller,
            model,
            upstream,
            status,
            streaming,
            request_bytes,
            error,
        } = self;

        let failed = error.is_some() || status.is_client_error() || status.is_server_error();
        let outcome = match (&error, aborted) {
            (Some(_), _) => "request failed",
            (None, true) => "request aborted",
            (None, false) => "request completed",
        };

        let caller = caller.as_deref().unwrap_or("-");
        let model = model.as_deref().unwrap_or("-");
        let upstream = upstream.as_deref().unwrap_or("-");
        let error = error.as_deref().unwrap_or("-");
        let status = status.as_u16();
        let elapsed_ms = started.elapsed().as_millis() as u64;

        // Strings are recorded with `%` so the log stays unquoted and greppable.
        if failed {
            warn!(
                request_id = %request_id,
                caller = %caller,
                model = %model,
                upstream = %upstream,
                status,
                elapsed_ms,
                request_bytes,
                response_bytes,
                stream = streaming,
                aborted,
                method = %method,
                path = %path,
                error = %error,
                "{outcome}"
            );
        } else {
            info!(
                request_id = %request_id,
                caller = %caller,
                model = %model,
                upstream = %upstream,
                status,
                elapsed_ms,
                request_bytes,
                response_bytes,
                stream = streaming,
                aborted,
                method = %method,
                path = %path,
                error = %error,
                "{outcome}"
            );
        }
    }
}

/// Wraps a response body, counts the bytes handed to the client and reports the total
/// when the body is dropped — that is, when the response finished or the client went away.
pub struct MeteredBody {
    inner: RespBody,
    report: Option<Box<dyn FnOnce(u64, bool) + Send + Sync>>,
    /// Exact size when the inner body announces one. Needed because hyper stops polling a
    /// sized body once it has written that many bytes, so the end of the stream is never
    /// observed through `poll_frame` in that case.
    expected: Option<u64>,
    bytes: u64,
    finished: bool,
}

impl MeteredBody {
    pub fn new(inner: RespBody, report: Box<dyn FnOnce(u64, bool) + Send + Sync>) -> Self {
        let expected = inner.size_hint().exact();
        Self {
            inner,
            report: Some(report),
            expected,
            bytes: 0,
            finished: false,
        }
    }

    /// The whole body was handed to the client: either the end of the stream was polled, or
    /// every announced byte was written.
    fn is_complete(&self) -> bool {
        self.finished || self.expected == Some(self.bytes)
    }

    /// Bytes forwarded to the client so far. Only the tests inspect the meter
    /// directly; in production the total is reported through the drop callback.
    #[cfg(test)]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether the inner body signalled the end of the stream.
    #[cfg(test)]
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Body for MeteredBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        // `BoxBody` is a wrapper around `Pin<Box<dyn Body..>>` and is `Unpin`, so it can be
        // re-pinned for the poll.
        match std::task::ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    this.bytes += data.len() as u64;
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
            None => {
                this.finished = true;
                Poll::Ready(None)
            }
        }
    }

    // Framing must stay transparent, otherwise a known content length would turn into
    // chunked encoding just because the body is wrapped.
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        // Never claim the end of the stream based on our own count: hyper uses this before
        // polling and would truncate the body.
        self.inner.is_end_stream()
    }
}

impl Drop for MeteredBody {
    fn drop(&mut self) {
        if let Some(report) = self.report.take() {
            report(self.bytes, !self.is_complete());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use hyper::header::HeaderValue;
    use std::collections::VecDeque;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::task::Waker;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn reuses_a_usable_request_id() {
        let headers = headers(&[("x-request-id", "  trace-me-123  ")]);
        assert_eq!(request_id(&headers), "trace-me-123");
    }

    #[test]
    fn replaces_unusable_request_ids() {
        // Missing, empty, too long and non ASCII values are all replaced.
        assert_ne!(request_id(&HeaderMap::new()).len(), 0);
        assert_eq!(request_id(&headers(&[("x-request-id", "   ")])).len(), 36);
        assert_eq!(
            request_id(&headers(&[("x-request-id", &"x".repeat(200))])).len(),
            36
        );
        assert_eq!(
            request_id(&headers(&[("x-request-id", "中文请求标识")])).len(),
            36
        );
    }

    #[test]
    fn generated_request_ids_look_like_uuids_and_are_unique() {
        let first = generate_request_id();
        let second = generate_request_id();
        assert_ne!(first, second);
        assert_eq!(first.len(), 36);
        assert_eq!(first.matches('-').count(), 4);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        // Version 4 and the RFC 4122 variant.
        assert_eq!(&first[14..15], "4");
        assert!(matches!(&first[19..20], "8" | "9" | "a" | "b"));
    }

    /// Test body without a known length, i.e. what a chunked or streamed response looks like.
    struct Frames(VecDeque<Bytes>);

    impl Body for Frames {
        type Data = Bytes;
        type Error = BoxError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
            Poll::Ready(
                self.get_mut()
                    .0
                    .pop_front()
                    .map(|data| Ok(Frame::data(data))),
            )
        }
    }

    fn frames(chunks: &[&'static [u8]]) -> RespBody {
        let frames = chunks
            .iter()
            .map(|chunk| Bytes::from_static(chunk))
            .collect();
        Frames(frames).boxed()
    }

    /// What the meter reports on drop: bytes sent, and whether the stream was aborted.
    type DropReport = (u64, bool);

    fn meter(inner: RespBody) -> (MeteredBody, Arc<Mutex<Option<DropReport>>>) {
        let seen = Arc::new(Mutex::new(None));
        let recorder = Arc::clone(&seen);
        let body = MeteredBody::new(
            inner,
            Box::new(move |bytes, aborted| *recorder.lock().unwrap() = Some((bytes, aborted))),
        );
        (body, seen)
    }

    fn drive(body: &mut MeteredBody, waker: &Waker) -> Option<usize> {
        let mut cx = Context::from_waker(waker);
        match Pin::new(body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => Some(frame.data_ref().map(|d| d.len()).unwrap_or(0)),
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) | Poll::Pending => None,
        }
    }

    #[test]
    fn sized_bodies_count_as_complete_without_an_end_of_stream_frame() {
        // hyper writes a body with a known length and stops there, so the end of the stream
        // is never polled: the byte count has to be enough to call it complete.
        let inner = Full::new(Bytes::from_static(b"hello"))
            .map_err(|e: std::convert::Infallible| -> BoxError { match e {} })
            .boxed();
        let (mut body, seen) = meter(inner);

        assert_eq!(drive(&mut body, Waker::noop()), Some(5));
        assert!(!body.is_finished(), "no end of stream frame was polled yet");

        drop(body);
        assert_eq!(*seen.lock().unwrap(), Some((5, false)));
    }

    #[test]
    fn streamed_bodies_are_complete_after_the_end_of_stream() {
        let (mut body, seen) = meter(frames(&[b"data: 1\n\n", b"data: 2\n\n"]));

        while drive(&mut body, Waker::noop()).is_some() {}
        assert!(body.is_finished());
        assert_eq!(body.bytes(), 18);

        drop(body);
        assert_eq!(*seen.lock().unwrap(), Some((18, false)));
    }

    #[test]
    fn aborted_streams_are_reported_as_aborted() {
        let (mut body, seen) = meter(frames(&[b"data: 1\n\n", b"data: 2\n\n"]));

        // One frame went out, then the client disappeared before the second one.
        assert_eq!(drive(&mut body, Waker::noop()), Some(9));

        drop(body);
        assert_eq!(*seen.lock().unwrap(), Some((9, true)));
    }

    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn finish_emits_every_documented_field() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || SharedBuffer(Arc::clone(&writer)))
            .with_ansi(false)
            .with_target(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let mut log = RequestLog::new(
            "trace-me-123".to_string(),
            Method::POST,
            "/v1/chat/completions".to_string(),
            Instant::now(),
        );
        log.caller = Some("frontend".to_string());
        log.model = Some("llama-3.1-8b-instruct".to_string());
        log.upstream = Some("primary".to_string());
        log.status = StatusCode::OK;
        log.streaming = true;
        log.request_bytes = 42;
        log.finish(128, false);

        let line = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        for expected in [
            "request_id=trace-me-123",
            "caller=frontend",
            "model=llama-3.1-8b-instruct",
            "upstream=primary",
            "status=200",
            "elapsed_ms=",
            "request_bytes=42",
            "response_bytes=128",
            "stream=true",
            "aborted=false",
            "method=POST",
            "path=/v1/chat/completions",
            "request completed",
        ] {
            assert!(line.contains(expected), "missing {expected} in: {line}");
        }
        assert!(
            line.contains(" INFO "),
            "successful requests log at INFO: {line}"
        );
    }

    #[test]
    fn failures_are_logged_at_warn_with_the_error() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || SharedBuffer(Arc::clone(&writer)))
            .with_ansi(false)
            .with_target(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let mut log = RequestLog::new(
            "id".to_string(),
            Method::POST,
            "/v1/chat/completions".to_string(),
            Instant::now(),
        );
        log.error = Some("unknown model 'ghost'".to_string());
        log.status = StatusCode::NOT_FOUND;
        log.finish(94, false);

        let line = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(line.contains(" WARN "), "failures log at WARN: {line}");
        assert!(line.contains("request failed"), "{line}");
        assert!(line.contains("error=unknown model 'ghost'"), "{line}");
    }
}
