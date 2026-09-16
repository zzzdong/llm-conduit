//! Reading the thinking effort and the token usage out of an OpenAI compatible body.
//!
//! Both directions take the "stream past large fields" path: `messages` on the way up and
//! `choices` on the way down are skipped through `serde_json::de::IgnoredAny` without
//! allocating, so a log record never materializes a completion as a `serde_json::Value`.

use serde::Deserialize;
use serde::de::{IgnoredAny, MapAccess, Visitor};

use crate::observe::TokenUsage;

/// Keys that carry a thinking effort, in the order they take precedence.
///
/// `reasoning_effort` is OpenAI's own field, `thinking` is what several Chinese model
/// providers use (`off` / `low` / `high`, sometimes as an object with a `type`), and
/// `reasoning.level` appears in the more chatty variants of the same idea.
const EFFORT_KEYS: [&str; 4] = [
    "reasoning_effort",
    "thinking",
    "reasoning",
    "thinking_effort",
];

/// Read the thinking effort from a request body.
///
/// Returns `None` for an empty or non-object body and for any value that is not a string —
/// the gateway never guesses, so an exotic shape simply leaves the log field as `-`.
pub fn extract_effort(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    serde_json::from_slice::<EffortProbe>(body).ok()?.0
}

/// Probe that keeps only the first usable thinking effort field.
#[derive(Debug, Default)]
struct EffortProbe(Option<String>);

impl<'de> Deserialize<'de> for EffortProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EffortVisitor)
    }
}

struct EffortVisitor;

impl<'de> Visitor<'de> for EffortVisitor {
    type Value = EffortProbe;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut effort: Option<String> = None;
        // Rank of the key currently held, so a later, more authoritative field can replace it
        // regardless of the order the caller happened to serialize the body in.
        let mut rank = usize::MAX;
        while let Some(key) = map.next_key::<String>()? {
            if let Some(index) = EFFORT_KEYS.iter().position(|candidate| *candidate == key) {
                let value = map.next_value::<EffortCandidate>()?.0;
                if value.is_some() && index < rank {
                    effort = value;
                    rank = index;
                }
            } else {
                // `messages` is skipped without allocating, exactly like in `json_model`.
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(EffortProbe(effort))
    }
}

/// One effort value, accepted either as a plain string or as an object (`{"type": "high"}`).
#[derive(Debug, Default)]
struct EffortCandidate(Option<String>);

impl<'de> Deserialize<'de> for EffortCandidate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let effort = match value {
            serde_json::Value::String(text) => non_empty(text),
            serde_json::Value::Object(object) => object
                .get("type")
                .or_else(|| object.get("effort"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .and_then(non_empty),
            _ => None,
        };
        Ok(EffortCandidate(effort))
    }
}

fn non_empty(text: String) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else if trimmed.len() == text.len() {
        Some(text)
    } else {
        Some(trimmed.to_string())
    }
}

/// Pick the more authoritative of two efforts.
///
/// A request is read in one piece, so both values normally come from the same parse and the
/// precedence is already settled there; this keeps the rule identical for a request whose body
/// was observed more than once.
pub fn merge_effort(current: &mut Option<String>, seen: Option<String>) {
    let Some(value) = seen else { return };
    if current.is_none() {
        *current = Some(value);
    }
}

/// The request keys that carry token counters, as they appear in a request body.
const REQUEST_USAGE_KEYS: [&str; 4] = [
    "max_tokens",
    "max_completion_tokens",
    "max_output_tokens",
    "n",
];

/// Read the token budget and the choice count of a request body.
///
/// This is what a request can contribute to the token side of the log: the prompt length is
/// only known to the upstream, and the completion counters arrive with the response.
pub fn extract_request_usage(body: &[u8]) -> RequestUsage {
    if body.is_empty() {
        return RequestUsage::default();
    }
    serde_json::from_slice::<RequestUsageProbe>(body)
        .unwrap_or_default()
        .0
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RequestUsage {
    /// `max_tokens` / `max_completion_tokens` as configured by the caller, if any.
    pub max_tokens: Option<u64>,
    /// `n`, the number of completions requested.
    pub choices: Option<u64>,
}

#[derive(Debug, Default)]
struct RequestUsageProbe(RequestUsage);

impl<'de> Deserialize<'de> for RequestUsageProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RequestUsageVisitor)
    }
}

struct RequestUsageVisitor;

impl<'de> Visitor<'de> for RequestUsageVisitor {
    type Value = RequestUsageProbe;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut usage = RequestUsage::default();
        while let Some(key) = map.next_key::<String>()? {
            if REQUEST_USAGE_KEYS.contains(&key.as_str()) {
                let value = map.next_value::<serde_json::Value>()?;
                if key == "n" {
                    usage.choices = value.as_u64();
                } else if usage.max_tokens.is_none() {
                    usage.max_tokens = value.as_u64();
                }
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(RequestUsageProbe(usage))
    }
}

/// Scan response frames and merge what they say about the token usage.
///
/// The scanner is fed raw chunks of the response as they are forwarded, so it has to survive
/// chunk boundaries that cut through a JSON object. Two shapes have to work:
///
/// - a buffered JSON response, where the chunk is simply the body;
/// - server-sent events, where one chunk may hold zero, one or many `data: {...}` payloads
///   (an upstream is free to batch several events into a single write).
///
/// State is therefore kept per *event*: `pending` holds the not yet complete object of the
/// event currently being read, and the remaining bytes of the frame are scanned for further
/// events.
#[derive(Debug, Default)]
pub struct UsageScanner {
    /// Start of the object that is still incomplete, i.e. what the next frame continues.
    pending: Vec<u8>,
}

/// Marker that introduces an SSE payload.
const SSE_DATA: &[u8] = b"data:";

/// Longest object the scanner is willing to buffer before giving up. A usage object is a few
/// hundred bytes; anything far larger is not one, and this is the only per-request buffer
/// the logging path adds.
const MAX_PENDING: usize = 16 * 1024;

impl UsageScanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one response frame and merge the usage it contains.
    ///
    /// The candidate objects of the frame are offered to the buffer one at a time, and after
    /// each offer a parse is attempted:
    ///
    /// - success means the buffer held a complete object, so it is merged and freed;
    /// - "unexpected end of input" means the object is cut off, so the buffer is kept and the
    ///   next candidate (or the next frame) continues it;
    /// - any other error means the buffer can never become a usable object, so it is dropped
    ///   and the following candidates can still succeed.
    ///
    /// Because a usage object is the only thing parsed, every frame of a stream is walked
    /// rather than just its last event, and a frame that starts in the middle of an event is
    /// treated as its continuation.
    pub fn push(&mut self, frame: &[u8], usage: &mut Option<TokenUsage>) {
        for candidate in Candidates::new(frame, !self.pending.is_empty()) {
            self.pending.extend_from_slice(candidate);
            // All three outcomes are handled inside `consume`: it leaves the buffer holding
            // either nothing (parsed, rejected) or the still open object (cut off).
            self.consume(usage);
        }
    }

    /// Try to parse the buffered bytes as a response object, merging the usage when they are one.
    ///
    /// The buffer is left empty unless the object is cut off, in which case it is kept for the
    /// next candidate or frame.
    fn consume(&mut self, usage: &mut Option<TokenUsage>) {
        if self.pending.len() > MAX_PENDING {
            // Not a usage object: give up on it instead of holding the whole body.
            self.pending.clear();
            return;
        }

        match serde_json::from_slice::<UsageProbe>(&self.pending) {
            Ok(probe) => {
                merge_usage(usage, probe.0);
                self.pending.clear();
            }
            // The object continues in the next candidate or frame; keep buffering it.
            Err(error) if error.is_eof() => {}
            // Not usable as an object: drop it so the following bytes get a chance.
            Err(_) => self.pending.clear(),
        }
    }
}

/// Yields the byte sequences of one frame that could extend or start an object.
///
/// Two shapes have to work:
///
/// - a buffered JSON response, where the frame is the body and there is no marker at all, so
///   the whole frame is offered at once;
/// - server-sent events, where `data: ` precedes a payload and several payloads may be batched
///   into one frame, so each of them is offered separately.
///
/// When `open` is set an object is still incomplete and the bytes before the first marker are
/// part of it, which is what makes a frame starting in the middle of an event work. Payloads
/// are offered without their marker, their newline and their surrounding whitespace.
struct Candidates<'a> {
    rest: &'a [u8],
    /// Whether the leading part of the frame continues an object that is still open.
    open: bool,
}

impl<'a> Candidates<'a> {
    fn new(frame: &'a [u8], open: bool) -> Self {
        Self { rest: frame, open }
    }

    /// Consume `length` bytes off the front and return them.
    fn take(&mut self, length: usize) -> &'a [u8] {
        let (head, tail) = self.rest.split_at(length);
        self.rest = tail;
        head
    }
}

impl<'a> Iterator for Candidates<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }

        if self.open {
            self.open = false;
            // Everything up to the first marker continues the open object. With no marker the
            // object runs to the end of the frame.
            let head = match find(self.rest, SSE_DATA) {
                Some(start) => self.take(start),
                None => self.take(self.rest.len()),
            };
            if !head.is_empty() {
                return Some(head);
            }
        }

        match find(self.rest, SSE_DATA) {
            Some(start) => {
                // Drop the marker itself; it is not part of the payload.
                self.take(start + SSE_DATA.len());
                // The single space that conventionally follows the colon is not part of it
                // either, but any other whitespace is trimmed along with the line.
                if self.rest.first() == Some(&b' ') {
                    self.take(1);
                }
                let line = match find(self.rest, b"\n") {
                    Some(newline) => self.take(newline),
                    // The frame ends mid event, so the rest of the frame is the payload and it
                    // has to be continued by the next one.
                    None => self.take(self.rest.len()),
                };
                Some(trim(line))
            }
            // No markers at all: the frame is a plain body, offered as a single candidate.
            None => Some(trim(self.take(self.rest.len()))),
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    (0..=haystack.len() - needle.len())
        .find(|&index| &haystack[index..index + needle.len()] == needle)
}

/// ASCII whitespace and carriage returns, which SSE allows before the payload.
fn trim(mut data: &[u8]) -> &[u8] {
    while let Some(first) = data.first() {
        if first.is_ascii_whitespace() {
            data = &data[1..];
        } else {
            break;
        }
    }
    while let Some(last) = data.last() {
        if last.is_ascii_whitespace() {
            data = &data[..data.len() - 1];
        } else {
            break;
        }
    }
    data
}

/// Probe that keeps only the `usage` field of a response body or SSE chunk.
#[derive(Debug, Default)]
struct UsageProbe(Option<TokenUsage>);

impl<'de> Deserialize<'de> for UsageProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(UsageVisitor)
    }
}

struct UsageVisitor;

impl<'de> Visitor<'de> for UsageVisitor {
    type Value = UsageProbe;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut usage = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "usage" {
                // `"usage": null` is common before the final chunk of a stream.
                usage = map.next_value::<Option<UsageBody>>()?.map(TokenUsage::from);
            } else {
                // `choices` is skipped without allocating, exactly like `messages` above.
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(UsageProbe(usage))
    }
}

/// The `usage` object of an OpenAI compatible response.
///
/// Every field is optional because providers differ: the prompt and completion counters are
/// always there, while the cache and reasoning counters are extensions that many servers
/// simply omit.
#[derive(Debug, Default, Deserialize)]
struct UsageBody {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    #[serde(default)]
    prompt_tokens_details: TokenDetails,
    #[serde(default)]
    completion_tokens_details: TokenDetails,
}

#[derive(Debug, Default, Deserialize)]
struct TokenDetails {
    cached_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
}

impl From<UsageBody> for TokenUsage {
    fn from(body: UsageBody) -> Self {
        let total =
            body.total_tokens
                .or_else(|| match (body.prompt_tokens, body.completion_tokens) {
                    (Some(prompt), Some(completion)) => Some(prompt + completion),
                    _ => None,
                });
        Self {
            prompt_tokens: body.prompt_tokens,
            completion_tokens: body.completion_tokens,
            total_tokens: total,
            reasoning_tokens: body.completion_tokens_details.reasoning_tokens,
            cached_tokens: body.prompt_tokens_details.cached_tokens,
        }
    }
}

/// Keep the most complete usage seen: a stream reports the full counters in its last chunk,
/// so later values replace earlier partial ones.
fn merge_usage(current: &mut Option<TokenUsage>, seen: Option<TokenUsage>) {
    let Some(seen) = seen else { return };
    match current {
        Some(existing) if completeness(existing) > completeness(&seen) => {}
        _ => *current = Some(seen),
    }
}

fn completeness(usage: &TokenUsage) -> usize {
    [
        usage.prompt_tokens,
        usage.completion_tokens,
        usage.total_tokens,
        usage.reasoning_tokens,
        usage.cached_tokens,
    ]
    .iter()
    .filter(|field| field.is_some())
    .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAT_BODY: &str = r#"{
        "model": "llama-3.1-8b-instruct",
        "messages": [{"role": "user", "content": "Hello!"}],
        "reasoning_effort": "high"
    }"#;

    fn scan(chunks: &[&[u8]]) -> Option<TokenUsage> {
        let mut scanner = UsageScanner::new();
        let mut usage = None;
        for chunk in chunks {
            scanner.push(chunk, &mut usage);
        }
        usage
    }

    #[test]
    fn extracts_reasoning_effort() {
        assert_eq!(
            extract_effort(CHAT_BODY.as_bytes()).as_deref(),
            Some("high")
        );
        // Any of the known spellings, and the object form.
        assert_eq!(
            extract_effort(br#"{"reasoning":{"effort":"low"}}"#).as_deref(),
            Some("low")
        );
        assert_eq!(
            extract_effort(br#"{"thinking":{"type":"enabled"}}"#).as_deref(),
            Some("enabled")
        );
        assert_eq!(
            extract_effort(br#"{"messages":[],"thinking":"medium"}"#).as_deref(),
            Some("medium")
        );
    }

    #[test]
    fn reasoning_effort_wins_over_thinking() {
        let body = br#"{"thinking":"low","reasoning_effort":"high"}"#;
        assert_eq!(extract_effort(body).as_deref(), Some("high"));
        let body = br#"{"reasoning_effort":"high","thinking":"low"}"#;
        assert_eq!(extract_effort(body).as_deref(), Some("high"));
    }

    #[test]
    fn effort_is_absent_for_bodies_that_do_not_carry_one() {
        assert!(extract_effort(b"").is_none());
        assert!(extract_effort(b"not json").is_none());
        assert!(extract_effort(br#"{"model":"m","messages":[]}"#).is_none());
        assert!(extract_effort(br#"{"reasoning_effort":null}"#).is_none());
        assert!(extract_effort(br#"{"reasoning_effort":"  "}"#).is_none());
        assert!(extract_effort(br#"{"reasoning_effort":{}}"#).is_none());
    }

    #[test]
    fn effort_is_read_without_materializing_large_fields() {
        let big = "x".repeat(2_000_000);
        let body = format!(
            r#"{{"messages":[{{"role":"user","content":"{big}"}}],"reasoning_effort":"minimal"}}"#
        );
        assert_eq!(extract_effort(body.as_bytes()).as_deref(), Some("minimal"));
    }

    #[test]
    fn extracts_the_request_token_budget() {
        let usage = extract_request_usage(
            br#"{"model":"m","messages":[],"max_tokens":512,"n":3}"#.as_slice(),
        );
        assert_eq!(usage.max_tokens, Some(512));
        assert_eq!(usage.choices, Some(3));

        let usage = extract_request_usage(br#"{"max_completion_tokens":1024}"#.as_slice());
        assert_eq!(usage.max_tokens, Some(1024));
        assert_eq!(usage.choices, None);

        assert_eq!(extract_request_usage(b""), RequestUsage::default());
    }

    #[test]
    fn reads_usage_from_a_single_json_response() {
        let body = br#"{"id":"1","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":12,"completion_tokens":34,"total_tokens":46,"completion_tokens_details":{"reasoning_tokens":20},"prompt_tokens_details":{"cached_tokens":4}}}"#;
        let usage = scan(&[body]).unwrap();
        assert_eq!(usage.prompt_tokens, Some(12));
        assert_eq!(usage.completion_tokens, Some(34));
        assert_eq!(usage.total_tokens, Some(46));
        assert_eq!(usage.reasoning_tokens, Some(20));
        assert_eq!(usage.cached_tokens, Some(4));
    }

    #[test]
    fn reads_usage_from_streamed_chunks() {
        let usage = scan(&[
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
            b"data: {\"choices\":[{\"delta\":{}}]}\n\n",
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":34,\"total_tokens\":46}}\n\n",
            b"data: [DONE]\n\n",
        ]).unwrap();
        assert_eq!(usage.total_tokens, Some(46));
    }

    #[test]
    fn survives_chunk_boundaries_inside_the_usage_object() {
        let usage = scan(&[
            b"data: {\"choices\":[],\"usage\":{\"prompt",
            b"_tokens\":7,\"completion_tokens\":9}}\n\n",
        ])
        .unwrap();
        assert_eq!(usage.prompt_tokens, Some(7));
        assert_eq!(usage.completion_tokens, Some(9));
        assert_eq!(usage.total_tokens, Some(16));
    }

    #[test]
    fn a_body_split_at_the_object_boundary_is_still_read() {
        let usage = scan(&[b"{\"choices\":[]", b",\"usage\":{\"total_tokens\":5}}"]);
        assert_eq!(usage.unwrap().total_tokens, Some(5));
    }

    #[test]
    fn a_frame_without_usage_does_not_disturb_the_scanner() {
        let usage = scan(&[
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
            b"not json at all",
            b"{\"usage\":{\"total_tokens\":3}}",
        ]);
        assert_eq!(usage.unwrap().total_tokens, Some(3));
    }

    #[test]
    fn keeps_the_most_complete_usage_of_a_stream() {
        let usage = scan(&[
            b"data: {\"usage\":{\"prompt_tokens\":1}}\n\n",
            b"data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
        ]).unwrap();
        assert_eq!(usage.total_tokens, Some(3));
    }

    #[test]
    fn reads_usage_from_an_event_batched_into_one_frame() {
        // An upstream may write several SSE events in a single body chunk; the usage is in
        // the middle one, so a scanner that only looked at the last `data:` would miss it.
        let frame: &[u8] = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                              data: {\"choices\":[],\"usage\":{\"total_tokens\":46}}\n\n\
                              data: [DONE]\n\n";
        let usage = scan(&[frame]);
        assert_eq!(usage.unwrap().total_tokens, Some(46));
    }

    #[test]
    fn keeps_the_last_usage_of_a_batched_frame() {
        let frame: &[u8] = b"data: {\"usage\":{\"total_tokens\":1}}\n\n\
                              data: {\"usage\":{\"total_tokens\":2}}\n\n";
        assert_eq!(scan(&[frame]).unwrap().total_tokens, Some(2));
    }

    #[test]
    fn reads_usage_split_across_two_batched_frames() {
        // The first frame ends in the middle of an event, the second one starts with the rest
        // of it and only then opens a new event.
        let usage = scan(&[
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt",
            b"_tokens\":7}}\n\ndata: [DONE]\n\n",
        ]);
        assert_eq!(usage.unwrap().prompt_tokens, Some(7));
    }

    #[test]
    fn a_buffered_body_reaches_the_scanner_in_any_number_of_frames() {
        // No SSE markers at all: the body is one object and the frames are just TCP splits.
        let usage = scan(&[
            b"{\"id\":\"1\",\"cho",
            b"ices\":[],\"usa",
            b"ge\":{\"total_tokens\":8}}",
        ]);
        assert_eq!(usage.unwrap().total_tokens, Some(8));
    }

    #[test]
    fn an_event_whose_usage_arrives_only_in_the_next_frame_is_read() {
        // The whole `usage` object lands in the second frame, one byte into the event.
        let usage = scan(&[
            b"data: {\"choices\":[{\"delta\":{}}],\"usage\"",
            b":{\"total_tokens\":9}}\n\n",
        ]);
        assert_eq!(usage.unwrap().total_tokens, Some(9));
    }

    #[test]
    fn reads_usage_before_a_truncated_event() {
        // A trailing, incomplete event must not hide the usage of the event before it.
        let usage = scan(&[
            b"data: {\"usage\":{\"total_tokens\":5}}\n\ndata: {\"choices\":[{\"delta\"",
            b":{\"content\":\"tail\"}}]}\n\n",
        ]);
        assert_eq!(usage.unwrap().total_tokens, Some(5));
    }

    #[test]
    fn a_huge_non_usage_body_is_not_buffered() {
        // A `{` that never closes into a usable object must not grow the buffer forever.
        let mut scanner = UsageScanner::new();
        let mut usage = None;
        let filler = vec![b'x'; 1024 * 1024];
        for _ in 0..64 {
            scanner.push(b"{", &mut usage);
            scanner.push(&filler, &mut usage);
        }
        assert!(usage.is_none());
        assert!(scanner.pending.len() <= MAX_PENDING);
    }
}
