//! Extraction and rewriting of the top-level `model` field of a JSON body.
//!
//! Extraction takes the "stream past large fields" path: arrays and objects such as
//! `messages` are skipped through `serde_json::de::IgnoredAny` without allocating, so the
//! only extra memory used is the `model` string itself.

use bytes::Bytes;
use serde::Deserialize;
use serde::de::{IgnoredAny, MapAccess, Visitor};

/// Extract the top-level `model` field from a request body.
///
/// Returns `None` for an empty body, a non-object body, or a non-string `model`.
pub fn extract_model(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    serde_json::from_slice::<ModelProbe>(body).ok()?.0
}

/// Probe that keeps only the top-level `model` field.
#[derive(Debug, Default)]
struct ModelProbe(Option<String>);

impl<'de> Deserialize<'de> for ModelProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ModelVisitor)
    }
}

struct ModelVisitor;

impl<'de> Visitor<'de> for ModelVisitor {
    type Value = ModelProbe;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut model = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "model" {
                if let serde_json::Value::String(value) = map.next_value()?
                    && !value.is_empty()
                {
                    model = Some(value);
                }
            } else {
                // Key point: never materialize large fields (messages / prompt / input ...)
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(ModelProbe(model))
    }
}

/// Rewrite the top-level `model` field of a request body when needed.
///
/// - Body is not a JSON object or has no `model` field: returns `None` (caller passes the body through).
/// - `model` already equals the target: returns `None` (avoids a pointless re-serialization).
/// - Otherwise the rewritten body is returned.
///
/// Note: re-serialization turns the JSON key order into lexicographic order (semantically identical).
/// It is the "only path that amplifies memory" called out in the design doc, so prefer consistent naming.
pub fn try_rewrite_model(body: &[u8], new_model: &str) -> Option<Bytes> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let object = value.as_object_mut()?;

    match object.get("model") {
        None => return None,
        Some(serde_json::Value::String(current)) if current == new_model => return None,
        Some(_) => {}
    }

    object.insert(
        "model".to_string(),
        serde_json::Value::String(new_model.to_string()),
    );
    serde_json::to_vec(&value).ok().map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAT_BODY: &str = r#"{
        "model": "llama-3.1-8b-instruct",
        "messages": [{"role": "user", "content": "Hello!"}, {"role": "assistant", "content": "Hi!"}],
        "temperature": 0.7,
        "stream": true
    }"#;

    #[test]
    fn extracts_model_from_standard_body() {
        assert_eq!(
            extract_model(CHAT_BODY.as_bytes()).as_deref(),
            Some("llama-3.1-8b-instruct")
        );
    }

    #[test]
    fn extraction_is_order_independent() {
        let body = br#"{"messages":[{"role":"user","content":"hi"}],"model":"mistral-7b-instruct","stream":false}"#;
        assert_eq!(extract_model(body).as_deref(), Some("mistral-7b-instruct"));
    }

    #[test]
    fn returns_none_for_unusable_bodies() {
        assert!(extract_model(b"").is_none());
        assert!(extract_model(b"not json").is_none());
        assert!(extract_model(b"[1,2,3]").is_none());
        assert!(extract_model(br#"{"messages":[]}"#).is_none());
        assert!(extract_model(br#"{"model":""}"#).is_none());
        assert!(extract_model(br#"{"model":123}"#).is_none());
    }

    #[test]
    fn skips_large_nested_fields() {
        let big = "x".repeat(2_000_000);
        let body = format!(r#"{{"messages":[{{"role":"user","content":"{big}"}}],"model":"m1"}}"#);
        assert_eq!(extract_model(body.as_bytes()).as_deref(), Some("m1"));
    }

    #[test]
    fn rewrites_model_when_different() {
        let rewritten =
            try_rewrite_model(CHAT_BODY.as_bytes(), "meta-llama/Llama-3.1-8B-Instruct").unwrap();
        let value: serde_json::Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(value["model"], "meta-llama/Llama-3.1-8B-Instruct");
        // Other fields must be preserved as-is
        assert_eq!(value["messages"][0]["content"], "Hello!");
        assert_eq!(value["stream"], true);
        assert_eq!(
            extract_model(&rewritten).as_deref(),
            Some("meta-llama/Llama-3.1-8B-Instruct")
        );
    }

    #[test]
    fn skips_rewrite_when_already_equal_or_unusable() {
        assert!(try_rewrite_model(CHAT_BODY.as_bytes(), "llama-3.1-8b-instruct").is_none());
        assert!(try_rewrite_model(b"not json", "m").is_none());
        assert!(try_rewrite_model(br#"{"messages":[]}"#, "m").is_none());
        assert!(try_rewrite_model(b"[]", "m").is_none());
    }
}
