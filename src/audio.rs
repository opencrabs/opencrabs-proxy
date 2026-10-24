//! Voice-only fallback upstream.
//!
//! A chat request that carries an `input_audio` part may be answered by a
//! second upstream when every audio model on the primary upstream fails. The
//! fallback is never consulted for a text request, so a chat prompt cannot
//! reach a provider that was configured for voice.
//!
//! Configured with `AUDIO_FALLBACK_BASE_URL`, `AUDIO_FALLBACK_API_KEY` and
//! `AUDIO_FALLBACK_MODELS` (comma list, tried in order). All three are
//! required together: a partial configuration stops the process at startup
//! rather than silently leaving voice without the fallback it was promised.
//!
//! # Which leg actually answers
//!
//! The chain is walked in the order the operator wrote it, and this module
//! does not reorder it. Two facts about the default chain are worth knowing
//! before reading a voice failure as a fallback problem:
//!
//! - The head of the default `AUDIO_MODELS` chain is refused by the upstream
//!   with `403`, because that id is not in the catalog the upstream publishes.
//!   Every voice request therefore spends one attempt before reaching the id
//!   that answers. That attempt is cheap and bounded by
//!   `ROTATION_ATTEMPT_TIMEOUT_SECS`, but it is why a voice request logs a
//!   refusal before its `200`.
//! - The id that does answer on the primary upstream serves both text and
//!   audio, and it is the only one in the default chain that does.
//!
//! So a voice request that fails on the primary leg has failed for a reason
//! other than the chain order, and moving ids around will not fix it. The
//! operator can drop the refused id from `AUDIO_MODELS`; this module will not
//! do it for them, because a silently reordered chain hides exactly the kind
//! of upstream change the log is meant to show.
//!
//! A chat model is never added here as a voice fallback. An id that answers
//! text is not evidence that it answers audio, and a wrong guess turns a
//! working voice note into a `400` from a model that was never able to read it.

/// The second upstream a voice request may use, and the models it serves.
#[derive(Clone, Debug)]
pub struct AudioFallback {
    base_url: String,
    key: String,
    /// Tried in order, after every audio id on the primary upstream failed.
    pub models: Vec<String>,
}

impl AudioFallback {
    /// Reads the three variables. All unset means no fallback, which is a
    /// valid deployment. One or two set is a config error: the operator meant
    /// to configure it and would otherwise believe voice has a second leg.
    pub fn from_env() -> Option<Self> {
        Self::from_values(
            std::env::var("AUDIO_FALLBACK_BASE_URL").ok().as_deref(),
            std::env::var("AUDIO_FALLBACK_API_KEY").ok().as_deref(),
            std::env::var("AUDIO_FALLBACK_MODELS").ok().as_deref(),
        )
    }

    pub fn from_values(
        base_url: Option<&str>,
        key: Option<&str>,
        models: Option<&str>,
    ) -> Option<Self> {
        fn clean(v: Option<&str>) -> Option<&str> {
            v.map(str::trim).filter(|s| !s.is_empty())
        }
        let (base_url, key, models) = (clean(base_url), clean(key), clean(models));
        let present = [base_url.is_some(), key.is_some(), models.is_some()]
            .iter()
            .filter(|set| **set)
            .count();
        if present == 0 {
            return None;
        }
        assert_eq!(
            present, 3,
            "AUDIO_FALLBACK_BASE_URL, AUDIO_FALLBACK_API_KEY and AUDIO_FALLBACK_MODELS \
             must be set together"
        );
        let models: Vec<String> = models
            .unwrap()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        assert!(!models.is_empty(), "AUDIO_FALLBACK_MODELS lists no model");
        Some(Self {
            // A base URL may be given with or without the `/v1` suffix, since
            // providers document it both ways. The suffix is stripped here and
            // added back by `chat_url`, so neither form produces `/v1/v1/...`.
            base_url: base_url
                .unwrap()
                .trim_end_matches('/')
                .trim_end_matches("/v1")
                .trim_end_matches('/')
                .to_string(),
            key: key.unwrap().to_string(),
            models,
        })
    }

    /// The chat endpoint on the fallback upstream. Audio rides the chat route
    /// as an `input_audio` part, so there is no separate audio path.
    pub fn chat_url(&self) -> String {
        format!("{}/v1/chat/completions", self.base_url)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// For the startup log: which models this leg would serve.
    pub fn describe(&self) -> String {
        self.models.join(",")
    }
}

/// Rewrite every `input_audio` part so `data` holds a data URI.
///
/// A caller sends bare base64, which is the shape the OpenAI API documents.
/// Some upstreams read that field as a URL or a data URI instead, and answer a
/// bare base64 string with `InvalidParameter: The provided URL does not appear
/// to be valid`. ModelScope does exactly that. A value that is already a data
/// URI or an `http(s)` URL is left alone, the rest of the body is untouched,
/// and the return value counts how many parts were rewritten.
pub fn audio_parts_as_data_uris(body: &mut serde_json::Value) -> usize {
    let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) else {
        return 0;
    };
    let mut converted = 0;
    for message in messages.iter_mut() {
        let Some(parts) = message.get_mut("content").and_then(|c| c.as_array_mut()) else {
            continue;
        };
        for part in parts.iter_mut() {
            let Some(audio) = part.get_mut("input_audio").and_then(|a| a.as_object_mut()) else {
                continue;
            };
            let is_reference = audio
                .get("data")
                .and_then(|d| d.as_str())
                .map(|d| {
                    d.starts_with("data:") || d.starts_with("http://") || d.starts_with("https://")
                })
                .unwrap_or(true);
            if is_reference {
                continue;
            }
            let data = audio
                .get("data")
                .and_then(|d| d.as_str())
                .unwrap_or_default()
                .to_string();
            let format = audio
                .get("format")
                .and_then(|f| f.as_str())
                .unwrap_or("wav");
            let uri = format!("data:audio/{format};base64,{data}");
            audio.insert("data".to_string(), serde_json::Value::String(uri));
            converted += 1;
        }
    }
    converted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_is_no_fallback() {
        assert!(AudioFallback::from_values(None, None, None).is_none());
        assert!(AudioFallback::from_values(Some("  "), Some(""), None).is_none());
    }

    #[test]
    fn reads_all_three_and_trims() {
        let f = AudioFallback::from_values(
            Some("https://api-inference.modelscope.ai/v1/"),
            Some(" secret "),
            Some(" a/one , b/two ,"),
        )
        .unwrap();
        assert_eq!(
            f.chat_url(),
            "https://api-inference.modelscope.ai/v1/chat/completions"
        );
        assert_eq!(f.key(), "secret");
        assert_eq!(f.models, vec!["a/one", "b/two"]);
        assert_eq!(f.describe(), "a/one,b/two");
    }

    /// Providers document the base URL both with and without `/v1`. Both
    /// forms must reach the same endpoint, or a correct config 404s.
    #[test]
    fn base_url_is_accepted_with_or_without_the_api_suffix() {
        for base in [
            "https://x.example",
            "https://x.example/",
            "https://x.example/v1",
        ] {
            let f = AudioFallback::from_values(Some(base), Some("k"), Some("a/one")).unwrap();
            assert_eq!(
                f.chat_url(),
                "https://x.example/v1/chat/completions",
                "base `{base}`"
            );
        }
    }

    #[test]
    #[should_panic(expected = "must be set together")]
    fn partial_configuration_is_an_error() {
        AudioFallback::from_values(Some("https://x.example"), None, Some("a/one"));
    }

    #[test]
    #[should_panic(expected = "lists no model")]
    fn empty_model_list_is_an_error() {
        AudioFallback::from_values(Some("https://x.example"), Some("k"), Some(" , "));
    }

    fn audio_body(data: &str, format: &str) -> serde_json::Value {
        serde_json::json!({
            "model": "a/one",
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": [
                    {"type": "text", "text": "what do you hear"},
                    {"type": "input_audio", "input_audio": {"data": data, "format": format}}
                ]}
            ]
        })
    }

    /// ModelScope reads the audio field as a URL or data URI and refuses bare
    /// base64 with `InvalidParameter`. The fallback leg must hand it the form
    /// it accepts, or every voice request that reaches the second upstream
    /// comes back 400 and the caller sees the primary's error instead.
    #[test]
    fn bare_base64_becomes_a_data_uri() {
        let mut body = audio_body("T2dnUw==", "ogg");
        assert_eq!(audio_parts_as_data_uris(&mut body), 1);
        assert_eq!(
            body["messages"][1]["content"][1]["input_audio"]["data"],
            "data:audio/ogg;base64,T2dnUw=="
        );
        // Nothing else moved: the text part and the system message stay put.
        assert_eq!(body["messages"][0]["content"], "be brief");
        assert_eq!(
            body["messages"][1]["content"][0],
            serde_json::json!({"type": "text", "text": "what do you hear"})
        );
    }

    /// A part that already carries a data URI or a URL is left exactly as it
    /// is, so a caller that speaks the upstream's own dialect is not mangled,
    /// and a second pass over the same body changes nothing.
    #[test]
    fn a_reference_is_left_alone() {
        for data in [
            "data:audio/wav;base64,T2dnUw==",
            "https://bucket.example/note.wav",
            "http://bucket.example/note.wav",
        ] {
            let mut body = audio_body(data, "wav");
            assert_eq!(audio_parts_as_data_uris(&mut body), 0, "data `{data}`");
            assert_eq!(
                body["messages"][1]["content"][1]["input_audio"]["data"],
                data
            );
            assert_eq!(audio_parts_as_data_uris(&mut body), 0);
        }
    }

    #[test]
    fn a_body_without_audio_is_untouched() {
        let mut body = serde_json::json!({
            "model": "a/one",
            "messages": [{"role": "user", "content": "plain text"}]
        });
        assert_eq!(audio_parts_as_data_uris(&mut body), 0);
        assert_eq!(body["messages"][0]["content"], "plain text");

        let mut no_messages = serde_json::json!({"model": "a/one"});
        assert_eq!(audio_parts_as_data_uris(&mut no_messages), 0);
    }

    /// Two audio parts in one request are both rewritten, and the format is
    /// taken from each part rather than assumed.
    #[test]
    fn every_audio_part_is_rewritten_with_its_own_format() {
        let mut body = serde_json::json!({
            "model": "a/one",
            "messages": [{"role": "user", "content": [
                {"type": "input_audio", "input_audio": {"data": "AAAA", "format": "mp3"}},
                {"type": "text", "text": "and this one"},
                {"type": "input_audio", "input_audio": {"data": "BBBB", "format": "wav"}},
                {"type": "input_audio", "input_audio": {"data": "CCCC"}}
            ]}]
        });
        assert_eq!(audio_parts_as_data_uris(&mut body), 3);
        let parts = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(
            parts[0]["input_audio"]["data"],
            "data:audio/mp3;base64,AAAA"
        );
        assert_eq!(
            parts[2]["input_audio"]["data"],
            "data:audio/wav;base64,BBBB"
        );
        assert_eq!(
            parts[3]["input_audio"]["data"], "data:audio/wav;base64,CCCC",
            "a missing format falls back to wav"
        );
    }
}
