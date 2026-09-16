use crate::error::{Error, Result};
use crate::options::{WatchFilter, MAX_WATCH_TOPIC};

pub(crate) fn normalize_watch_filter(filter: WatchFilter) -> Result<(Vec<String>, Vec<String>)> {
    Ok((
        normalize_topics(filter.topics)?,
        normalize_types(filter.event_types),
    ))
}

fn normalize_topics(in_topics: Vec<String>) -> Result<Vec<String>> {
    if in_topics.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for raw in in_topics {
        let mut t = raw.trim().to_string();
        if let Some(rest) = t.strip_prefix("custom.") {
            t = rest.to_string();
        }
        if t.is_empty() {
            continue;
        }
        if t.len() > MAX_WATCH_TOPIC || !t.chars().all(valid_topic_char) {
            return Err(Error::new(format!(
                "clusdr: watch topic {raw:?} is invalid"
            )));
        }
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    Ok(out)
}

fn normalize_types(in_types: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for raw in in_types {
        let t = raw.trim().to_string();
        if t.is_empty() {
            continue;
        }
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

fn valid_topic_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')
}
