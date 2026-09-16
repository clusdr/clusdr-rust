use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) const DEFAULT_ADDR: &str = "127.0.0.1:7947";
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const MAX_PAYLOAD: usize = 64 * 1024;
pub(crate) const CA_FILE: &str = "ca.crt";
pub(crate) const CERT_FILE: &str = "node.crt";
pub(crate) const KEY_FILE: &str = "node.key";
pub(crate) const MAX_WATCH_TOPIC: usize = 128;

/// Connect options for [`crate::local`] and [`crate::dial`].
#[derive(Clone, Debug)]
pub struct Options {
    pub(crate) insecure: bool,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) holder: String,
    pub(crate) request_timeout: Duration,
    pub(crate) ready_timeout: Duration,
    pub(crate) server_name: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            insecure: false,
            data_dir: None,
            holder: String::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            ready_timeout: DEFAULT_READY_TIMEOUT,
            server_name: String::new(),
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plaintext. Also implied when `CLUSDR_TLS=disabled` and `data_dir` is empty.
    pub fn insecure(mut self, value: bool) -> Self {
        self.insecure = value;
        self
    }

    /// Directory with `ca.crt`, `node.crt`, and `node.key`.
    pub fn data_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.data_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Lock/lease identity. Empty → `sdk-<hex>` for this connection.
    pub fn holder(mut self, id: impl Into<String>) -> Self {
        self.holder = id.into().trim().to_string();
        self
    }

    /// Used when a call does not pass its own timeout (default 10s).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        if !timeout.is_zero() {
            self.request_timeout = timeout;
        }
        self
    }

    /// Health wait on connect (default 10s). Zero skips the wait.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// TLS server name (peer node id). Else `CLUSDR_TLS_SERVER_NAME`, else CN of `node.crt`.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = name.into().trim().to_string();
        self
    }
}

/// Watch filters. Empty is the full bus.
#[derive(Clone, Debug, Default)]
pub struct WatchFilter {
    pub(crate) topics: Vec<String>,
    pub(crate) event_types: Vec<String>,
}

impl WatchFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict to `custom.<topic>` for these keys (with or without a `custom.` prefix).
    pub fn topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics.extend(topics.into_iter().map(Into::into));
        self
    }

    /// Restrict to these full type strings (`member.join`, `custom.deployment`, …).
    pub fn event_types<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.event_types.extend(types.into_iter().map(Into::into));
        self
    }
}

pub(crate) fn env_addr() -> String {
    std::env::var("CLUSDR_GRPC_ADDR")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_ADDR.to_string())
}

pub(crate) fn env_insecure() -> bool {
    matches!(
        std::env::var("CLUSDR_TLS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "disabled" | "off" | "false" | "0"
    )
}

pub(crate) fn env_data_dir() -> PathBuf {
    std::env::var("CLUSDR_DATA_DIR")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs_home()
                .map(|h| h.join(".clusdr"))
                .unwrap_or_else(|| PathBuf::from(".clusdr"))
        })
}

pub(crate) fn env_server_name() -> String {
    std::env::var("CLUSDR_TLS_SERVER_NAME")
        .ok()
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}
