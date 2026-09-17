use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A cluster node as seen by the local daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: String,
    pub address: String,
    /// Liveness: `alive` or `dead`. A left id is gone from `members()`.
    pub status: String,
    pub leader: bool,
    pub role: String,
}

/// A cluster or custom event from the Watch stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// `member.join`, `member.dead` (crash, still listed), `member.left`
    /// (`clusdr leave`, gone), `leader.changed`, `custom.<topic>`, `watch.sync`, …
    pub event_type: String,
    pub source: String,
    pub payload: Vec<u8>,
    pub timestamp: SystemTime,
    pub seq: u64,
}

/// A held exclusive lock. `token` is the fencing token.
#[derive(Debug)]
pub struct Lock {
    pub name: String,
    pub holder: String,
    pub token: u64,
    pub(crate) deadline: Mutex<Option<SystemTime>>,
}

impl Lock {
    pub fn deadline(&self) -> Option<SystemTime> {
        *self.deadline.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn set_deadline(&self, value: Option<SystemTime>) {
        *self.deadline.lock().unwrap_or_else(|e| e.into_inner()) = value;
    }
}

/// A held TTL grant. `token` is the fencing token.
#[derive(Debug)]
pub struct Lease {
    pub name: String,
    pub owner: String,
    pub token: u64,
    pub(crate) deadline: Mutex<Option<SystemTime>>,
    pub(crate) stop: tokio_util::sync::CancellationToken,
}

impl Lease {
    pub fn deadline(&self) -> Option<SystemTime> {
        *self.deadline.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn set_deadline(&self, value: Option<SystemTime>) {
        *self.deadline.lock().unwrap_or_else(|e| e.into_inner()) = value;
    }

    /// Stop background renewal; the grant then expires at its deadline.
    pub fn stop_renew(&self) {
        self.stop.cancel();
    }
}

pub(crate) fn from_ms(unix_ms: i64) -> Option<SystemTime> {
    if unix_ms <= 0 {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::from_millis(unix_ms as u64))
}

pub(crate) fn ttl_ms(ttl: Option<Duration>) -> i64 {
    match ttl {
        Some(d) if !d.is_zero() => d.as_millis().min(i64::MAX as u128) as i64,
        _ => 0,
    }
}

pub(crate) fn renew_interval(ttl: Option<Duration>, deadline: Option<SystemTime>) -> Duration {
    let mut d = ttl;
    if d.map(|x| x.is_zero()).unwrap_or(true) {
        if let Some(deadline) = deadline {
            d = deadline.duration_since(SystemTime::now()).ok();
        }
    }
    let d = d
        .filter(|x| !x.is_zero())
        .unwrap_or(Duration::from_secs(15));
    (d / 3).max(Duration::from_millis(50))
}
