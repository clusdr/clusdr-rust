use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};

use crate::coord::Coord;
use crate::error::{Error, Result};
use crate::options::{env_addr, env_insecure, Options, WatchFilter, MAX_PAYLOAD};
use crate::retry::{ready_transient, remaining, retry, retry_if, sleep_ctx};
use crate::tls::open_channel;
use crate::types::{from_ms, Event, Member};
use crate::v1alpha1::event_service_client::EventServiceClient;
use crate::v1alpha1::health_service_client::HealthServiceClient;
use crate::v1alpha1::lease_service_client::LeaseServiceClient;
use crate::v1alpha1::lock_service_client::LockServiceClient;
use crate::v1alpha1::membership_service_client::MembershipServiceClient;
use crate::v1alpha1::watch_service_client::WatchServiceClient;
use crate::v1alpha1::{
    GetLeaderRequest, HealthRequest, ListMembersRequest, PublishEventRequest, WatchRequest,
};
use crate::watch::normalize_watch_filter;

pub(crate) struct Inner {
    pub opts: Options,
    pub holder: String,
    pub mem: MembershipServiceClient<tonic::transport::Channel>,
    pub watch: WatchServiceClient<tonic::transport::Channel>,
    pub ev: EventServiceClient<tonic::transport::Channel>,
    pub health: HealthServiceClient<tonic::transport::Channel>,
    pub lock: LockServiceClient<tonic::transport::Channel>,
    pub lease: LeaseServiceClient<tonic::transport::Channel>,
    pub closed: AtomicBool,
    pub cancel: CancellationToken,
    pub coord: Mutex<Coord>,
}

/// Application view of the local Clusdr daemon.
///
/// Obtained from [`crate::local`] or [`crate::dial`]. Call [`Cluster::close`]
/// to unlock, revoke, cancel Watch, and drop the channel.
#[derive(Clone)]
pub struct Cluster {
    pub(crate) inner: Arc<Inner>,
}

/// Connect to the daemon on this host.
///
/// Address: `CLUSDR_GRPC_ADDR` or `127.0.0.1:7947`. Then waits on Health
/// (`ready_timeout`, default 10s). This is the application path; tests use
/// [`dial`].
pub async fn local(opts: Options) -> Result<Cluster> {
    connect(env_addr(), opts).await
}

/// Connect to `addr` (Runtime API host:port).
///
/// Tests and a second daemon on this host. Applications use [`local`].
/// Do not point this at a remote node's Runtime API as the normal path.
pub async fn dial(addr: impl AsRef<str>, opts: Options) -> Result<Cluster> {
    let addr = addr.as_ref().trim();
    if addr.is_empty() {
        return Err(Error::new("clusdr: empty dial address"));
    }
    connect(addr.to_string(), opts).await
}

async fn connect(addr: String, mut opts: Options) -> Result<Cluster> {
    if addr.trim().is_empty() {
        return Err(Error::new("clusdr: empty dial address"));
    }
    if !opts.insecure && env_insecure() && opts.data_dir.is_none() {
        opts.insecure = true;
    }
    let channel = open_channel(&opts, addr.trim()).await?;
    let holder = if opts.holder.is_empty() {
        new_holder_id()
    } else {
        opts.holder.clone()
    };
    let cluster = Cluster {
        inner: Arc::new(Inner {
            opts: opts.clone(),
            holder,
            mem: MembershipServiceClient::new(channel.clone()),
            watch: WatchServiceClient::new(channel.clone()),
            ev: EventServiceClient::new(channel.clone()),
            health: HealthServiceClient::new(channel.clone()),
            lock: LockServiceClient::new(channel.clone()),
            lease: LeaseServiceClient::new(channel),
            closed: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            coord: Mutex::new(Coord {
                held: Default::default(),
                leased: Default::default(),
            }),
        }),
    };
    if opts.ready_timeout > Duration::ZERO {
        let deadline = Instant::now() + opts.ready_timeout;
        let ready = retry_if(deadline, ready_transient, &mut || {
            let mut health = cluster.inner.health.clone();
            async move {
                let slice = deadline.saturating_duration_since(Instant::now());
                let timeout = slice.clamp(Duration::from_millis(50), Duration::from_millis(500));
                let mut req = Request::new(HealthRequest {});
                req.set_timeout(timeout);
                health.health(req).await.map(|r| r.into_inner())
            }
        })
        .await;
        if let Err(e) = ready {
            cluster.close().await.ok();
            return Err(Error::new(format!(
                "clusdr: daemon not ready at {addr}: {e}"
            )));
        }
    }
    Ok(cluster)
}

impl Cluster {
    pub async fn members(&self) -> Result<Vec<Member>> {
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let resp = retry(deadline, || {
            let mut mem = self.inner.mem.clone();
            async move {
                let mut req = Request::new(ListMembersRequest {});
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                mem.list_members(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: members: {e}")))?;
        Ok(resp
            .members
            .into_iter()
            .map(|m| Member {
                id: m.id,
                address: m.address,
                status: m.status,
                leader: m.leader,
                role: if m.role.is_empty() {
                    "voter".into()
                } else {
                    m.role
                },
            })
            .collect())
    }

    pub async fn leader(&self) -> Result<Member> {
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let resp = retry(deadline, || {
            let mut mem = self.inner.mem.clone();
            async move {
                let mut req = Request::new(GetLeaderRequest {});
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                mem.get_leader(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: leader: {e}")))?;
        Ok(Member {
            id: resp.leader_id,
            address: resp.address,
            status: "alive".into(),
            leader: true,
            role: "voter".into(),
        })
    }

    pub async fn publish(&self, topic: impl Into<String>, payload: impl AsRef<[u8]>) -> Result<()> {
        let topic = topic.into();
        let body = payload.as_ref().to_vec();
        if body.len() > MAX_PAYLOAD {
            return Err(Error::new(format!(
                "clusdr: publish payload exceeds {MAX_PAYLOAD} bytes"
            )));
        }
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let resp = retry(deadline, || {
            let mut ev = self.inner.ev.clone();
            let topic = topic.clone();
            let body = body.clone();
            async move {
                let mut req = Request::new(PublishEventRequest {
                    topic,
                    payload: body,
                    event_id: String::new(),
                    source: String::new(),
                    relay: false,
                });
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                ev.publish_event(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: publish: {e}")))?;
        if !resp.accepted {
            return Err(Error::new(format!(
                "clusdr: publish rejected: {}",
                resp.message
            )));
        }
        Ok(())
    }

    /// Stream events. Empty filter is the full bus.
    ///
    /// Reconnects with `last_seq` on drop. Dropping the stream or calling
    /// [`close`](Self::close) ends it. Several Watch streams on one client are
    /// fine; each has its own reconnect loop.
    pub async fn watch(&self, filter: WatchFilter) -> Result<EventStream> {
        let (topics, event_types) = normalize_watch_filter(filter)?;
        let (tx, rx) = mpsc::channel(64);
        let cluster = self.clone();
        tokio::spawn(async move {
            cluster.watch_loop(tx, topics, event_types).await;
        });
        Ok(EventStream {
            inner: ReceiverStream::new(rx),
        })
    }

    async fn watch_loop(
        &self,
        tx: mpsc::Sender<Result<Event>>,
        topics: Vec<String>,
        event_types: Vec<String>,
    ) {
        let mut last_seq = 0u64;
        let mut backoff = Duration::from_millis(50);
        loop {
            if self.inner.closed.load(Ordering::SeqCst) || self.inner.cancel.is_cancelled() {
                return;
            }
            let mut client = self.inner.watch.clone();
            let req = WatchRequest {
                last_seq,
                topics: topics.clone(),
                event_types: event_types.clone(),
            };
            let stream = match client.watch(Request::new(req)).await {
                Ok(s) => s.into_inner(),
                Err(status) => {
                    if self.inner.cancel.is_cancelled() || status.code() == Code::Cancelled {
                        return;
                    }
                    if status.code() == Code::InvalidArgument {
                        let _ = tx
                            .send(Err(Error::new(format!("clusdr: watch: {status}"))))
                            .await;
                        return;
                    }
                    if !sleep_ctx(&self.inner.cancel, backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                    continue;
                }
            };
            backoff = Duration::from_millis(50);
            tokio::pin!(stream);
            loop {
                tokio::select! {
                    _ = self.inner.cancel.cancelled() => return,
                    next = stream.next() => {
                        match next {
                            Some(Ok(resp)) => {
                                if resp.seq > last_seq {
                                    last_seq = resp.seq;
                                }
                                let ev = Event {
                                    event_type: resp.r#type,
                                    source: resp.source,
                                    payload: resp.payload,
                                    timestamp: from_ms(resp.timestamp_unix_ms)
                                        .unwrap_or(UNIX_EPOCH),
                                    seq: resp.seq,
                                };
                                if tx.send(Ok(ev)).await.is_err() {
                                    return;
                                }
                            }
                            Some(Err(status)) => {
                                if self.inner.cancel.is_cancelled() || status.code() == Code::Cancelled {
                                    return;
                                }
                                if status.code() == Code::InvalidArgument {
                                    let _ = tx.send(Err(Error::new(format!("clusdr: watch: {status}")))).await;
                                    return;
                                }
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }
            if !sleep_ctx(&self.inner.cancel, backoff).await {
                return;
            }
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.cancel.cancel();
        self.release_grants().await;
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.inner.cancel.cancel();
    }
}

/// Stream of Watch events. Implements [`tokio_stream::Stream`].
pub struct EventStream {
    inner: ReceiverStream<Result<Event>>,
}

impl tokio_stream::Stream for EventStream {
    type Item = Result<Event>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn new_holder_id() -> String {
    format!("sdk-{}", uuid::Uuid::new_v4().simple())
}
