//! In-process gRPC daemon used by SDK tests.

#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use clusdr::v1alpha1::event_service_server::{EventService, EventServiceServer};
use clusdr::v1alpha1::health_service_server::{HealthService, HealthServiceServer};
use clusdr::v1alpha1::lease_service_server::{LeaseService, LeaseServiceServer};
use clusdr::v1alpha1::lock_service_server::{LockService, LockServiceServer};
use clusdr::v1alpha1::membership_service_server::{MembershipService, MembershipServiceServer};
use clusdr::v1alpha1::watch_service_server::{WatchService, WatchServiceServer};
use clusdr::v1alpha1::{
    GetLeaderRequest, GetLeaderResponse, GrantLeaseRequest, GrantLeaseResponse, HealthRequest,
    HealthResponse, ListMembersRequest, ListMembersResponse, LockRequest, LockResponse, Member,
    PublishEventRequest, PublishEventResponse, RenewLeaseRequest, RenewLeaseResponse,
    RenewLockRequest, RenewLockResponse, RevokeLeaseRequest, RevokeLeaseResponse, UnlockRequest,
    UnlockResponse, WatchRequest, WatchResponse,
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct Grant {
    pub name: String,
    pub holder: String,
    pub token: u64,
    pub deadline: Instant,
    pub ttl: Duration,
}

#[derive(Default)]
pub struct CoordTable {
    locks: Mutex<HashMap<String, Grant>>,
    leases: Mutex<HashMap<String, Grant>>,
    next_lock: AtomicU64,
    next_lease: AtomicU64,
    pub notify: Notify,
}

impl CoordTable {
    async fn expire_locks(&self) {
        let mut table = self.locks.lock().await;
        let now = Instant::now();
        table.retain(|_, g| g.deadline > now);
    }

    async fn expire_leases(&self) {
        let mut table = self.leases.lock().await;
        let now = Instant::now();
        table.retain(|_, g| g.deadline > now);
    }

    pub async fn acquire_lock(&self, name: &str, holder: &str, ttl: Duration) -> (Grant, bool) {
        self.expire_locks().await;
        let mut table = self.locks.lock().await;
        if let Some(rec) = table.get_mut(name) {
            if rec.holder == holder {
                rec.deadline = Instant::now() + ttl;
                rec.ttl = ttl;
                self.notify.notify_waiters();
                return (rec.clone(), true);
            }
            return (rec.clone(), false);
        }
        let token = self.next_lock.fetch_add(1, Ordering::SeqCst) + 1;
        let rec = Grant {
            name: name.to_string(),
            holder: holder.to_string(),
            token,
            deadline: Instant::now() + ttl,
            ttl,
        };
        table.insert(name.to_string(), rec.clone());
        self.notify.notify_waiters();
        (rec, true)
    }

    pub async fn release_lock(&self, name: &str, holder: &str, token: u64) -> Result<(), Status> {
        let mut table = self.locks.lock().await;
        match table.get(name) {
            Some(rec) if rec.holder == holder && rec.token == token => {
                table.remove(name);
                self.notify.notify_waiters();
                Ok(())
            }
            _ => Err(Status::failed_precondition("fencing token mismatch")),
        }
    }

    pub async fn renew_lock(
        &self,
        name: &str,
        holder: &str,
        token: u64,
        ttl: Duration,
    ) -> Result<Grant, Status> {
        let mut table = self.locks.lock().await;
        match table.get_mut(name) {
            Some(rec) if rec.holder == holder && rec.token == token => {
                rec.deadline = Instant::now() + ttl;
                rec.ttl = ttl;
                Ok(rec.clone())
            }
            _ => Err(Status::failed_precondition("fencing token mismatch")),
        }
    }

    pub async fn grant_lease(&self, name: &str, owner: &str, ttl: Duration) -> (Grant, bool) {
        self.expire_leases().await;
        let mut table = self.leases.lock().await;
        if let Some(rec) = table.get_mut(name) {
            if rec.holder == owner {
                rec.deadline = Instant::now() + ttl;
                rec.ttl = ttl;
                return (rec.clone(), true);
            }
            return (rec.clone(), false);
        }
        let token = self.next_lease.fetch_add(1, Ordering::SeqCst) + 1;
        let rec = Grant {
            name: name.to_string(),
            holder: owner.to_string(),
            token,
            deadline: Instant::now() + ttl,
            ttl,
        };
        table.insert(name.to_string(), rec.clone());
        (rec, true)
    }

    pub async fn revoke_lease(&self, name: &str, owner: &str, token: u64) -> Result<(), Status> {
        let mut table = self.leases.lock().await;
        match table.get(name) {
            Some(rec) if rec.holder == owner && rec.token == token => {
                table.remove(name);
                Ok(())
            }
            _ => Err(Status::failed_precondition("fencing token mismatch")),
        }
    }

    pub async fn renew_lease(
        &self,
        name: &str,
        owner: &str,
        token: u64,
        ttl: Duration,
    ) -> Result<Grant, Status> {
        let mut table = self.leases.lock().await;
        match table.get_mut(name) {
            Some(rec) if rec.holder == owner && rec.token == token => {
                rec.deadline = Instant::now() + ttl;
                rec.ttl = ttl;
                Ok(rec.clone())
            }
            _ => Err(Status::failed_precondition("fencing token mismatch")),
        }
    }

    pub async fn lock_ttl(&self, name: &str) -> Duration {
        self.locks
            .lock()
            .await
            .get(name)
            .map(|g| g.ttl)
            .unwrap_or(Duration::from_secs(15))
    }

    pub async fn lease_ttl(&self, name: &str) -> Duration {
        self.leases
            .lock()
            .await
            .get(name)
            .map(|g| g.ttl)
            .unwrap_or(Duration::from_secs(15))
    }
}

pub struct FakeState {
    pub members: Vec<Member>,
    pub events: broadcast::Sender<WatchResponse>,
    pub published: Mutex<Vec<PublishEventRequest>>,
    pub coord: Arc<CoordTable>,
}

impl FakeState {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            members: vec![Member {
                id: "node-a".into(),
                address: "127.0.0.1:1".into(),
                status: "alive".into(),
                leader: true,
                role: String::new(),
            }],
            events,
            published: Mutex::new(Vec::new()),
            coord: Arc::new(CoordTable::default()),
        }
    }
}

pub struct Health;

#[tonic::async_trait]
impl HealthService for Health {
    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            node_id: "node-a".into(),
            cluster_id: "c1".into(),
            role: "leader".into(),
            healthy: true,
        }))
    }
}

pub struct Membership {
    pub state: Arc<FakeState>,
}

#[tonic::async_trait]
impl MembershipService for Membership {
    async fn list_members(
        &self,
        _: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        Ok(Response::new(ListMembersResponse {
            members: self.state.members.clone(),
        }))
    }

    async fn get_leader(
        &self,
        _: Request<GetLeaderRequest>,
    ) -> Result<Response<GetLeaderResponse>, Status> {
        for m in &self.state.members {
            if m.leader {
                return Ok(Response::new(GetLeaderResponse {
                    leader_id: m.id.clone(),
                    address: m.address.clone(),
                }));
            }
        }
        Ok(Response::new(GetLeaderResponse::default()))
    }
}

pub struct Events {
    pub state: Arc<FakeState>,
}

#[tonic::async_trait]
impl EventService for Events {
    async fn publish_event(
        &self,
        request: Request<PublishEventRequest>,
    ) -> Result<Response<PublishEventResponse>, Status> {
        let req = request.into_inner();
        let mut published = self.state.published.lock().await;
        published.push(req.clone());
        let ev = WatchResponse {
            r#type: format!("custom.{}", req.topic),
            source: "node-a".into(),
            payload: req.payload.clone(),
            timestamp_unix_ms: 1,
            seq: published.len() as u64,
        };
        let _ = self.state.events.send(ev.clone());
        Ok(Response::new(PublishEventResponse {
            accepted: true,
            message: String::new(),
            event_id: String::new(),
            r#type: ev.r#type,
        }))
    }
}

pub struct WatchSvc {
    pub state: Arc<FakeState>,
}

#[tonic::async_trait]
impl WatchService for WatchSvc {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = request.into_inner();
        let topics: Vec<String> = req
            .topics
            .into_iter()
            .map(|t| t.strip_prefix("custom.").unwrap_or(&t).to_string())
            .collect();
        let types = req.event_types;
        let mut rx = self.state.events.subscribe();
        let snap = WatchResponse {
            r#type: "member.join".into(),
            source: "node-a".into(),
            payload: Vec::new(),
            timestamp_unix_ms: 1,
            seq: 0,
        };
        let stream = async_stream::stream! {
            if watch_match(&snap.r#type, &topics, &types) {
                yield Ok(snap);
            }
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        if watch_match(&ev.r#type, &topics, &types) {
                            yield Ok(ev);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

fn watch_match(event_type: &str, topics: &[String], types: &[String]) -> bool {
    if event_type == "watch.sync" || event_type == "watch.gap" {
        return true;
    }
    if !topics.is_empty()
        && (!event_type.starts_with("custom.") || !topics.iter().any(|t| event_type[7..] == *t))
    {
        return false;
    }
    if !types.is_empty() && !types.iter().any(|t| t == event_type) {
        return false;
    }
    true
}

fn ttl(ttl_ms: i64, reuse: Duration) -> Duration {
    if ttl_ms > 0 {
        Duration::from_millis(ttl_ms as u64)
    } else {
        reuse
    }
}

fn deadline_ms(rec: &Grant) -> i64 {
    let now = SystemTime::now();
    let remain = rec.deadline.saturating_duration_since(Instant::now());
    now.duration_since(UNIX_EPOCH)
        .map(|d| (d + remain).as_millis() as i64)
        .unwrap_or(0)
}

pub struct Locks {
    pub table: Arc<CoordTable>,
}

#[tonic::async_trait]
impl LockService for Locks {
    async fn lock(&self, request: Request<LockRequest>) -> Result<Response<LockResponse>, Status> {
        let req = request.into_inner();
        let ttl = ttl(req.ttl_ms, Duration::from_secs(15));
        loop {
            let (rec, ok) = self.table.acquire_lock(&req.name, &req.holder, ttl).await;
            if ok {
                let deadline_unix_ms = deadline_ms(&rec);
                return Ok(Response::new(LockResponse {
                    acquired: true,
                    message: String::new(),
                    fencing_token: rec.token,
                    holder: rec.holder,
                    deadline_unix_ms,
                }));
            }
            self.table.notify.notified().await;
        }
    }

    async fn try_lock(
        &self,
        request: Request<LockRequest>,
    ) -> Result<Response<LockResponse>, Status> {
        let req = request.into_inner();
        let (rec, ok) = self
            .table
            .acquire_lock(
                &req.name,
                &req.holder,
                ttl(req.ttl_ms, Duration::from_secs(15)),
            )
            .await;
        let deadline_unix_ms = deadline_ms(&rec);
        if !ok {
            return Ok(Response::new(LockResponse {
                acquired: false,
                message: "held".into(),
                fencing_token: rec.token,
                holder: rec.holder,
                deadline_unix_ms,
            }));
        }
        Ok(Response::new(LockResponse {
            acquired: true,
            message: String::new(),
            fencing_token: rec.token,
            holder: rec.holder,
            deadline_unix_ms,
        }))
    }

    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        let req = request.into_inner();
        self.table
            .release_lock(&req.name, &req.holder, req.fencing_token)
            .await?;
        Ok(Response::new(UnlockResponse {
            released: true,
            message: String::new(),
        }))
    }

    async fn renew(
        &self,
        request: Request<RenewLockRequest>,
    ) -> Result<Response<RenewLockResponse>, Status> {
        let req = request.into_inner();
        let reuse = self.table.lock_ttl(&req.name).await;
        let rec = self
            .table
            .renew_lock(
                &req.name,
                &req.holder,
                req.fencing_token,
                ttl(req.ttl_ms, reuse),
            )
            .await?;
        Ok(Response::new(RenewLockResponse {
            renewed: true,
            message: String::new(),
            fencing_token: req.fencing_token,
            deadline_unix_ms: deadline_ms(&rec),
        }))
    }

    async fn list_locks(
        &self,
        _: Request<clusdr::v1alpha1::ListLocksRequest>,
    ) -> Result<Response<clusdr::v1alpha1::ListLocksResponse>, Status> {
        Ok(Response::new(clusdr::v1alpha1::ListLocksResponse {
            locks: vec![],
        }))
    }
}

pub struct Leases {
    pub table: Arc<CoordTable>,
}

#[tonic::async_trait]
impl LeaseService for Leases {
    async fn grant(
        &self,
        request: Request<GrantLeaseRequest>,
    ) -> Result<Response<GrantLeaseResponse>, Status> {
        let req = request.into_inner();
        let (rec, ok) = self
            .table
            .grant_lease(
                &req.name,
                &req.owner,
                ttl(req.ttl_ms, Duration::from_secs(15)),
            )
            .await;
        let deadline_unix_ms = deadline_ms(&rec);
        if !ok {
            return Ok(Response::new(GrantLeaseResponse {
                granted: false,
                message: "held".into(),
                fencing_token: rec.token,
                owner: rec.holder,
                deadline_unix_ms,
            }));
        }
        Ok(Response::new(GrantLeaseResponse {
            granted: true,
            message: String::new(),
            fencing_token: rec.token,
            owner: rec.holder,
            deadline_unix_ms,
        }))
    }

    async fn renew(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<RenewLeaseResponse>, Status> {
        let req = request.into_inner();
        let reuse = self.table.lease_ttl(&req.name).await;
        let rec = self
            .table
            .renew_lease(
                &req.name,
                &req.owner,
                req.fencing_token,
                ttl(req.ttl_ms, reuse),
            )
            .await?;
        Ok(Response::new(RenewLeaseResponse {
            renewed: true,
            message: String::new(),
            fencing_token: req.fencing_token,
            deadline_unix_ms: deadline_ms(&rec),
        }))
    }

    async fn revoke(
        &self,
        request: Request<RevokeLeaseRequest>,
    ) -> Result<Response<RevokeLeaseResponse>, Status> {
        let req = request.into_inner();
        self.table
            .revoke_lease(&req.name, &req.owner, req.fencing_token)
            .await?;
        Ok(Response::new(RevokeLeaseResponse {
            revoked: true,
            message: String::new(),
        }))
    }

    async fn list_leases(
        &self,
        _: Request<clusdr::v1alpha1::ListLeasesRequest>,
    ) -> Result<Response<clusdr::v1alpha1::ListLeasesResponse>, Status> {
        Ok(Response::new(clusdr::v1alpha1::ListLeasesResponse {
            leases: vec![],
        }))
    }
}

pub struct FlakyHealth {
    pub failures: u32,
    pub calls: AtomicU64,
}

#[tonic::async_trait]
impl HealthService for FlakyHealth {
    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if n <= self.failures as u64 {
            return Err(Status::unavailable("wait"));
        }
        Health.health(Request::new(HealthRequest {})).await
    }
}

pub async fn start_fake_server() -> (String, Arc<FakeState>) {
    start_with_health(Health).await
}

pub async fn start_with_health<H>(health: H) -> (String, Arc<FakeState>)
where
    H: HealthService,
{
    let state = Arc::new(FakeState::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let incoming = TcpListenerStream::new(listener);
    let svc_state = state.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HealthServiceServer::new(health))
            .add_service(MembershipServiceServer::new(Membership {
                state: svc_state.clone(),
            }))
            .add_service(EventServiceServer::new(Events {
                state: svc_state.clone(),
            }))
            .add_service(WatchServiceServer::new(WatchSvc {
                state: svc_state.clone(),
            }))
            .add_service(LockServiceServer::new(Locks {
                table: svc_state.coord.clone(),
            }))
            .add_service(LeaseServiceServer::new(Leases {
                table: svc_state.coord.clone(),
            }))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    (addr.to_string(), state)
}
