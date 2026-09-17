use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tonic::Request;

use crate::error::{Error, Result};
use crate::retry::{remaining, retry};
use crate::types::{from_ms, renew_interval, ttl_ms, Lease, Lock};
use crate::v1alpha1::{
    GrantRequest, GrantResponse, LeaseServiceRenewRequest, LockRequest, LockServiceRenewRequest,
    RevokeRequest, TryLockRequest, UnlockRequest,
};
use crate::Cluster;

pub(crate) struct Coord {
    pub held: HashMap<String, HeldLock>,
    pub leased: HashMap<String, HeldLease>,
}

pub(crate) struct HeldLock {
    pub lock: Arc<Lock>,
    pub stop: CancellationToken,
}

pub(crate) struct HeldLease {
    pub lease: Arc<Lease>,
}

impl Cluster {
    pub async fn lock(&self, name: impl Into<String>, ttl: Option<Duration>) -> Result<Arc<Lock>> {
        let name = name.into();
        if let Some(existing) = self.held_lock(&name).await {
            return Ok(existing);
        }
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let holder = self.inner.holder.clone();
        let name_rpc = name.clone();
        let resp = retry(deadline, || {
            let mut client = self.inner.lock.clone();
            let holder = holder.clone();
            let name_rpc = name_rpc.clone();
            async move {
                let mut req = Request::new(LockRequest {
                    name: name_rpc,
                    holder,
                    ttl_ms: ttl_ms(ttl),
                });
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.lock(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: lock {name:?}: {e}")))?;
        if !resp.acquired {
            let msg = if resp.message.is_empty() {
                "not acquired"
            } else {
                resp.message.as_str()
            };
            return Err(Error::new(format!("clusdr: lock {name:?}: {msg}")));
        }
        self.adopt_lock(
            resp.holder,
            resp.fencing_token,
            resp.deadline_unix_ms,
            name,
            ttl,
        )
        .await
    }

    pub async fn try_lock(
        &self,
        name: impl Into<String>,
        ttl: Option<Duration>,
    ) -> Result<Option<Arc<Lock>>> {
        let name = name.into();
        if let Some(existing) = self.held_lock(&name).await {
            return Ok(Some(existing));
        }
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let holder = self.inner.holder.clone();
        let name_rpc = name.clone();
        let resp = retry(deadline, || {
            let mut client = self.inner.lock.clone();
            let holder = holder.clone();
            let name_rpc = name_rpc.clone();
            async move {
                let mut req = Request::new(TryLockRequest {
                    name: name_rpc,
                    holder,
                    ttl_ms: ttl_ms(ttl),
                });
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.try_lock(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: trylock {name:?}: {e}")))?;
        if !resp.acquired {
            return Ok(None);
        }
        Ok(Some(
            self.adopt_lock(
                resp.holder,
                resp.fencing_token,
                resp.deadline_unix_ms,
                name,
                ttl,
            )
            .await?,
        ))
    }

    pub async fn unlock(&self, name: impl AsRef<str>) -> Result<()> {
        let name = name.as_ref();
        let Some(held) = self.take_held_lock(name).await else {
            return Err(Error::new(format!(
                "clusdr: lock {name:?} is not held by this client"
            )));
        };
        self.release_lock(&held).await
    }

    pub async fn lease(
        &self,
        name: impl Into<String>,
        ttl: Option<Duration>,
    ) -> Result<Arc<Lease>> {
        let name = name.into();
        if let Some(existing) = self.held_lease(&name).await {
            return Ok(existing);
        }
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let owner = self.inner.holder.clone();
        let name_rpc = name.clone();
        let resp = retry(deadline, || {
            let mut client = self.inner.lease.clone();
            let owner = owner.clone();
            let name_rpc = name_rpc.clone();
            async move {
                let mut req = Request::new(GrantRequest {
                    name: name_rpc,
                    owner,
                    ttl_ms: ttl_ms(ttl),
                });
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.grant(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: lease {name:?}: {e}")))?;
        if !resp.granted {
            let msg = if resp.message.is_empty() {
                "not granted"
            } else {
                resp.message.as_str()
            };
            if !resp.owner.is_empty() {
                return Err(Error::new(format!(
                    "clusdr: lease {name:?}: {msg} (owner {})",
                    resp.owner
                )));
            }
            return Err(Error::new(format!("clusdr: lease {name:?}: {msg}")));
        }
        self.adopt_lease(resp, name, ttl).await
    }

    pub async fn renew(&self, name: impl AsRef<str>) -> Result<()> {
        let name = name.as_ref();
        let Some(ls) = self.held_lease(name).await else {
            return Err(Error::new(format!(
                "clusdr: lease {name:?} is not held by this client"
            )));
        };
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let req_body = LeaseServiceRenewRequest {
            name: ls.name.clone(),
            owner: ls.owner.clone(),
            fencing_token: ls.token,
            ttl_ms: 0,
        };
        let resp = retry(deadline, || {
            let mut client = self.inner.lease.clone();
            let req_body = req_body.clone();
            async move {
                let mut req = Request::new(req_body);
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.renew(req).await.map(|r| r.into_inner())
            }
        })
        .await
        .map_err(|e| Error::new(format!("clusdr: renew {name:?}: {e}")))?;
        if !resp.renewed {
            return Err(Error::new(format!(
                "clusdr: renew {name:?}: {}",
                resp.message
            )));
        }
        if resp.deadline_unix_ms != 0 {
            ls.set_deadline(from_ms(resp.deadline_unix_ms));
        }
        Ok(())
    }

    pub async fn revoke(&self, name: impl AsRef<str>) -> Result<()> {
        let name = name.as_ref();
        let Some(held) = self.take_held_lease(name).await else {
            return Err(Error::new(format!(
                "clusdr: lease {name:?} is not held by this client"
            )));
        };
        self.drop_lease(&held).await
    }

    async fn held_lock(&self, name: &str) -> Option<Arc<Lock>> {
        self.inner
            .coord
            .lock()
            .await
            .held
            .get(name)
            .map(|h| h.lock.clone())
    }

    async fn held_lease(&self, name: &str) -> Option<Arc<Lease>> {
        self.inner
            .coord
            .lock()
            .await
            .leased
            .get(name)
            .map(|h| h.lease.clone())
    }

    async fn take_held_lock(&self, name: &str) -> Option<HeldLock> {
        self.inner.coord.lock().await.held.remove(name)
    }

    async fn take_held_lease(&self, name: &str) -> Option<HeldLease> {
        self.inner.coord.lock().await.leased.remove(name)
    }

    async fn adopt_lock(
        &self,
        holder: String,
        token: u64,
        deadline_unix_ms: i64,
        name: String,
        ttl: Option<Duration>,
    ) -> Result<Arc<Lock>> {
        let holder = if holder.is_empty() {
            self.inner.holder.clone()
        } else {
            holder
        };
        let lock = Arc::new(Lock {
            name: name.clone(),
            holder,
            token,
            deadline: std::sync::Mutex::new(from_ms(deadline_unix_ms)),
        });
        let stop = CancellationToken::new();
        {
            let mut coord = self.inner.coord.lock().await;
            if let Some(existing) = coord.held.get(&name) {
                if existing.lock.token == lock.token {
                    return Ok(existing.lock.clone());
                }
            }
            coord.held.insert(
                name,
                HeldLock {
                    lock: lock.clone(),
                    stop: stop.clone(),
                },
            );
        }
        self.spawn_lock_renew(lock.clone(), stop, ttl);
        Ok(lock)
    }

    async fn adopt_lease(
        &self,
        resp: GrantResponse,
        name: String,
        ttl: Option<Duration>,
    ) -> Result<Arc<Lease>> {
        let owner = if resp.owner.is_empty() {
            self.inner.holder.clone()
        } else {
            resp.owner
        };
        let stop = CancellationToken::new();
        let lease = Arc::new(Lease {
            name: name.clone(),
            owner,
            token: resp.fencing_token,
            deadline: std::sync::Mutex::new(from_ms(resp.deadline_unix_ms)),
            stop: stop.clone(),
        });
        {
            let mut coord = self.inner.coord.lock().await;
            if let Some(existing) = coord.leased.get(&name) {
                if existing.lease.token == lease.token {
                    return Ok(existing.lease.clone());
                }
            }
            coord.leased.insert(
                name,
                HeldLease {
                    lease: lease.clone(),
                },
            );
        }
        self.spawn_lease_renew(lease.clone(), ttl);
        Ok(lease)
    }

    async fn release_lock(&self, held: &HeldLock) -> Result<()> {
        held.stop.cancel();
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let body = UnlockRequest {
            name: held.lock.name.clone(),
            holder: held.lock.holder.clone(),
            fencing_token: held.lock.token,
        };
        let name = held.lock.name.clone();
        let token = held.lock.token;
        match retry(deadline, || {
            let mut client = self.inner.lock.clone();
            let body = body.clone();
            async move {
                let mut req = Request::new(body);
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.unlock(req).await.map(|r| r.into_inner())
            }
        })
        .await
        {
            Ok(resp) if !resp.released => Err(Error::new(format!(
                "clusdr: unlock {name:?}: {}",
                resp.message
            ))),
            Ok(_) => {
                self.forget_lock(&name, token).await;
                Ok(())
            }
            Err(e) => {
                if e.to_string().contains("FailedPrecondition")
                    || e.to_string().contains("failed precondition")
                {
                    self.forget_lock(&name, token).await;
                }
                Err(Error::new(format!("clusdr: unlock {name:?}: {e}")))
            }
        }
    }

    async fn drop_lease(&self, held: &HeldLease) -> Result<()> {
        held.lease.stop.cancel();
        let deadline = Instant::now() + self.inner.opts.request_timeout;
        let body = RevokeRequest {
            name: held.lease.name.clone(),
            owner: held.lease.owner.clone(),
            fencing_token: held.lease.token,
        };
        let name = held.lease.name.clone();
        let token = held.lease.token;
        match retry(deadline, || {
            let mut client = self.inner.lease.clone();
            let body = body.clone();
            async move {
                let mut req = Request::new(body);
                req.set_timeout(remaining(deadline).unwrap_or(Duration::from_millis(1)));
                client.revoke(req).await.map(|r| r.into_inner())
            }
        })
        .await
        {
            Ok(resp) if !resp.revoked => Err(Error::new(format!(
                "clusdr: revoke {name:?}: {}",
                resp.message
            ))),
            Ok(_) => {
                self.forget_lease(&name, token).await;
                Ok(())
            }
            Err(e) => {
                if e.to_string().contains("FailedPrecondition")
                    || e.to_string().contains("failed precondition")
                {
                    self.forget_lease(&name, token).await;
                }
                Err(Error::new(format!("clusdr: revoke {name:?}: {e}")))
            }
        }
    }

    async fn forget_lock(&self, name: &str, token: u64) {
        let mut coord = self.inner.coord.lock().await;
        if coord.held.get(name).is_some_and(|h| h.lock.token == token) {
            coord.held.remove(name);
        }
    }

    async fn forget_lease(&self, name: &str, token: u64) {
        let mut coord = self.inner.coord.lock().await;
        if coord
            .leased
            .get(name)
            .is_some_and(|h| h.lease.token == token)
        {
            coord.leased.remove(name);
        }
    }

    pub(crate) async fn release_grants(&self) {
        let (locks, leases) = {
            let mut coord = self.inner.coord.lock().await;
            let locks: Vec<HeldLock> = coord.held.drain().map(|(_, v)| v).collect();
            let leases: Vec<HeldLease> = coord.leased.drain().map(|(_, v)| v).collect();
            (locks, leases)
        };
        for held in locks {
            let _ = self.release_lock(&held).await;
        }
        for held in leases {
            let _ = self.drop_lease(&held).await;
        }
    }

    fn spawn_lock_renew(&self, lock: Arc<Lock>, stop: CancellationToken, ttl: Option<Duration>) {
        let mut client = self.inner.lock.clone();
        let timeout = self.inner.opts.request_timeout;
        let cluster_stop = self.inner.cancel.clone();
        let interval = renew_interval(ttl, lock.deadline());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = cluster_stop.cancelled() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                let mut req = Request::new(LockServiceRenewRequest {
                    name: lock.name.clone(),
                    holder: lock.holder.clone(),
                    fencing_token: lock.token,
                    ttl_ms: 0,
                });
                req.set_timeout(timeout);
                match client.renew(req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        if inner.deadline_unix_ms != 0 {
                            lock.set_deadline(from_ms(inner.deadline_unix_ms));
                        }
                    }
                    Err(status)
                        if status.code() == tonic::Code::FailedPrecondition
                            || status.code() == tonic::Code::Cancelled =>
                    {
                        return;
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    fn spawn_lease_renew(&self, lease: Arc<Lease>, ttl: Option<Duration>) {
        let mut client = self.inner.lease.clone();
        let timeout = self.inner.opts.request_timeout;
        let cluster_stop = self.inner.cancel.clone();
        let stop = lease.stop.clone();
        let interval = renew_interval(ttl, lease.deadline());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = cluster_stop.cancelled() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                let mut req = Request::new(LeaseServiceRenewRequest {
                    name: lease.name.clone(),
                    owner: lease.owner.clone(),
                    fencing_token: lease.token,
                    ttl_ms: 0,
                });
                req.set_timeout(timeout);
                match client.renew(req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        if inner.deadline_unix_ms != 0 {
                            lease.set_deadline(from_ms(inner.deadline_unix_ms));
                        }
                    }
                    Err(status)
                        if status.code() == tonic::Code::FailedPrecondition
                            || status.code() == tonic::Code::Cancelled =>
                    {
                        return;
                    }
                    Err(_) => continue,
                }
            }
        });
    }
}
