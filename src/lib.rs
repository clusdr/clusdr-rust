//! Application SDK for the local Clusdr daemon.
//!
//! The application is not a cluster member. It dials the daemon on this host
//! the same way a process talks to a local Docker engine. This crate never
//! dials other nodes and never joins Raft.
//!
//! ```text
//! your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
//! ```
//!
//! ```no_run
//! use clusdr::{local, Options};
//! use std::time::Duration;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), clusdr::Error> {
//! let c = local(Options::new()).await?;
//! let members = c.members().await?;
//! let _ = members;
//! c.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! TLS is on unless `Options::insecure(true)` or `CLUSDR_TLS=disabled`.
//! Missing PEMs are an error (no skip-verify fallback). Unary calls retry
//! `UNAVAILABLE` / `ABORTED` / `RESOURCE_EXHAUSTED`.

#![allow(clippy::result_large_err)]
#![doc(html_root_url = "https://docs.rs/clusdr/0.2.0")]

mod cluster;
mod coord;
mod error;
mod options;
mod retry;
mod tls;
mod types;
mod watch;

pub mod v1alpha1 {
    tonic::include_proto!("clusdr.v1alpha1");
}

pub use cluster::{dial, local, Cluster, EventStream};
pub use error::{Error, Result};
pub use options::{Options, WatchFilter};
pub use types::{Event, Lease, Lock, Member};
