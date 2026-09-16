<h1 align="center">
  <a href="https://clusdr.io/docs/sdk/rust">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/clusdr/clusdr-rust/main/assets/rust-lettermark-dark.svg">
      <img src="https://raw.githubusercontent.com/clusdr/clusdr-rust/main/assets/rust-lettermark.svg" alt="clusdr Rust" width="160" height="164">
    </picture>
  </a>
</h1>

<p align="center">A runtime for the cluster. An SDK for the app.</p>

<p align="center">
  <a href="https://clusdr.io/docs/sdk/rust"><img src="https://img.shields.io/badge/docs-clusdr.io-0C0C10" alt="docs"></a>
  <a href="https://crates.io/crates/clusdr"><img src="https://img.shields.io/crates/v/clusdr" alt="crates.io"></a>
  <a href="https://github.com/clusdr/clusdr-rust/actions/workflows/ci.yml"><img src="https://github.com/clusdr/clusdr-rust/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/clusdr/clusdr-rust/blob/main/Cargo.toml"><img src="https://img.shields.io/badge/rust-1.82%2B-orange" alt="Rust 1.82+"></a>
  <a href="https://github.com/clusdr/clusdr-rust/blob/main/LICENSE"><img src="https://img.shields.io/github/license/clusdr/clusdr-rust" alt="License"></a>
</p>

Application SDK for the **local** Clusdr daemon. This crate does not join the cluster.

```text
your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
```

Not a database, queue, or Kubernetes. Wire API is **v1alpha1**. TLS is on by default. Async on Tokio.

## Install

```toml
[dependencies]
clusdr = "0.1.4"
```

Same version train as the daemon. A running daemon on this host is required:

```bash
curl -fsSL https://clusdr.io/install.sh | sh
```

Linux amd64/arm64, or [Docker Hub `durguto/clusdr`](https://hub.docker.com/r/durguto/clusdr) (GHCR: `ghcr.io/clusdr/clusdr`). Then `clusdr init` and `clusdr start --bootstrap`. Guide: [first member](https://clusdr.io/docs/guide/first-member).

## Use

`local` dials `CLUSDR_GRPC_ADDR` or `127.0.0.1:7947`, waits on Health, then you own the connection.

```rust
use clusdr::{local, Options, WatchFilter};
use std::time::Duration;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), clusdr::Error> {
    let c = local(Options::new()).await?;
    let members = c.members().await?;
    let _leader = c.leader().await?;
    let _ = members;

    c.publish("deployment", br#"{"sha":"abc"}"#).await?;

    let lk = c.lock("scheduler", Some(Duration::from_secs(15))).await?;
    let _ = lk.token;
    c.unlock("scheduler").await?;

    let mut events = c.watch(WatchFilter::default()).await?;
    while let Some(event) = events.next().await {
        let event = event?;
        // member.join, leader.changed, custom.deployment, …
        let _ = event.event_type;
        break;
    }

    c.close().await?;
    Ok(())
}
```

`dial` is for tests and operators. Apps use `local`.

One `Cluster` is cheap to clone (shared connection, same holder). `unlock` is process-wide for that name. Several Watch streams on one client are fine.

`ttl` is `Option<Duration>`. `None` or zero sends `ttl_ms = 0`; the daemon uses its default (15s). `close` stops Watch, unlocks, and revokes what this process still holds. Failures are `clusdr::Error`.

Full surface: [Rust SDK](https://clusdr.io/docs/sdk/rust). Runnable copies (Go, Python, Rust, TypeScript, and Java): [examples](https://github.com/clusdr/clusdr/tree/main/examples).

## TLS

On unless `Options::insecure(true)` or `CLUSDR_TLS=disabled`. PEMs (`ca.crt`, `node.crt`, `node.key`) come from `data_dir`, `CLUSDR_DATA_DIR`, or `~/.clusdr`. Missing files are an error; this client does not skip-verify.

## Not in this crate

- Join, promote, or configure the cluster (CLI)
- Talking to a remote node's Runtime API as the normal path — put a daemon on that host

## Links

- **Docs:** [clusdr.io](https://clusdr.io) · [Rust SDK](https://clusdr.io/docs/sdk/rust) · [from your app](https://clusdr.io/docs/guide/from-your-app)
- **Daemon:** [github.com/clusdr/clusdr](https://github.com/clusdr/clusdr)
- **This repo:** [github.com/clusdr/clusdr-rust](https://github.com/clusdr/clusdr-rust)

Apache-2.0. Contributor checkout: [CONTRIBUTING.md](CONTRIBUTING.md).
