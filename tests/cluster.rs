mod fake;

use std::time::Duration;

use clusdr::{dial, local, Options, WatchFilter};
use tokio_stream::StreamExt;

use fake::{start_fake_server, start_with_health, FlakyHealth};

fn insecure() -> Options {
    Options::new().insecure(true)
}

#[tokio::test]
async fn members_leader_watch_publish() {
    let (addr, state) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let members = c.members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, "node-a");
    assert!(members[0].leader);

    let leader = c.leader().await.unwrap();
    assert_eq!(leader.id, "node-a");
    assert!(leader.leader);

    let mut watch = c.watch(WatchFilter::default()).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.event_type, "member.join");
    assert_eq!(first.source, "node-a");

    c.publish("deployment", br#"{"sha":"abc"}"#).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ev = watch.next().await.unwrap().unwrap();
            if ev.event_type == "custom.deployment" {
                return ev;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ev.payload, br#"{"sha":"abc"}"#);
    let published = state.published.lock().await;
    assert_eq!(published[0].topic, "deployment");
    c.close().await.unwrap();
}

#[tokio::test]
async fn watch_topics() {
    let (addr, _) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let mut watch = c
        .watch(WatchFilter::new().topics(["deployment"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.publish("noise", b"x").await.unwrap();
    c.publish("deployment", br#"{"sha":"abc"}"#).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ev = watch.next().await.unwrap().unwrap();
            if ev.event_type == "custom.deployment" {
                return ev;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ev.event_type, "custom.deployment");
    c.close().await.unwrap();
}

#[tokio::test]
async fn watch_event_types() {
    let (addr, _) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let mut watch = c
        .watch(WatchFilter::new().event_types(["custom.deployment"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.publish("noise", b"x").await.unwrap();
    c.publish("deployment", br#"{"sha":"abc"}"#).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ev = watch.next().await.unwrap().unwrap();
            if ev.event_type == "custom.deployment" {
                return ev;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ev.event_type, "custom.deployment");
    c.close().await.unwrap();
}

#[tokio::test]
async fn watch_bad_topic() {
    let (addr, _) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let err = match c.watch(WatchFilter::new().topics(["bad topic"])).await {
        Err(e) => e,
        Ok(_) => panic!("expected invalid topic"),
    };
    assert!(err.to_string().contains("topic"));
    c.close().await.unwrap();
}

#[tokio::test]
async fn local_uses_env_addr() {
    let (addr, _) = start_fake_server().await;
    std::env::set_var("CLUSDR_GRPC_ADDR", &addr);
    std::env::set_var("CLUSDR_TLS", "disabled");
    let c = local(Options::new()).await.unwrap();
    let members = c.members().await.unwrap();
    assert_eq!(members[0].id, "node-a");
    c.close().await.unwrap();
    std::env::remove_var("CLUSDR_GRPC_ADDR");
    std::env::remove_var("CLUSDR_TLS");
}

#[tokio::test]
async fn retry_until_ready() {
    let health = FlakyHealth {
        failures: 2,
        calls: Default::default(),
    };
    let (addr, _) = start_with_health(health).await;
    let c = dial(
        &addr,
        Options::new()
            .insecure(true)
            .ready_timeout(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert_eq!(c.members().await.unwrap()[0].id, "node-a");
    c.close().await.unwrap();
}

#[tokio::test]
async fn empty_dial() {
    let err = match dial("  ", insecure()).await {
        Err(e) => e,
        Ok(_) => panic!("expected empty dial error"),
    };
    assert!(err.to_string().contains("empty"));
}

#[tokio::test]
async fn publish_too_large() {
    let (addr, _) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let err = c
        .publish("deployment", vec![b'x'; 64 * 1024 + 1])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exceeds"));
    c.close().await.unwrap();
}

#[tokio::test]
async fn lock_and_unlock() {
    let (addr, _) = start_fake_server().await;
    let c = dial(&addr, insecure()).await.unwrap();
    let lk = c
        .lock("scheduler", Some(Duration::from_secs(15)))
        .await
        .unwrap();
    assert_eq!(lk.name, "scheduler");
    assert!(lk.token > 0);
    c.unlock("scheduler").await.unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn try_lock_contention() {
    let (addr, _) = start_fake_server().await;
    let a = dial(&addr, Options::new().insecure(true).holder("a"))
        .await
        .unwrap();
    let b = dial(&addr, Options::new().insecure(true).holder("b"))
        .await
        .unwrap();
    let _lk = a.lock("job", Some(Duration::from_secs(15))).await.unwrap();
    let missed = b
        .try_lock("job", Some(Duration::from_secs(15)))
        .await
        .unwrap();
    assert!(missed.is_none());
    a.unlock("job").await.unwrap();
    let got = b
        .try_lock("job", Some(Duration::from_secs(15)))
        .await
        .unwrap();
    assert!(got.is_some());
    b.unlock("job").await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}
