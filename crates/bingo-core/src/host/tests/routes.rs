use super::*;

async fn open_route(host: &Host, key: &str) -> Attachment {
    host.open(
        SessionSelector::Create {
            spec: SessionSpec {
                key: Some(key.into()),
                ..spec("/work")
            },
        },
        who(),
        OpenOptions::default(),
    )
    .await
    .unwrap()
}

async fn assert_reconfigured_route(key: Option<&str>) {
    let store = Arc::new(crate::journal::MemoryStore::new());
    let host = host_on(store.clone(), ScriptedProvider::new(vec![])).await;
    let attachment = open_route(&host, "host/chat").await;
    let expected = key.map(str::to_string);
    host.reconfigure(&attachment.session, SessionChange::Key(expected.clone()))
        .await
        .unwrap();
    // Reading the summary waits behind reconfigure in the actor's mailbox.
    let summary = host.session_summary(&attachment.session).await.unwrap();
    let frames = store.replay(&attachment.session, Seq::ZERO).await.unwrap();
    let replayed = crate::session::replayed(&frames).unwrap();
    assert_eq!(
        (summary.key, replayed.summary.key),
        (expected.clone(), expected),
        "the actor and journal must both carry the changed route"
    );
    host.shutdown().await;
}

#[tokio::test]
async fn a_released_route_reaches_the_actor_and_journal() {
    assert_reconfigured_route(None).await;
}

#[tokio::test]
async fn a_renamed_route_reaches_the_actor_and_journal() {
    assert_reconfigured_route(Some("host/other-chat")).await;
}

#[tokio::test]
async fn a_released_route_allows_both_sessions_to_resume_on_another_host() {
    let store = Arc::new(crate::journal::MemoryStore::new());
    let host_a = host_on(store.clone(), ScriptedProvider::new(vec![])).await;
    let old = open_route(&host_a, "host/chat").await;
    host_a
        .reconfigure(&old.session, SessionChange::Key(None))
        .await
        .unwrap();
    let fresh = open_route(&host_a, "host/chat").await;
    let old_id = old.session.clone();
    let fresh_id = fresh.session.clone();
    host_a.shutdown().await;
    drop((old, fresh, host_a));

    let host_b = host_on(store, ScriptedProvider::new(vec![])).await;
    let fresh = host_b
        .open(
            SessionSelector::ByKey {
                key: "host/chat".into(),
            },
            who(),
            OpenOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(fresh.session, fresh_id);
    let old = host_b
        .open(
            SessionSelector::ById { id: old_id.clone() },
            who(),
            OpenOptions::default(),
        )
        .await
        .expect("the released route must not conflict with the fresh session");
    assert_eq!(old.session, old_id);
    assert_eq!(old.snapshot.summary.key, None);
    assert_eq!(fresh.snapshot.summary.key.as_deref(), Some("host/chat"));
    host_b.shutdown().await;
}
