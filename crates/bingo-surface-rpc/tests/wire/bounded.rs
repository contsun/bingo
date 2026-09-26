//! Wire obligations for opt-in bounded sessions; synthetic content only.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use bingo_sdk::{
    ErrorCode, Event, HistoryChunk, HistoryPage, HostApi, Item, ItemBody, ItemId, ItemStatus,
    OpenHistory, OpenOptions, OversizedItem, TreeBackfill, TurnId, TurnStatus, Usage,
};
use bingo_surface_rpc::codec::Message;
use bingo_surface_rpc::document;
use bingo_surface_rpc::methods::{
    EventParams, ListHeadsParams, OmittedField, ReferenceAvailability, WireOversizedItem, name,
};
use serde_json::{Value, json};

use super::host::{TestHost, fnv1a64, frame, session_id, ts};
use super::{RemoteKernel, Wire};

const LINE_LIMIT: usize = 16 * 1024 * 1024;

fn large_item(repeats: usize) -> Item {
    Item {
        id: ItemId::from_raw("itm_oversized"),
        turn: None,
        round: 0,
        status: ItemStatus::Completed,
        started_at: ts(),
        completed_at: Some(ts()),
        intent: None,
        body: ItemBody::Assistant {
            text: "\"\\中\n".repeat(repeats),
        },
        meta: Default::default(),
    }
}

#[test]
fn bounded_extensions_are_discoverable_without_changing_legacy_defaults() {
    let options = serde_json::to_value(OpenOptions::default()).expect("legacy options serialize");
    assert_eq!(options, json!({ "children": false }));
    let schema = document();
    let options = &schema["$defs"]["OpenOptions"]["properties"];
    assert!(options.get("maxSnapshotBytes").is_some());
    assert!(options.get("treeBackfill").is_some());
    assert!(
        schema["$defs"]["HistoryPage"]["properties"]
            .get("maxBytes")
            .is_some()
    );
    assert!(
        schema["$defs"]["HistoryChunk"]["properties"]
            .get("oversized")
            .is_some()
    );
    assert!(
        schema["$defs"]["HistoryResult"]["properties"]
            .get("oversized")
            .is_some()
    );
    assert!(
        schema["$defs"]["WireOversizedItem"]["properties"]
            .get("availability")
            .is_some()
    );
    assert!(
        schema["$defs"]["OversizedItem"]["properties"]
            .get("availability")
            .is_none()
    );
    assert!(
        schema["$defs"]["OpenResult"]["properties"]
            .get("history")
            .is_some()
    );
    assert!(
        schema["$defs"]["OpenResult"]["properties"]
            .get("tree")
            .is_some()
    );
    assert!(schema["methods"].get("session/listHeads").is_some());
    assert!(schema["notifications"].get("gateway/sessionHead").is_some());
    assert!(
        serde_json::from_value::<ListHeadsParams>(json!({
            "filter":{"cwd":"/tmp", "limit":500}, "maxBytes":4096
        }))
        .is_err(),
        "old count-limit must not silently truncate the ID cursor"
    );
    assert!(schema["methods"].get("session/itemPart").is_some());
    assert!(schema["methods"].get("session/fieldPart").is_some());
    assert!(schema["methods"].get("session/eventPart").is_some());
    assert!(schema["notifications"].get("eventRef").is_some());
}

async fn deferred_event_case(repeats: usize) {
    let original = frame(
        1,
        Event::ItemCompleted {
            item: large_item(repeats),
        },
    );
    let expected = serde_json::to_string(&EventParams {
        frame: original.clone(),
        root: None,
    })
    .expect("synthetic event serializes");
    assert!(expected.len() > LINE_LIMIT);
    let next = frame(
        2,
        Event::TurnCompleted {
            turn: TurnId::from_raw("trn_1"),
            status: TurnStatus::Completed,
            usage: Usage::default(),
        },
    );
    let (host, _) = TestHost::with(vec![original, next]);
    let mut wire = Wire::started(host).await;
    wire.call(
        name::SESSION_OPEN,
        json!({"selector": super::host::selector(), "options": {
            "maxSnapshotBytes": 4 * 1024 * 1024
        }}),
    )
    .await
    .expect("bounded attachment opens");

    // Opening does not automatically transfer the body, even if it is 100MiB.
    let reference = match wire.recv().await {
        Message::Notification(notification) => {
            assert_eq!(notification.method, "eventRef");
            let value = notification.params;
            assert_eq!(value["session"], json!(session_id()));
            assert_eq!(value["seq"], json!(1));
            assert_eq!(value["item"], json!(ItemId::from_raw("itm_oversized")));
            assert_eq!(value["totalBytes"], json!(expected.len()));
            assert_eq!(value["checksum"], json!(fnv1a64(expected.as_bytes())));
            assert!(
                value["availability"]["token"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
            );
            value
        }
        other => panic!("expected bounded reference, got {other:?}"),
    };
    let following = wire.recv().await;
    assert!(
        matches!(following, Message::Notification(n) if n.method == name::EVENT && n.params["seq"] == 2)
    );

    // Only an explicit user-directed part request starts moving the full bytes.
    let mut reconstructed = String::new();
    let mut offset = 0;
    loop {
        let part = wire
            .call(
                name::SESSION_EVENT_PART,
                json!({"session":session_id(), "token":reference["availability"]["token"],
                "offset":offset, "maxBytes": 256 * 1024}),
            )
            .await
            .expect("a pinned event is accessible on demand");
        let data = part["data"].as_str().expect("UTF-8 fragment");
        assert!(data.len() <= 256 * 1024);
        assert!(!data.is_empty(), "every nonterminal page advances");
        assert_eq!(part["totalBytes"], json!(expected.len()));
        reconstructed.push_str(data);
        let next = part["nextOffset"].as_u64();
        match next {
            Some(next) => {
                assert!(next as usize > offset, "no infinite cursor");
                offset = next as usize;
            }
            None => break,
        }
    }
    assert_eq!(
        reconstructed, expected,
        "original event exact, no fake item"
    );
    let decoded: Value = serde_json::from_str(&reconstructed).expect("original EventParams JSON");
    assert_eq!(
        decoded["event"]["item"]["id"],
        json!(ItemId::from_raw("itm_oversized"))
    );
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn oversized_event_reference_does_not_block_next_lifecycle_event() {
    deferred_event_case(2_400_000).await;
}

#[tokio::test]
async fn event_above_the_desktop_queue_budget_is_read_only_on_demand() {
    deferred_event_case(4_400_000).await;
}

/// Stress case is separate from the routine suite: the synthetic 100MiB
/// payload exercises the agreed single-reference pin ceiling without making
/// every CI matrix job allocate several copies of it.
#[tokio::test]
#[ignore = "100MiB pinned-event stress; run explicitly after bounded server implementation"]
async fn hundred_megabyte_event_is_not_pushed_on_open() {
    deferred_event_case(11_500_000).await;
}

#[tokio::test]
async fn oversized_nonitem_open_retains_trusted_cwd_and_serves_immutable_field_by_part() {
    let mut state = super::host::fresh_state();
    let payload = json!({ "note": "\"\\中\n".repeat(2_400_000) });
    let original = serde_json::to_string(&payload).expect("synthetic payload serializes");
    assert!(original.len() > LINE_LIMIT);
    state
        .extensions
        .entry("test".into())
        .or_default()
        .insert("large".into(), payload);
    let (host, _) = TestHost::with_snapshot(state);
    let mut wire = Wire::started(host).await;
    let result = wire
        .call(
            name::SESSION_OPEN,
            json!({
                "selector": super::host::selector(),
                "options": {"maxSnapshotBytes": 4 * 1024 * 1024}
            }),
        )
        .await
        .expect("safe bounded shell opens");
    assert_eq!(result["session"], json!(session_id()));
    assert_eq!(result["snapshot"]["summary"]["id"], json!(session_id()));
    assert_eq!(result["snapshot"]["summary"]["cwd"], json!("/tmp"));
    assert_eq!(result["snapshot"]["seq"], json!(0));
    assert!(result["snapshot"]["extensions"]["test"]["large"].is_null());
    let field = &result["omittedFields"][0];
    assert_eq!(field["path"], json!(["extensions", "test", "large"]));
    assert_eq!(field["totalBytes"], json!(original.len()));
    assert_eq!(field["checksum"], json!(fnv1a64(original.as_bytes())));
    assert!(
        field["availability"]["token"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    let part = wire
        .call(
            name::SESSION_FIELD_PART,
            json!({
                "session":session_id(), "token":field["availability"]["token"], "offset":0, "maxBytes":256 * 1024
            }),
        )
        .await
        .expect("pinned field is accessible only on demand");
    assert!(
        part["data"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.len() <= 256 * 1024)
    );
    assert_eq!(part["totalBytes"], json!(original.len()));
    assert!(part["nextOffset"].as_u64().is_some_and(|n| n > 0));
    wire.call(name::SESSION_CLOSE, json!({"session":session_id()}))
        .await
        .expect("the session closes");
    assert!(
        wire.call(
            name::SESSION_FIELD_PART,
            json!({
                "session":session_id(), "token":field["availability"]["token"], "offset":0, "maxBytes":4096
            })
        )
        .await
        .is_err(),
        "a closed attachment does not retain pinned bytes"
    );
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn bounded_tree_open_declares_all_old_descendants_unloaded() {
    let (host, _) = TestHost::with(Vec::new());
    let mut wire = Wire::started(host).await;
    let opened = wire
        .call(
            name::SESSION_OPEN,
            json!({
                "selector": super::host::selector(),
                "options": {"children": true, "maxSnapshotBytes": 4 * 1024 * 1024,
                    "treeBackfill": "liveOnly"}
            }),
        )
        .await
        .expect("bounded tree opens");
    assert_eq!(
        opened["tree"],
        json!({
            "backfill":"liveOnly", "descendantsComplete":false
        })
    );
    assert_eq!(opened["snapshot"]["summary"]["id"], json!(session_id()));
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn a_near_limit_string_request_id_gets_a_small_null_id_error() {
    let (host, _) = TestHost::with(Vec::new());
    let mut wire = Wire::started(host).await;
    let request = json!({
        "jsonrpc":"2.0", "id":"x".repeat(9 * 1024 * 1024),
        "method":"session/open", "params":{
            "selector":super::host::selector(),
            "options":{"maxSnapshotBytes":4096}
        }
    });
    wire.line(&serde_json::to_string(&request).expect("a valid incoming line"))
        .await;
    let Message::Response(response) = wire.recv().await else {
        panic!("a bounded protocol error is sent")
    };
    assert!(response.id.is_none(), "do not echo an oversized id");
    assert!(wire.line_bytes.last().is_some_and(|bytes| *bytes <= 1024));
    assert_eq!(wire.finish().await.code, 0, "the host survives the error");
}

#[tokio::test]
async fn invalid_byte_budgets_fail_boundedly_instead_of_returning_empty_pages() {
    let (host, _) = TestHost::with(Vec::new());
    let mut wire = Wire::started(host).await;
    for budget in [0, 8 * 1024 * 1024 + 1] {
        let error = wire
            .call(
                name::SESSION_OPEN,
                json!({
                    "selector":super::host::selector(), "options":{"maxSnapshotBytes":budget}
                }),
            )
            .await
            .expect_err("invalid open budget");
        assert_eq!(error.data, Some(json!({"code":"INVALID_INPUT"})));
        let error = wire
            .call(
                name::SESSION_HISTORY,
                json!({
                    "session":session_id(), "page":{"maxBytes":budget}
                }),
            )
            .await
            .expect_err("invalid history budget");
        assert_eq!(error.data, Some(json!({"code":"INVALID_INPUT"})));
        let error = wire
            .call(
                name::SESSION_CHILDREN,
                json!({
                    "parent":session_id(), "maxBytes":budget
                }),
            )
            .await
            .expect_err("invalid children budget");
        assert_eq!(error.data, Some(json!({"code":"INVALID_INPUT"})));
        let error = wire
            .call(
                name::SESSION_LIST_HEADS,
                json!({
                    "filter":{}, "maxBytes":budget
                }),
            )
            .await
            .expect_err("invalid head-list budget");
        assert_eq!(error.data, Some(json!({"code":"INVALID_INPUT"})));
    }
    let tiny = wire
        .call(
            name::SESSION_CHILDREN,
            json!({
                "parent":session_id(), "maxBytes":1
            }),
        )
        .await
        .expect_err("below the negotiated minimum");
    assert_eq!(tiny.data, Some(json!({"code":"INVALID_INPUT"})));
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn a_valid_child_page_budget_that_fits_no_id_fails_instead_of_looping() {
    let mut child = super::host::summary();
    child.id = bingo_sdk::SessionId::from_raw(format!("ses_{}", "x".repeat(2_000)));
    child.parent = Some(bingo_sdk::ParentLink {
        session: session_id(),
        item: None,
    });
    let (host, _) = TestHost::with_summaries(vec![child]);
    let mut wire = Wire::started(host).await;
    for (method, params) in [
        (
            name::SESSION_CHILDREN,
            json!({"parent":session_id(), "maxBytes":1024}),
        ),
        (
            name::SESSION_LIST_HEADS,
            json!({"filter":{}, "maxBytes":1024}),
        ),
    ] {
        let error = wire
            .call(method, params)
            .await
            .expect_err("do not return a zero-progress next cursor");
        assert_eq!(error.data, Some(json!({"code":"PROTOCOL_LIMIT"})));
    }
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn same_generation_item_change_never_mints_a_token_for_wrong_bytes() {
    let item = large_item(2_400_000);
    let original = serde_json::to_string(&item).expect("original item JSON");
    let mut changed = item.clone();
    let ItemBody::Assistant { text } = &mut changed.body else {
        panic!("the fixture is an assistant item")
    };
    text.replace_range(..1, "\\");
    let altered = serde_json::to_string(&changed).expect("changed item JSON");
    assert_eq!(
        original.len(),
        altered.len(),
        "length is not enough to verify a pin"
    );
    assert_ne!(fnv1a64(original.as_bytes()), fnv1a64(altered.as_bytes()));

    let (host, session) = TestHost::with_history_item(item);
    session.change_on_pin.store(true, Ordering::SeqCst);
    let mut wire = Wire::started(host).await;
    wire.call(
        name::SESSION_OPEN,
        json!({
            "selector":super::host::selector(), "options":{"maxSnapshotBytes":4 * 1024 * 1024}
        }),
    )
    .await
    .expect("safe open");
    let error = wire
        .call(
            name::SESSION_HISTORY,
            json!({
                "session":session_id(), "page":{"limit":20, "maxBytes":4 * 1024 * 1024,
                    "generation":3}
            }),
        )
        .await
        .expect_err("changed item must not be certified with the old checksum");
    assert_eq!(error.data, Some(json!({"code":"STALE_GENERATION"})));
    assert!(
        wire.line_bytes.last().is_some_and(|bytes| *bytes < 1024),
        "no available token or large body escapes on stale metadata"
    );
    assert_eq!(
        session.pin_reads.lock().expect("recorder").as_slice(),
        &[(ItemId::from_raw("itm_oversized"), 3)]
    );
    let sessions = wire
        .call(name::SESSION_LIST, json!({"filter":{}}))
        .await
        .expect("the host survives a failed pin");
    assert_eq!(sessions["sessions"][0]["id"], json!(session_id()));
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn oversized_history_item_has_no_cursor_loop_and_is_exact_on_demand() {
    let item = large_item(2_400_000);
    let expected = serde_json::to_string(&item).expect("synthetic item serializes");
    assert!(expected.len() > LINE_LIMIT);
    let (host, session) = TestHost::with_history_item(item);
    let mut wire = Wire::started(host).await;
    let open = wire
        .call(
            name::SESSION_OPEN,
            json!({
                "selector":super::host::selector(),
                "options":{"maxSnapshotBytes":4 * 1024 * 1024}
            }),
        )
        .await
        .expect("small shell before old history");
    assert_eq!(
        open["history"],
        json!({
            "before":null, "hasMore":true, "generation":3
        })
    );
    let page = wire
        .call(
            name::SESSION_HISTORY,
            json!({
                "session":session_id(), "page":{"limit":20, "maxBytes":4 * 1024 * 1024,
                    "generation":3}
            }),
        )
        .await
        .expect("an exact oversized reference, not a truncated item");
    assert_eq!(page["items"], json!([]));
    assert_eq!(page["next"], json!(ItemId::from_raw("itm_oversized")));
    let oversized = &page["oversized"];
    assert_eq!(oversized["id"], page["next"]);
    assert_eq!(oversized["availability"]["kind"], json!("available"));
    assert_eq!(oversized["totalBytes"], json!(expected.len()));
    assert_eq!(oversized["checksum"], json!(fnv1a64(expected.as_bytes())));
    for invalid in [0, 256 * 1024 + 1] {
        let error = wire
            .call(
                name::SESSION_ITEM_PART,
                json!({
                    "session":session_id(), "item":oversized["id"], "generation":3,
                    "token":oversized["availability"]["token"], "offset":0,
                    "maxBytes":invalid
                }),
            )
            .await
            .expect_err("invalid part budget");
        assert_eq!(error.data, Some(json!({"code":"INVALID_INPUT"})));
    }

    let mut offset = 0;
    let mut actual = String::new();
    loop {
        let part = wire
            .call(
                name::SESSION_ITEM_PART,
                json!({
                    "session":session_id(), "item":oversized["id"],
                    "generation":3, "token":oversized["availability"]["token"],
                    "offset":offset, "maxBytes":256 * 1024
                }),
            )
            .await
            .expect("explicit item part");
        let text = part["data"].as_str().expect("UTF-8 JSON fragment");
        assert!(!text.is_empty() && text.len() <= 256 * 1024);
        actual.push_str(text);
        match part["nextOffset"].as_u64() {
            Some(next) => {
                assert!(next as usize > offset);
                offset = next as usize;
            }
            None => break,
        }
    }
    assert_eq!(actual, expected);
    assert_eq!(
        session
            .pin_reads
            .lock()
            .expect("recorder is not poisoned")
            .as_slice(),
        &[(ItemId::from_raw("itm_oversized"), 3)],
        "part transport pins one immutable actor-cut item, not N mutable reads"
    );
    let end = wire
        .call(
            name::SESSION_HISTORY,
            json!({
                "session":session_id(), "page":{"before":page["next"],
                    "limit":20, "maxBytes":4 * 1024 * 1024, "generation":3}
            }),
        )
        .await
        .expect("exclusive next cursor advances");
    assert_eq!(end["items"], json!([]));
    assert!(end.get("next").is_none() || end["next"].is_null());
    assert!(end.get("oversized").is_none() || end["oversized"].is_null());
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn bounded_gateway_created_event_invalidates_head_without_a_large_summary() {
    let mut summary = super::host::summary();
    summary.title = Some("t".repeat(17 * 1024 * 1024));
    let (host, _) = TestHost::with_gateway_summary(summary);
    let mut wire = Wire::started(host).await;
    wire.call(name::GATEWAY_SUBSCRIBE, json!({"maxBytes":8192}))
        .await
        .expect("bounded gateway subscription");
    match wire.recv().await {
        Message::Notification(notification) => {
            assert_eq!(notification.method, "gateway/sessionHead");
            assert_eq!(notification.params, json!({"session":session_id()}));
            assert!(wire.line_bytes.last().is_some_and(|bytes| *bytes <= 8192));
        }
        other => panic!("expected ID-only gateway invalidation, got {other:?}"),
    }
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn first_connect_lists_verified_heads_without_large_root_title_or_key() {
    let mut summaries = Vec::new();
    for index in (0..500).rev() {
        let mut summary = super::host::summary();
        summary.id = bingo_sdk::SessionId::from_raw(format!("ses_{index:04}"));
        summary.title = Some(if index > 2 {
            "m".repeat(35 * 1024)
        } else {
            format!("Session {index}")
        });
        summary.key = Some(format!("host/{index}"));
        if index == 0 {
            summary.title = Some("t".repeat(17 * 1024 * 1024));
        }
        if index == 1 {
            summary.key = Some(format!("host/{}", "k".repeat(17 * 1024 * 1024)));
        }
        summaries.push(summary);
    }
    let (host, _) = TestHost::with_summaries(summaries);
    let mut wire = Wire::started(host).await;
    let mut seen = BTreeSet::new();
    let mut after: Option<String> = None;
    loop {
        let result = wire
            .call(
                name::SESSION_LIST_HEADS,
                json!({
                    "filter":{"cwd":"/tmp"}, "after":after, "maxBytes":8192
                }),
            )
            .await
            .expect("bounded heads for first-connect ownership check");
        assert!(wire.line_bytes.last().is_some_and(|bytes| *bytes <= 8192));
        let heads = result["heads"].as_array().expect("heads array");
        for head in heads {
            let id = head["id"].as_str().expect("true session id");
            assert!(after.as_deref().is_none_or(|cursor| id > cursor));
            assert!(seen.insert(id.to_owned()), "no omitted or repeated session");
            assert_eq!(
                head["cwd"],
                json!("/tmp"),
                "trusted cwd for Native owner check"
            );
            assert_eq!(head["driver"], json!("model"));
            assert_eq!(head["busy"], json!(false));
            assert!(head["createdAt"].as_str().is_some());
            assert!(head["updatedAt"].as_str().is_some());
            if id == "ses_0000" {
                assert!(head.get("title").is_none(), "absent, never fake/truncated");
                assert_eq!(head["omitted"][0]["field"], json!("title"));
                assert_eq!(head["omitted"][0]["totalBytes"], json!(17 * 1024 * 1024));
            } else if id == "ses_0001" {
                assert!(head.get("key").is_none(), "not a keyless session");
                assert_eq!(head["omitted"][0]["field"], json!("key"));
                assert!(
                    head["omitted"][0]["totalBytes"]
                        .as_u64()
                        .is_some_and(|n| n > 17 * 1024 * 1024)
                );
            } else if id == "ses_0002" {
                assert_eq!(head["title"], json!("Session 2"));
                assert_eq!(head["key"], json!("host/2"));
            } else {
                assert!(head.get("title").is_none());
                assert_eq!(head["omitted"][0]["field"], json!("title"));
            }
        }
        match result["next"].as_str() {
            Some(next) => {
                assert_eq!(
                    heads.last().and_then(|head| head["id"].as_str()),
                    Some(next)
                );
                assert!(after.as_deref().is_none_or(|previous| next > previous));
                after = Some(next.to_owned());
            }
            None => {
                assert!(result.get("next").is_none() || result["next"].is_null());
                break;
            }
        }
    }
    assert_eq!(seen.len(), 500);
    assert_eq!(wire.finish().await.code, 0);
}

#[tokio::test]
async fn child_ids_page_without_repeating_or_serializing_huge_titles() {
    let mut summaries = Vec::new();
    for index in (0..1_000).rev() {
        let mut child = super::host::summary();
        child.id = bingo_sdk::SessionId::from_raw(format!("ses_child_{index:04}"));
        child.parent = Some(bingo_sdk::ParentLink {
            session: session_id(),
            item: None,
        });
        if index == 500 {
            child.title = Some("x".repeat(17 * 1024 * 1024));
        }
        summaries.push(child);
    }
    let (host, _) = TestHost::with_summaries(summaries);
    let mut wire = Wire::started(host).await;
    let mut seen = BTreeSet::new();
    let mut after: Option<String> = None;
    loop {
        let result = wire
            .call(
                name::SESSION_CHILDREN,
                json!({
                    "parent": session_id(), "after": after, "maxBytes": 8192
                }),
            )
            .await
            .expect("bounded page of child ids");
        assert!(
            wire.line_bytes.last().is_some_and(|n| *n <= 8192),
            "count the full UTF-8 JSON-RPC envelope, not just ids"
        );
        let children = result["children"].as_array().expect("child ids array");
        if children.is_empty() {
            assert!(result.get("next").is_none() || result["next"].is_null());
            break;
        }
        for child in children {
            let id = child.as_str().expect("child id");
            assert!(after.as_deref().is_none_or(|prior| id > prior));
            assert!(seen.insert(id.to_owned()), "no child repeated across pages");
        }
        let last = children
            .last()
            .and_then(Value::as_str)
            .expect("nonempty page");
        match result["next"].as_str() {
            Some(next) => {
                assert_eq!(next, last, "exclusive cursor is the last included id");
                after = Some(next.to_owned());
            }
            None => {
                after = Some(last.to_owned());
                let terminal = wire
                    .call(
                        name::SESSION_CHILDREN,
                        json!({
                            "parent": session_id(), "after": after, "maxBytes":8192
                        }),
                    )
                    .await
                    .expect("empty terminal page");
                assert!(terminal["children"].as_array().is_some_and(Vec::is_empty));
                assert!(terminal.get("next").is_none() || terminal["next"].is_null());
                break;
            }
        }
    }
    assert_eq!(seen.len(), 1_000, "no old descendants silently missing");
    assert_eq!(wire.finish().await.code, 0);
}

#[test]
fn a_pin_budget_failure_is_explicit_and_not_a_fake_empty_field() {
    let unavailable = OmittedField {
        path: vec!["extensions".into(), "test".into(), "large".into()],
        availability: ReferenceAvailability::Unavailable {
            reason: "pinBudgetExceeded".into(),
        },
        total_bytes: 140 * 1024 * 1024,
        checksum: "e2e682ad48446123".into(),
    };
    let value = serde_json::to_value(unavailable).expect("ref serializes");
    assert_eq!(
        value["availability"],
        json!({
            "kind":"unavailable", "reason":"pinBudgetExceeded"
        })
    );
    assert_eq!(value["totalBytes"], json!(140 * 1024 * 1024));
    for invalid in [
        json!({"kind":"available"}),
        json!({"kind":"unavailable"}),
        json!({"kind":"available","token":"x","reason":"also unavailable"}),
        json!({"kind":"unavailable","reason":"quota","token":"forged"}),
    ] {
        assert!(serde_json::from_value::<ReferenceAvailability>(invalid).is_err());
    }
    let availability = &document()["$defs"]["ReferenceAvailability"]["oneOf"];
    assert!(availability.as_array().is_some_and(|variants| {
        variants.len() == 2
            && variants
                .iter()
                .all(|variant| variant["additionalProperties"] == false)
    }));
    assert!(
        document()["$defs"]["ErrorCode"]["oneOf"]
            .as_array()
            .is_some_and(|variants| variants
                .iter()
                .any(|variant| variant["const"] == "PROTOCOL_LIMIT"))
    );
}

#[tokio::test]
async fn canonical_remote_kernel_rejects_bounded_refs_instead_of_dropping_them() {
    let (host, _) = TestHost::with(Vec::new());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server);
    let (client_reader, client_writer) = tokio::io::split(client);
    let served = tokio::spawn(super::serve(host, server_reader, server_writer));
    let remote = RemoteKernel::connect(client_reader, client_writer);
    remote
        .initialize(super::host::who())
        .await
        .expect("initializes");
    for options in [
        OpenOptions {
            max_snapshot_bytes: Some(4 * 1024 * 1024),
            ..Default::default()
        },
        OpenOptions {
            children: true,
            tree_backfill: Some(TreeBackfill::LiveOnly),
            ..Default::default()
        },
    ] {
        let error = remote
            .open(super::host::selector(), super::host::who(), options)
            .await
            .expect_err("canonical FrameStream cannot carry references");
        assert_eq!(error.code, ErrorCode::InvalidInput);
    }
    let attachment = remote
        .open(
            super::host::selector(),
            super::host::who(),
            OpenOptions::default(),
        )
        .await
        .expect("legacy open remains compatible");
    let error = attachment
        .handle
        .history(HistoryPage {
            max_bytes: Some(4096),
            ..Default::default()
        })
        .await
        .expect_err("canonical history cannot carry deferred item references");
    assert_eq!(error.code, ErrorCode::InvalidInput);
    drop(attachment);
    remote.shutdown().await.expect("shutdown succeeds");
    served
        .await
        .expect("server task runs")
        .expect("server exits cleanly");
}

#[test]
fn history_cursor_can_represent_an_empty_initial_window_without_a_fake_item_id() {
    let schema = document();
    let history = &schema["$defs"]["OpenHistory"]["properties"];
    assert!(history.get("before").is_some());
    assert!(history.get("hasMore").is_some());
    assert!(history.get("generation").is_some());
    let initial = OpenHistory {
        before: None,
        has_more: true,
        generation: 7,
    };
    assert_eq!(
        serde_json::to_value(initial).unwrap(),
        json!({
            "before":null, "hasMore":true, "generation":7
        })
    );
    let item = ItemId::from_raw("itm_oversized");
    let first = HistoryChunk {
        items: Vec::new(),
        next: Some(item.clone()),
        generation: 7,
        oversized: Some(OversizedItem {
            id: item.clone(),
            total_bytes: 20_000_000,
            checksum: "0000000000000000".into(),
        }),
    };
    assert_eq!(first.oversized.as_ref().unwrap().id, item);
    let wire = WireOversizedItem {
        id: item.clone(),
        total_bytes: 20_000_000,
        checksum: "0000000000000000".into(),
        availability: ReferenceAvailability::Available {
            token: "opaque".into(),
        },
    };
    assert_eq!(
        serde_json::to_value(wire).unwrap()["availability"]["kind"],
        "available"
    );
    let next = HistoryPage {
        before: first.next,
        limit: 20,
        max_bytes: Some(4 * 1024 * 1024),
        generation: Some(first.generation),
    };
    assert_eq!(next.before, Some(item));
    // `before` is exclusive: after explicit part fetch, the same huge item
    // cannot be selected forever by a zero-progress page.
}
