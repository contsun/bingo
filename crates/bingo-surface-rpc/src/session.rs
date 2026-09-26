//! A subscription forwarded to notifications: one task per open session, one
//! for the gateway. Dropping the forwarder stops the task, so `session/close`,
//! a reopen and a resync are all "replace the value in the map".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bingo_sdk::{Event, GatewayEvent, IntentId, InteractionId, ItemId, SessionHandle};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::codec::{Message, Notification};
use crate::methods::{EventParams, EventRefParams, GatewaySessionHeadParams, name};
use crate::server::bounded::{Kind, Pins, fnv1a64};

/// A task draining one stream into one notification method.
#[derive(Debug)]
pub(crate) struct Pump(JoinHandle<()>);

impl Pump {
    pub(crate) fn session<S>(
        stream: S,
        out: Sender<Message>,
        pins: Pins,
        max_bytes: Option<usize>,
        generation: Arc<AtomicU64>,
    ) -> Pump
    where
        S: Stream<Item = EventParams> + Unpin + Send + 'static,
    {
        Pump(tokio::spawn(drain_session(
            stream, out, pins, max_bytes, generation,
        )))
    }

    pub(crate) fn gateway<S>(stream: S, out: Sender<Message>, max_bytes: Option<usize>) -> Pump
    where
        S: Stream<Item = GatewayEvent> + Unpin + Send + 'static,
    {
        Pump(tokio::spawn(drain_gateway(stream, out, max_bytes)))
    }
}

impl Drop for Pump {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn drain_session<S>(
    mut stream: S,
    out: Sender<Message>,
    pins: Pins,
    max_bytes: Option<usize>,
    generation: Arc<AtomicU64>,
) where
    S: Stream<Item = EventParams> + Unpin,
{
    while let Some(params) = stream.next().await {
        match &params.frame.event {
            Event::Compacted {
                generation: next, ..
            }
            | Event::Rewound {
                generation: next, ..
            } => {
                generation.store(*next, Ordering::Release);
            }
            _ => {}
        }
        let Ok(raw) = serde_json::to_string(&params) else {
            tracing::error!("a session event cannot serialize");
            continue;
        };
        if max_bytes.is_none_or(|budget| raw.len().saturating_add(128) <= budget)
            && let Ok(value) = serde_json::to_value(&params)
        {
            let message = Message::Notification(Notification::new(name::EVENT, value));
            if max_bytes.is_none_or(|budget| {
                serde_json::to_vec(&message).is_ok_and(|line| line.len() <= budget)
            }) {
                if out.send(message).await.is_err() {
                    break;
                }
                continue;
            }
        }
        let reference = event_reference(&params, raw, generation.load(Ordering::Acquire), &pins);
        let Ok(value) = serde_json::to_value(reference) else {
            tracing::error!("an event reference cannot serialize");
            continue;
        };
        if out
            .send(Message::Notification(Notification::new(
                name::EVENT_REF,
                value,
            )))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn event_reference(
    params: &EventParams,
    raw: String,
    generation: u64,
    pins: &Pins,
) -> EventRefParams {
    let event = &params.frame.event;
    let event_type = raw
        .split_once("\"event\":{\"type\":\"")
        .and_then(|(_, tail)| tail.split_once('"'))
        .map_or_else(|| "unknown".to_string(), |(kind, _)| kind.to_string());
    let links = event_links(event, params.frame.cause.as_ref());
    let total_bytes = raw.len();
    let checksum = fnv1a64(raw.as_bytes());
    let availability = pins.add(
        params.frame.session.clone(),
        params.root.clone(),
        Kind::Event,
        raw,
        Some(generation),
        links.item.clone(),
    );
    EventRefParams {
        session: params.frame.session.clone(),
        root: params.root.clone(),
        seq: params.frame.seq,
        message_id: format!("{}:{}", params.frame.session, params.frame.seq.0),
        event_type,
        item: links.item,
        interaction: links.interaction,
        intent: links.intent,
        state_uncertain: links.state_uncertain,
        generation,
        availability,
        total_bytes,
        checksum,
    }
}

struct EventLinks {
    item: Option<ItemId>,
    interaction: Option<InteractionId>,
    intent: Option<IntentId>,
    state_uncertain: bool,
}

fn event_links(event: &Event, cause: Option<&IntentId>) -> EventLinks {
    let item = match event {
        Event::ItemStarted { item }
        | Event::ItemUpdated { item }
        | Event::ItemCompleted { item } => Some(item.id.clone()),
        Event::ItemDelta { item, .. } => Some(item.clone()),
        _ => None,
    };
    let interaction = match event {
        Event::InteractionOpened { interaction } => Some(interaction.id.clone()),
        Event::InteractionResolved { id, .. } | Event::InteractionCancelled { id, .. } => {
            Some(id.clone())
        }
        _ => None,
    };
    let intent = match event {
        Event::IntentAck { intent, .. } => Some(intent.clone()),
        _ => cause.cloned(),
    };
    let state_uncertain = !matches!(
        event,
        Event::ItemStarted { .. }
            | Event::ItemUpdated { .. }
            | Event::ItemCompleted { .. }
            | Event::ItemDelta { .. }
            | Event::Notice { .. }
    );
    EventLinks {
        item,
        interaction,
        intent,
        state_uncertain,
    }
}

async fn drain_gateway<S>(mut stream: S, out: Sender<Message>, max_bytes: Option<usize>)
where
    S: Stream<Item = GatewayEvent> + Unpin,
{
    while let Some(event) = stream.next().await {
        let Ok(params) = serde_json::to_value(&event) else {
            continue;
        };
        let message = Message::Notification(Notification::new(name::GATEWAY_EVENT, params));
        let too_large = max_bytes.is_some_and(|budget| {
            serde_json::to_vec(&message).is_ok_and(|line| line.len() > budget)
        });
        let message = if too_large {
            let GatewayEvent::SessionCreated { summary } = event else {
                tracing::warn!("a bounded gateway event cannot fit its line");
                continue;
            };
            let params = GatewaySessionHeadParams {
                session: summary.id,
            };
            let Ok(value) = serde_json::to_value(params) else {
                continue;
            };
            Message::Notification(Notification::new(name::GATEWAY_SESSION_HEAD, value))
        } else {
            message
        };
        if out.send(message).await.is_err() {
            break;
        }
    }
}

/// One open session: where its frames go, and how a write reaches its actor.
#[derive(Debug)]
pub(crate) struct Forwarder {
    pub(crate) handle: SessionHandle,
    pub(crate) max_bytes: Option<usize>,
    pub(crate) generation: Arc<AtomicU64>,
    _events: Pump,
}

impl Forwarder {
    pub(crate) fn new(
        handle: SessionHandle,
        events: Pump,
        max_bytes: Option<usize>,
        generation: Arc<AtomicU64>,
    ) -> Self {
        Self {
            handle,
            max_bytes,
            generation,
            _events: events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bingo_sdk::{Frame, Level, Seq, SessionId};

    #[tokio::test]
    async fn a_rewound_frame_updates_the_generation_of_later_references() {
        let session = SessionId::from_raw("ses_1");
        let rewound = EventParams {
            frame: Frame {
                seq: Seq(1),
                ts: jiff::Timestamp::now(),
                session: session.clone(),
                cause: None,
                event: Event::Rewound {
                    generation: 2,
                    to_turn: bingo_sdk::TurnId::mint(),
                    dropped: Vec::new(),
                    files_restored: Vec::new(),
                },
            },
            root: None,
        };
        let large = EventParams {
            frame: Frame {
                seq: Seq(2),
                ts: jiff::Timestamp::now(),
                session,
                cause: None,
                event: Event::Notice {
                    level: Level::Info,
                    code: "large".into(),
                    text: "x".repeat(4096),
                },
            },
            root: None,
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        drain_session(
            futures::stream::iter([rewound, large]),
            tx,
            Pins::default(),
            Some(1024),
            Arc::clone(&generation),
        )
        .await;
        assert!(
            matches!(rx.recv().await, Some(Message::Notification(n)) if n.method == name::EVENT)
        );
        let Some(Message::Notification(reference)) = rx.recv().await else {
            panic!("large later event has a reference")
        };
        assert_eq!(reference.method, name::EVENT_REF);
        assert_eq!(reference.params["generation"], 2);
        assert_eq!(generation.load(Ordering::Acquire), 2);
    }

    #[test]
    fn event_reference_keeps_the_actual_event_tag_and_identity() {
        let params = EventParams {
            frame: Frame {
                seq: Seq(4),
                ts: jiff::Timestamp::now(),
                session: SessionId::from_raw("ses_child"),
                cause: None,
                event: Event::Notice {
                    level: Level::Info,
                    code: "x".into(),
                    text: "large body".into(),
                },
            },
            root: Some(SessionId::from_raw("ses_root")),
        };
        let raw = serde_json::to_string(&params).unwrap();
        let reference = event_reference(&params, raw.clone(), 3, &Pins::default());
        assert_eq!(reference.event_type, "notice");
        assert_eq!(reference.seq, Seq(4));
        assert_eq!(reference.root, params.root);
        assert_eq!(reference.total_bytes, raw.len());
        assert_eq!(reference.checksum, fnv1a64(raw.as_bytes()));
        assert!(!reference.state_uncertain);
    }
}
