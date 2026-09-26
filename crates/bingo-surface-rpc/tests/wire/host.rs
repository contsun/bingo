//! The scripted kernel the wire tests run against: a fixed turn of frames, a
//! `SessionPort` that records every write, and a `HostApi` that can refuse.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bingo_sdk::{
    Activation, Answer, Attachment, Catalog, CatalogEntry, CatalogKind, ClientIdentity,
    CloseReason, Delivery, ErrorCode, Event, Frame, FrameStream, GatewayEvent, GatewayStream,
    HistoryChunk, HistoryPage, HostApi, HostHandle, Input, IntentId, InteractionId, InterruptScope,
    Item, ItemBody, ItemId, ItemStatus, KernelError, OpenOptions, OversizedItem, Seq,
    SessionFilter, SessionHandle, SessionId, SessionPort, SessionSelector, SessionState,
    SessionSummary, TurnId, TurnOrigin, TurnStatus, Usage,
};
use futures::StreamExt;
use jiff::Timestamp;
use serde_json::Value;

pub fn fnv1a64(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn ts() -> Timestamp {
    Timestamp::from_second(1_700_000_000).expect("a fixed instant")
}

pub fn session_id() -> SessionId {
    SessionId::from_raw("ses_1")
}

pub fn summary() -> SessionSummary {
    SessionSummary {
        tools: None,
        system_extra: None,
        driver: Default::default(),
        id: session_id(),
        key: None,
        title: None,
        cwd: "/tmp".into(),
        parent: None,
        model: Some("fake-1".into()),
        provider: Some("fake".into()),
        created_at: ts(),
        updated_at: ts(),
        usage: Usage::default(),
        busy: false,
        messages: None,
    }
}

pub fn child_id() -> SessionId {
    SessionId::from_raw("ses_2")
}

/// The head frame of a child of the scripted session, as a tree attachment
/// would carry it after the root's own frames.
pub fn child_head() -> Frame {
    let mut summary = summary();
    summary.id = child_id();
    summary.parent = Some(bingo_sdk::ParentLink {
        session: session_id(),
        item: Some(ItemId::from_raw("itm_1")),
    });
    Frame {
        seq: Seq(1),
        ts: ts(),
        session: child_id(),
        cause: None,
        event: Event::SessionUpdated { summary },
    }
}

pub fn frame(seq: u64, event: Event) -> Frame {
    Frame {
        seq: Seq(seq),
        ts: ts(),
        session: session_id(),
        cause: None,
        event,
    }
}

/// A whole turn, with a `Lagged` marker in the middle to prove it travels like
/// any other frame.
pub fn script() -> Vec<Frame> {
    let turn = TurnId::from_raw("trn_1");
    vec![
        frame(
            1,
            Event::TurnStarted {
                turn: turn.clone(),
                inputs: Vec::new(),
                origin: TurnOrigin::Submit,
            },
        ),
        frame(
            2,
            Event::ItemCompleted {
                item: Item {
                    id: ItemId::from_raw("itm_1"),
                    turn: Some(turn.clone()),
                    round: 0,
                    status: ItemStatus::Completed,
                    started_at: ts(),
                    completed_at: Some(ts()),
                    intent: None,
                    body: ItemBody::Assistant {
                        text: "hello".into(),
                    },
                    meta: serde_json::Map::new(),
                },
            },
        ),
        frame(
            3,
            Event::Lagged {
                from: Seq(2),
                to: Seq(3),
            },
        ),
        frame(
            4,
            Event::TurnCompleted {
                turn,
                status: TurnStatus::Completed,
                usage: Usage::default(),
            },
        ),
    ]
}

pub fn last_seq() -> Seq {
    Seq(script().len() as u64)
}

/// The snapshot every `open` answers with, before any frame is applied.
pub fn fresh_state() -> SessionState {
    SessionState::new(summary())
}

#[derive(Default)]
pub struct TestSession {
    pub frames: Vec<Frame>,
    pub history_item: Option<Item>,
    pub pin_reads: Mutex<Vec<(ItemId, u64)>>,
    /// The actor changed this item's body after describing history but before
    /// answering the one-shot pin request, without advancing generation.
    pub change_on_pin: AtomicBool,
    pub submits: Mutex<Vec<(IntentId, Input)>>,
    pub interrupts: Mutex<Vec<(IntentId, InterruptScope)>>,
    pub answers: Mutex<Vec<(IntentId, InteractionId, Answer, Activation)>>,
    pub pages: Mutex<Vec<HistoryPage>>,
    /// What reached this session through the host rather than its port.
    pub delivered: Mutex<Vec<(IntentId, Input, Delivery)>>,
    pub extended: Mutex<Vec<(String, String, Value)>>,
    pub signalled: Mutex<Vec<(String, String, Value)>>,
}

impl TestSession {
    pub fn stream(&self, since: Seq, durable_only: bool) -> FrameStream {
        let frames: Vec<Frame> = self
            .frames
            .iter()
            .filter(|frame| frame.seq > since && (!durable_only || frame.event.is_durable()))
            .cloned()
            .collect();
        Box::pin(futures::stream::iter(frames))
    }

    pub fn submits(&self) -> MutexGuard<'_, Vec<(IntentId, Input)>> {
        self.submits.lock().expect("the recorder is not poisoned")
    }
}

#[async_trait]
impl SessionPort for TestSession {
    fn submit(&self, intent: IntentId, input: Input) {
        self.submits().push((intent, input));
    }

    fn interrupt(&self, intent: IntentId, scope: InterruptScope) {
        self.interrupts
            .lock()
            .expect("the recorder is not poisoned")
            .push((intent, scope));
    }

    fn answer(
        &self,
        intent: IntentId,
        interaction: InteractionId,
        answer: Answer,
        activation: Activation,
    ) {
        self.answers
            .lock()
            .expect("the recorder is not poisoned")
            .push((intent, interaction, answer, activation));
    }

    async fn history(&self, page: HistoryPage) -> Result<HistoryChunk, KernelError> {
        self.pages
            .lock()
            .expect("the recorder is not poisoned")
            .push(page.clone());
        let mut chunk = HistoryChunk {
            items: Vec::new(),
            next: None,
            generation: 3,
            oversized: None,
        };
        let (Some(item), Some(budget)) = (&self.history_item, page.max_bytes) else {
            return Ok(chunk);
        };
        if page.generation.is_some_and(|generation| generation != 3) {
            return Err(KernelError::new(
                ErrorCode::StaleGeneration,
                "history changed",
            ));
        }
        if page.before.as_ref() == Some(&item.id) {
            return Ok(chunk);
        }
        let json = serde_json::to_string(item).expect("scripted item serializes");
        if json.len() > budget {
            chunk.next = Some(item.id.clone());
            chunk.oversized = Some(OversizedItem {
                id: item.id.clone(),
                total_bytes: json.len(),
                checksum: fnv1a64(json.as_bytes()),
            });
        } else {
            chunk.items.push(item.clone());
        }
        Ok(chunk)
    }

    async fn item_for_pin(&self, id: &ItemId, generation: u64) -> Result<Item, KernelError> {
        self.pin_reads
            .lock()
            .expect("pin recorder is not poisoned")
            .push((id.clone(), generation));
        if generation != 3 {
            return Err(KernelError::new(
                ErrorCode::StaleGeneration,
                "history changed",
            ));
        }
        let mut item = self
            .history_item
            .as_ref()
            .filter(|item| &item.id == id)
            .cloned()
            .ok_or_else(|| KernelError::new(ErrorCode::NotFound, "no scripted item"))?;
        if self.change_on_pin.load(Ordering::SeqCst) {
            let ItemBody::Assistant { text } = &mut item.body else {
                panic!("this fault fixture requires an assistant item")
            };
            assert!(text.starts_with('"'));
            // Both `"` and `\\` escape to two JSON bytes: id, generation and
            // serialized length stay identical while the FNV checksum changes.
            text.replace_range(..1, "\\");
        }
        Ok(item)
    }

    /// The journal replay: durable frames only, as the kernel's is.
    async fn events_since(&self, since: Seq) -> Result<FrameStream, KernelError> {
        Ok(self.stream(since, true))
    }
}

pub struct TestHost {
    session: Arc<TestSession>,
    snapshot: SessionState,
    summaries: Vec<SessionSummary>,
    gateway: Option<GatewayEvent>,
    /// What `session/list` answers with when the kernel is unhappy.
    refuse: Option<KernelError>,
}

impl TestHost {
    pub fn with(frames: Vec<Frame>) -> (HostHandle, Arc<TestSession>) {
        TestHost::build(frames, None, fresh_state(), vec![summary()])
    }

    pub fn with_snapshot(snapshot: SessionState) -> (HostHandle, Arc<TestSession>) {
        TestHost::build(Vec::new(), None, snapshot, vec![summary()])
    }

    pub fn with_summaries(summaries: Vec<SessionSummary>) -> (HostHandle, Arc<TestSession>) {
        TestHost::build(Vec::new(), None, fresh_state(), summaries)
    }

    pub fn with_gateway_summary(summary: SessionSummary) -> (HostHandle, Arc<TestSession>) {
        let session = Arc::new(TestSession::default());
        let host = TestHost {
            session: Arc::clone(&session),
            snapshot: fresh_state(),
            summaries: vec![summary.clone()],
            gateway: Some(GatewayEvent::SessionCreated {
                summary: Box::new(summary),
            }),
            refuse: None,
        };
        (HostHandle(Arc::new(host)), session)
    }

    pub fn with_history_item(item: Item) -> (HostHandle, Arc<TestSession>) {
        let session = Arc::new(TestSession {
            history_item: Some(item),
            ..Default::default()
        });
        let host = TestHost {
            session: Arc::clone(&session),
            snapshot: fresh_state(),
            summaries: vec![summary()],
            gateway: None,
            refuse: None,
        };
        (HostHandle(Arc::new(host)), session)
    }

    pub fn refusing(error: KernelError) -> HostHandle {
        TestHost::build(Vec::new(), Some(error), fresh_state(), vec![summary()]).0
    }

    fn build(
        frames: Vec<Frame>,
        refuse: Option<KernelError>,
        snapshot: SessionState,
        summaries: Vec<SessionSummary>,
    ) -> (HostHandle, Arc<TestSession>) {
        let session = Arc::new(TestSession {
            frames,
            ..Default::default()
        });
        let host = TestHost {
            session: Arc::clone(&session),
            snapshot,
            summaries,
            gateway: None,
            refuse,
        };
        (HostHandle(Arc::new(host)), session)
    }
}

#[async_trait]
impl HostApi for TestHost {
    async fn sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>, KernelError> {
        match &self.refuse {
            Some(error) => Err(error.clone()),
            None => Ok(self
                .summaries
                .iter()
                .filter(|summary| {
                    filter
                        .cwd
                        .as_ref()
                        .is_none_or(|cwd| cwd.as_path() == std::path::Path::new(&summary.cwd))
                        && filter.parent.as_ref().is_none_or(|parent| {
                            summary
                                .parent
                                .as_ref()
                                .is_some_and(|link| &link.session == parent)
                        })
                })
                .cloned()
                .collect()),
        }
    }

    async fn open(
        &self,
        _selector: SessionSelector,
        _who: ClientIdentity,
        options: OpenOptions,
    ) -> Result<Attachment, KernelError> {
        let own = self.session.stream(Seq::ZERO, false);
        let events: FrameStream = if options.children {
            Box::pin(own.chain(futures::stream::iter([child_head()])))
        } else {
            own
        };
        Ok(Attachment {
            session: session_id(),
            snapshot: self.snapshot.clone(),
            history: options.max_snapshot_bytes.map(|_| bingo_sdk::OpenHistory {
                before: None,
                has_more: self.session.history_item.is_some(),
                generation: if self.session.history_item.is_some() {
                    3
                } else {
                    self.snapshot.history_generation
                },
            }),
            events,
            handle: SessionHandle(Arc::clone(&self.session) as Arc<dyn SessionPort>),
        })
    }

    async fn close(&self, _session: &SessionId, _reason: CloseReason) -> Result<(), KernelError> {
        Ok(())
    }

    async fn delete(&self, _session: &SessionId) -> Result<(), KernelError> {
        Ok(())
    }

    async fn deliver(
        &self,
        _to: &SessionId,
        intent: IntentId,
        input: Input,
        delivery: Delivery,
    ) -> Result<(), KernelError> {
        self.session
            .delivered
            .lock()
            .expect("the recorder is not poisoned")
            .push((intent, input, delivery));
        Ok(())
    }

    async fn extend(
        &self,
        _session: &SessionId,
        plugin: &str,
        kind: &str,
        payload: Value,
    ) -> Result<(), KernelError> {
        self.session
            .extended
            .lock()
            .expect("the recorder is not poisoned")
            .push((plugin.to_string(), kind.to_string(), payload));
        Ok(())
    }

    async fn signal(
        &self,
        _session: &SessionId,
        plugin: &str,
        kind: &str,
        payload: Value,
    ) -> Result<(), KernelError> {
        self.session
            .signalled
            .lock()
            .expect("the recorder is not poisoned")
            .push((plugin.to_string(), kind.to_string(), payload));
        Ok(())
    }

    async fn catalog(&self, kind: CatalogKind) -> Result<Catalog, KernelError> {
        Ok(Catalog {
            kind,
            entries: vec![CatalogEntry {
                id: "fake".into(),
                label: "the fake provider".into(),
                meta: Value::Null,
            }],
        })
    }

    fn gateway_events(&self) -> GatewayStream {
        let event = self
            .gateway
            .clone()
            .unwrap_or(GatewayEvent::CatalogChanged {
                kind: CatalogKind::Tools,
            });
        Box::pin(futures::stream::iter([event]))
    }

    fn service_any(&self, _key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}

pub fn who() -> ClientIdentity {
    ClientIdentity {
        name: "test".into(),
        surface: "test".into(),
    }
}

pub fn selector() -> SessionSelector {
    SessionSelector::ById { id: session_id() }
}
