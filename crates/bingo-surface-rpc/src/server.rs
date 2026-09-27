//! `bingo serve --stdio`: the kernel's `HostApi` with an envelope.
//!
//! One task reads lines and dispatches them; one task writes. Every response
//! and every notification goes through the same channel, so their order on the
//! wire is the order they were produced and needs no other rule. A forwarder is
//! started only after its `session/open` reply is already queued, which is what
//! makes "the snapshot precedes the frames" true by construction (ADR-0007).

pub(crate) mod bounded;
mod discovery;
mod snapshot;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bingo_sdk::{
    Attachment, ClientIdentity, CloseReason, ErrorCode, Exit, FrameStream, HistoryChunk,
    HostHandle, KernelError, SessionFilter, SessionHandle, SessionId, SessionSummary,
};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodecError};

use crate::codec::{
    self, INVALID_PARAMS, INVALID_REQUEST, Id, KERNEL_ERROR, METHOD_NOT_FOUND, Message,
    PARSE_ERROR, Request, Response, RpcError,
};
use crate::methods::{
    AnswerParams, CatalogParams, ChildrenParams, ChildrenResult, DeliverParams, Empty, EventParams,
    EventsParams, ExtendParams, GatewaySubscribeParams, HeadField, HeadOmission, HistoryParams,
    HistoryResult, InitializeParams, InitializeResult, InterruptParams, ItemPartParams,
    ListHeadsParams, ListHeadsResult, ListParams, ListResult, MAX_BOUNDED_LINE_BYTES, OmittedField,
    OpenParams, OpenResult, PinnedPartParams, ReferenceAvailability, SessionParams, SignalParams,
    SubmitParams, SummaryHead, TreeSnapshot, WireOversizedItem, name,
};
use crate::session::{Forwarder, Pump};
use bounded::{Kind, Pins, fnv1a64};

/// Enough to absorb a burst of frames without letting a slow reader grow it
/// without bound; a full channel is backpressure on the session's forwarder.
const OUT_CAPACITY: usize = 256;

/// Serve one client until it says `shutdown` or the input ends.
pub async fn serve<R, W>(host: HostHandle, reader: R, writer: W) -> Result<Exit, KernelError>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (out, queue) = mpsc::channel(OUT_CAPACITY);
    let writing = tokio::spawn(write_lines(writer, queue));
    let mut server = Server::new(host, out);
    let read = server.read_lines(reader).await;
    // Drops the forwarders and the last sender, so the writer drains and ends.
    drop(server);
    let written = writing.await.map_err(|error| {
        KernelError::new(
            ErrorCode::Internal,
            format!("the rpc writer failed: {error}"),
        )
    })?;
    let exit = read?;
    written?;
    Ok(exit)
}

async fn write_lines<W>(writer: W, mut queue: Receiver<Message>) -> Result<(), KernelError>
where
    W: AsyncWrite + Unpin + Send,
{
    let mut sink = FramedWrite::new(writer, codec::lines());
    while let Some(message) = queue.recv().await {
        let line = serde_json::to_string(&message).map_err(|error| {
            KernelError::new(
                ErrorCode::Internal,
                format!("unserialisable reply: {error}"),
            )
        })?;
        sink.send(line).await.map_err(broken_pipe)?;
    }
    Ok(())
}

fn broken_pipe(error: LinesCodecError) -> KernelError {
    KernelError::new(
        ErrorCode::Internal,
        format!("the rpc transport failed: {error}"),
    )
}

/// What a method answers, and the subscription to start once it is queued.
struct Reply {
    result: Value,
    then: Option<Start>,
}

impl Reply {
    fn of<T: Serialize>(value: &T) -> Result<Reply, RpcError> {
        Ok(Reply {
            result: encode(value)?,
            then: None,
        })
    }

    fn empty() -> Result<Reply, RpcError> {
        Reply::of(&Empty {})
    }

    fn then(self, start: Start) -> Reply {
        Reply {
            then: Some(start),
            ..self
        }
    }
}

/// A stream that must not produce a notification before the reply is on the wire.
enum Start {
    Session {
        session: SessionId,
        events: FrameStream,
        handle: SessionHandle,
        /// A tree attachment: every frame is notified with this root.
        tree: bool,
        max_bytes: Option<usize>,
        generation: u64,
    },
    Gateway {
        events: bingo_sdk::GatewayStream,
        max_bytes: Option<usize>,
    },
}

struct Server {
    host: HostHandle,
    out: Sender<Message>,
    /// `Some` once `initialize` succeeded, and the identity every `open` carries:
    /// the handshake and who is asking are one fact.
    client: Option<ClientIdentity>,
    open: HashMap<SessionId, Forwarder>,
    pins: Pins,
    gateway: Option<Pump>,
    /// Set by `shutdown`; the loop stops once the reply is queued.
    exit: Option<Exit>,
}

impl Server {
    fn new(host: HostHandle, out: Sender<Message>) -> Self {
        Self {
            host,
            out,
            client: None,
            open: HashMap::new(),
            pins: Pins::default(),
            gateway: None,
            exit: None,
        }
    }

    async fn read_lines<R>(&mut self, reader: R) -> Result<Exit, KernelError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut lines = FramedRead::new(reader, codec::lines());
        while self.exit.is_none() {
            match lines.next().await {
                None => break,
                Some(Ok(line)) => self.line(line).await?,
                Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                    self.fail(None, RpcError::new(PARSE_ERROR, "the line is too long"))
                        .await?;
                }
                Some(Err(error)) => return Err(broken_pipe(error)),
            }
        }
        Ok(self.exit.unwrap_or(Exit { code: 0 }))
    }

    /// A line the client sent: bad JSON is -32700, a shape that is not JSON-RPC
    /// is -32600, and either way the server goes on.
    async fn line(&mut self, line: String) -> Result<(), KernelError> {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            return self
                .fail(None, RpcError::new(PARSE_ERROR, "the line is not json"))
                .await;
        };
        match serde_json::from_value::<Message>(value) {
            Ok(Message::Request(request)) => self.request(request).await,
            // A client sends no notifications and no responses; JSON-RPC says a
            // notification is never answered, so both are dropped.
            Ok(other) => {
                tracing::debug!(?other, "ignoring a message that is not a request");
                Ok(())
            }
            Err(error) => {
                self.fail(None, RpcError::new(INVALID_REQUEST, error.to_string()))
                    .await
            }
        }
    }

    async fn request(&mut self, request: Request) -> Result<(), KernelError> {
        let Request {
            id, method, params, ..
        } = request;
        if serde_json::to_vec(&id).is_ok_and(|bytes| bytes.len() > 1024) {
            return self
                .fail(
                    None,
                    RpcError::new(INVALID_REQUEST, "request id is too long"),
                )
                .await;
        }
        match self.dispatch(&method, params, &id).await {
            Ok(reply) => {
                self.send(Message::Response(Response::ok(id, reply.result)))
                    .await?;
                if let Some(start) = reply.then {
                    self.start(start);
                }
                Ok(())
            }
            Err(error) => self.fail(Some(id), error).await,
        }
    }

    async fn dispatch(&mut self, method: &str, params: Value, id: &Id) -> Result<Reply, RpcError> {
        if method != name::INITIALIZE && self.client.is_none() {
            return Err(
                KernelError::new(ErrorCode::NotInitialized, "call initialize first").into(),
            );
        }
        match method {
            name::INITIALIZE => self.initialize(params),
            name::SHUTDOWN => self.shutdown(params),
            name::SESSION_LIST => self.list(params).await,
            name::SESSION_LIST_HEADS => self.list_heads(params, id).await,
            name::SESSION_CHILDREN => self.children(params, id).await,
            name::SESSION_OPEN => self.open(params, id).await,
            name::SESSION_CLOSE => self.close(params).await,
            name::SESSION_DELETE => self.delete(params).await,
            name::SESSION_DELIVER => self.deliver(params).await,
            name::SESSION_EXTEND => self.extend(params).await,
            name::SESSION_SIGNAL => self.signal(params).await,
            name::SESSION_HISTORY => self.history(params, id).await,
            name::SESSION_ITEM_PART => self.item_part(params, id),
            name::SESSION_FIELD_PART => self.pinned_part(params, id, Kind::Field),
            name::SESSION_EVENT_PART => self.pinned_part(params, id, Kind::Event),
            name::SESSION_EVENTS => self.events(params).await,
            name::SESSION_SUBMIT => self.submit(params),
            name::SESSION_INTERRUPT => self.interrupt(params),
            name::SESSION_ANSWER => self.answer(params),
            name::CATALOG_READ => self.catalog(params).await,
            name::GATEWAY_SUBSCRIBE => self.subscribe(params),
            unknown => Err(RpcError::new(
                METHOD_NOT_FOUND,
                format!("no such method: {unknown}"),
            )),
        }
    }

    fn initialize(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: InitializeParams = parse(params)?;
        if self.client.is_some() {
            return Err(RpcError::new(INVALID_REQUEST, "already initialized"));
        }
        self.client = Some(params.client);
        Reply::of(&InitializeResult::current())
    }

    fn shutdown(&mut self, params: Value) -> Result<Reply, RpcError> {
        let Empty {} = parse(params)?;
        self.exit = Some(Exit { code: 0 });
        Reply::empty()
    }

    async fn list(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: ListParams = parse(params)?;
        let sessions = self.host.sessions(params.filter).await?;
        Reply::of(&ListResult { sessions })
    }

    async fn open(&mut self, params: Value, id: &Id) -> Result<Reply, RpcError> {
        let params: OpenParams = parse(params)?;
        if let Some(budget) = params.options.max_snapshot_bytes {
            validate_budget(budget)?;
        }
        if params.options.tree_backfill.is_some()
            && (!params.options.children || params.options.max_snapshot_bytes.is_none())
        {
            return Err(KernelError::new(
                ErrorCode::InvalidInput,
                "live-only tree requires bounded children attachment",
            )
            .into());
        }
        let who = self.who()?;
        let Attachment {
            session,
            snapshot,
            history,
            events,
            handle,
        } = self.host.open(params.selector, who, params.options).await?;
        self.pins.remove_session(&session);
        let generation = snapshot.history_generation;
        let result = OpenResult {
            session: session.clone(),
            snapshot,
            history,
            tree: params.options.tree_backfill.map(|backfill| TreeSnapshot {
                backfill,
                descendants_complete: false,
            }),
            omitted_fields: Vec::new(),
        };
        let reply = if let Some(budget) = params.options.max_snapshot_bytes {
            match snapshot::fit_open(id, &session, &result, budget, &self.pins) {
                Ok(result) => Reply { result, then: None },
                Err(error) => {
                    self.pins.remove_session(&session);
                    return Err(error);
                }
            }
        } else {
            Reply::of(&result)?
        };
        Ok(reply.then(Start::Session {
            session,
            events,
            handle,
            tree: params.options.children,
            max_bytes: params.options.max_snapshot_bytes,
            generation,
        }))
    }

    async fn close(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: SessionParams = parse(params)?;
        self.open.remove(&params.session);
        self.pins.remove_session(&params.session);
        self.host
            .close(&params.session, CloseReason::Client)
            .await?;
        Reply::empty()
    }

    async fn delete(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: SessionParams = parse(params)?;
        self.open.remove(&params.session);
        self.pins.remove_session(&params.session);
        self.host.delete(&params.session).await?;
        Reply::empty()
    }

    async fn deliver(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: DeliverParams = parse(params)?;
        self.host
            .deliver(
                &params.session,
                params.intent,
                params.input,
                params.delivery,
            )
            .await?;
        Reply::empty()
    }

    async fn extend(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: ExtendParams = parse(params)?;
        self.host
            .extend(
                &params.session,
                &params.plugin,
                &params.kind,
                params.payload,
            )
            .await?;
        Reply::empty()
    }

    async fn signal(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: SignalParams = parse(params)?;
        self.host
            .signal(
                &params.session,
                &params.plugin,
                &params.kind,
                params.payload,
            )
            .await?;
        Reply::empty()
    }

    async fn history(&mut self, params: Value, id: &Id) -> Result<Reply, RpcError> {
        let params: HistoryParams = parse(params)?;
        let budget = params.page.max_bytes;
        let mut page = params.page;
        if let Some(budget) = budget {
            validate_budget(budget)?;
            page.max_bytes = Some(budget.saturating_sub(512));
        }
        let handle = self.port(&params.session)?;
        let chunk = handle.history(page).await?;
        let result = self.pinned_history(&params.session, &handle, chunk).await?;
        if let Some(budget) = budget
            && response_len(id, &result)? > budget
        {
            if let Some(WireOversizedItem {
                availability: ReferenceAvailability::Available { token },
                ..
            }) = &result.oversized
            {
                self.pins.remove_token(token);
            }
            return Err(protocol_limit("the history reply cannot fit this page"));
        }
        Reply::of(&result)
    }

    async fn pinned_history(
        &self,
        session: &SessionId,
        handle: &SessionHandle,
        chunk: HistoryChunk,
    ) -> Result<HistoryResult, RpcError> {
        let oversized = match chunk.oversized {
            Some(marker) => {
                let availability = if !self.pins.can_admit(marker.total_bytes) {
                    ReferenceAvailability::Unavailable {
                        reason: "pinBudgetExceeded".into(),
                    }
                } else {
                    let item = handle.item_for_pin(&marker.id, chunk.generation).await?;
                    let raw = serde_json::to_string(&item).map_err(|error| {
                        RpcError::new(KERNEL_ERROR, format!("unserialisable item: {error}"))
                    })?;
                    if item.id != marker.id
                        || raw.len() != marker.total_bytes
                        || fnv1a64(raw.as_bytes()) != marker.checksum
                    {
                        return Err(KernelError::new(
                            ErrorCode::StaleGeneration,
                            "item changed before its history reference was pinned",
                        )
                        .into());
                    }
                    self.pins.add(
                        session.clone(),
                        None,
                        Kind::Item,
                        raw,
                        Some(chunk.generation),
                        Some(marker.id.clone()),
                    )
                };
                Some(WireOversizedItem {
                    id: marker.id,
                    total_bytes: marker.total_bytes,
                    checksum: marker.checksum,
                    availability,
                })
            }
            None => None,
        };
        Ok(HistoryResult {
            items: chunk.items,
            next: chunk.next,
            generation: chunk.generation,
            oversized,
        })
    }

    fn item_part(&self, params: Value, id: &Id) -> Result<Reply, RpcError> {
        let params: ItemPartParams = parse(params)?;
        self.port(&params.session)?;
        if params.token.is_empty() {
            return Err(protocol_limit("this item is not available"));
        }
        let part = self.pins.part(
            &params.session,
            &params.token,
            Kind::Item,
            Some((&params.item, params.generation)),
            params.offset,
            params.max_bytes,
        )?;
        self.part_reply(id, &part)
    }

    fn pinned_part(&self, params: Value, id: &Id, kind: Kind) -> Result<Reply, RpcError> {
        let params: PinnedPartParams = parse(params)?;
        if self.port(&params.session).is_err() {
            let owner = self
                .pins
                .owner_for(&params.session, &params.token)
                .ok_or_else(|| {
                    KernelError::new(ErrorCode::SessionNotFound, "the source tree is not open")
                })?;
            self.port(&owner)?;
        }
        if params.token.is_empty() {
            return Err(protocol_limit("this field is not available"));
        }
        let part = self.pins.part(
            &params.session,
            &params.token,
            kind,
            None,
            params.offset,
            params.max_bytes,
        )?;
        self.part_reply(id, &part)
    }

    fn part_reply(
        &self,
        id: &Id,
        part: &crate::methods::SerializedPart,
    ) -> Result<Reply, RpcError> {
        if response_len(id, part)? >= 16 * 1024 * 1024 {
            return Err(protocol_limit("part reply exceeds the transport line"));
        }
        Reply::of(part)
    }

    /// Resync: the frames after `since`, then live, on a forwarder that replaces
    /// the one this session already had.
    async fn events(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: EventsParams = parse(params)?;
        let forwarder = self
            .open
            .get(&params.session)
            .ok_or_else(|| KernelError::new(ErrorCode::SessionNotFound, "session is not open"))?;
        let handle = forwarder.handle.clone();
        let max_bytes = forwarder.max_bytes;
        let generation = forwarder.generation.load(Ordering::Acquire);
        let events = handle.events_since(params.since).await?;
        Ok(Reply::empty()?.then(Start::Session {
            session: params.session,
            events,
            handle,
            tree: false,
            max_bytes,
            generation,
        }))
    }

    fn submit(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: SubmitParams = parse(params)?;
        self.port(&params.session)?
            .submit(params.intent, params.input);
        Reply::empty()
    }

    fn interrupt(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: InterruptParams = parse(params)?;
        self.port(&params.session)?
            .interrupt(params.intent, params.scope);
        Reply::empty()
    }

    fn answer(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: AnswerParams = parse(params)?;
        self.port(&params.session)?.answer(
            params.intent,
            params.interaction,
            params.answer,
            params.activation,
        );
        Reply::empty()
    }

    async fn catalog(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: CatalogParams = parse(params)?;
        let catalog = self.host.catalog(params.kind).await?;
        Reply::of(&catalog)
    }

    fn subscribe(&mut self, params: Value) -> Result<Reply, RpcError> {
        let params: GatewaySubscribeParams = parse(params)?;
        if let Some(budget) = params.max_bytes {
            validate_budget(budget)?;
        }
        if self.gateway.is_some() {
            return Reply::empty();
        }
        let events = self.host.gateway_events();
        Ok(Reply::empty()?.then(Start::Gateway {
            events,
            max_bytes: params.max_bytes,
        }))
    }

    /// After the reply is queued, never before.
    fn start(&mut self, start: Start) {
        match start {
            Start::Session {
                session,
                events,
                handle,
                tree,
                max_bytes,
                generation,
            } => {
                let root = tree.then(|| session.clone());
                let events = events.map(move |frame| EventParams {
                    frame,
                    root: root.clone(),
                });
                let generation = Arc::new(AtomicU64::new(generation));
                let pump = Pump::session(
                    events,
                    self.out.clone(),
                    self.pins.clone(),
                    max_bytes,
                    Arc::clone(&generation),
                );
                // Replacing drops the old forwarder, which stops its task.
                self.open
                    .insert(session, Forwarder::new(handle, pump, max_bytes, generation));
            }
            Start::Gateway { events, max_bytes } => {
                self.gateway = Some(Pump::gateway(events, self.out.clone(), max_bytes));
            }
        }
    }

    /// A write reaches an actor only through a session this client has open.
    fn port(&self, session: &SessionId) -> Result<SessionHandle, RpcError> {
        self.open
            .get(session)
            .map(|forwarder| forwarder.handle.clone())
            .ok_or_else(|| {
                KernelError::new(
                    ErrorCode::SessionNotFound,
                    format!("session {session} is not open on this connection"),
                )
                .into()
            })
    }

    fn who(&self) -> Result<ClientIdentity, RpcError> {
        self.client.clone().ok_or_else(|| {
            KernelError::new(ErrorCode::NotInitialized, "call initialize first").into()
        })
    }

    async fn send(&self, message: Message) -> Result<(), KernelError> {
        self.out
            .send(message)
            .await
            .map_err(|_| KernelError::new(ErrorCode::Internal, "the rpc writer stopped"))
    }

    async fn fail(&self, id: Option<Id>, error: RpcError) -> Result<(), KernelError> {
        self.send(Message::Response(Response::failed(id, error)))
            .await
    }
}

/// Absent params are an empty object, so a method whose params are all optional
/// can be called without them.
fn parse<T: DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    let params = if params.is_null() {
        Value::Object(serde_json::Map::new())
    } else {
        params
    };
    serde_json::from_value(params).map_err(|error| RpcError::new(INVALID_PARAMS, error.to_string()))
}

fn encode<T: Serialize>(value: &T) -> Result<Value, RpcError> {
    serde_json::to_value(value)
        .map_err(|error| RpcError::new(KERNEL_ERROR, format!("unserialisable result: {error}")))
}

fn response_len<T: Serialize>(id: &Id, value: &T) -> Result<usize, RpcError> {
    let message = Message::Response(Response::ok(id.clone(), encode(value)?));
    serde_json::to_vec(&message)
        .map(|line| line.len())
        .map_err(|error| RpcError::new(KERNEL_ERROR, format!("unserialisable reply: {error}")))
}

fn protocol_limit(message: &str) -> RpcError {
    KernelError::new(ErrorCode::ProtocolLimit, message).into()
}

fn validate_budget(budget: usize) -> Result<(), RpcError> {
    if !(1024..=MAX_BOUNDED_LINE_BYTES).contains(&budget) {
        return Err(
            KernelError::new(ErrorCode::InvalidInput, "invalid bounded line budget").into(),
        );
    }
    Ok(())
}
