//! The methods and notifications (ADR-0007): their names and
//! the shape of what goes in and comes back. Every params and result type is an
//! sdk type or a struct of sdk types.
//!
//! `METHODS` and `NOTIFICATIONS` are the one table; the dispatcher matches on
//! the names in [`name`], the schema walks the table, and `initialize` reports
//! it as the server's capabilities.

use std::path::PathBuf;

use bingo_sdk::{
    Activation, Answer, Catalog, CatalogKind, ClientIdentity, Delivery, Driver, Frame,
    GatewayEvent, HistoryPage, Input, IntentId, InteractionId, InterruptScope, Item, ItemId,
    OpenHistory, OpenOptions, ParentLink, Seq, SessionFilter, SessionId, SessionSelector,
    SessionState, SessionSummary,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The wire version a client checks against its own before it speaks.
pub const PROTOCOL: u32 = 1;

/// Opt-in byte budgets concern *whole serialized JSON-RPC lines*, not the
/// unescaped content or a Rust item's heap size.
pub const MAX_BOUNDED_LINE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PART_BYTES: usize = 256 * 1024;
pub const MAX_PINNED_BYTES_PER_CONNECTION: usize = 128 * 1024 * 1024;
pub const MAX_PINNED_REFERENCES_PER_CONNECTION: usize = 32;

/// What `initialize` calls the far side. A client talks to bingo, not to a crate.
pub const SERVER_NAME: &str = "bingo";

/// Every name that travels on the wire, in one place.
pub mod name {
    pub const INITIALIZE: &str = "initialize";
    pub const SHUTDOWN: &str = "shutdown";
    pub const SESSION_LIST: &str = "session/list";
    pub const SESSION_LIST_HEADS: &str = "session/listHeads";
    pub const SESSION_CHILDREN: &str = "session/children";
    pub const SESSION_OPEN: &str = "session/open";
    pub const SESSION_CLOSE: &str = "session/close";
    pub const SESSION_DELETE: &str = "session/delete";
    pub const SESSION_HISTORY: &str = "session/history";
    pub const SESSION_ITEM_PART: &str = "session/itemPart";
    pub const SESSION_FIELD_PART: &str = "session/fieldPart";
    pub const SESSION_EVENT_PART: &str = "session/eventPart";
    pub const SESSION_EVENTS: &str = "session/events";
    pub const SESSION_SUBMIT: &str = "session/submit";
    pub const SESSION_INTERRUPT: &str = "session/interrupt";
    pub const SESSION_ANSWER: &str = "session/answer";
    pub const SESSION_DELIVER: &str = "session/deliver";
    pub const SESSION_EXTEND: &str = "session/extend";
    pub const SESSION_SIGNAL: &str = "session/signal";
    pub const CATALOG_READ: &str = "catalog/read";
    pub const GATEWAY_SUBSCRIBE: &str = "gateway/subscribe";

    /// One session frame, verbatim.
    pub const EVENT: &str = "event";
    /// A bounded, explicitly incomplete reference to a large event.
    pub const EVENT_REF: &str = "eventRef";
    /// One host-wide event, verbatim.
    pub const GATEWAY_EVENT: &str = "gateway/event";
    /// An oversized created summary invalidates the bounded head list by id.
    pub const GATEWAY_SESSION_HEAD: &str = "gateway/sessionHead";
}

/// A method that answers nothing, and a method that asks for nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Empty {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub client: ClientIdentity,
    pub protocol: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol: u32,
    pub name: String,
    pub version: String,
    pub capabilities: Capabilities,
}

impl InitializeResult {
    /// What this build answers with.
    pub fn current() -> Self {
        Self {
            protocol: PROTOCOL,
            name: SERVER_NAME.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: Capabilities {
                methods: METHODS.iter().map(|method| method.0.to_owned()).collect(),
                notifications: NOTIFICATIONS
                    .iter()
                    .map(|notification| notification.0.to_owned())
                    .collect(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub methods: Vec<String>,
    pub notifications: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListParams {
    #[serde(default)]
    pub filter: SessionFilter,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListResult {
    pub sessions: Vec<SessionSummary>,
}

/// A safe, bounded summary projection for workspace ownership checks and
/// sidebar discovery. A missing key is *not* a keyless session when an
/// omission names `key`; callers must direct-open by trusted id to classify.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SummaryHead {
    pub id: SessionId,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentLink>,
    pub driver: Driver,
    pub created_at: String,
    pub updated_at: String,
    pub busy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted: Vec<HeadOmission>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum HeadField {
    Key,
    Title,
    Model,
    Provider,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeadOmission {
    pub field: HeadField,
    /// Original scalar string's UTF-8 byte length (no JSON quotes).
    pub total_bytes: usize,
    /// `openSessionToReadField`: direct-open a verified id and use its pinned
    /// snapshot field reference. A list never pins hundreds of large titles.
    pub reason: String,
}

/// `SessionFilter.limit` is deliberately absent: pagination walks every
/// matched id. The caller may stop displaying after its own chosen count,
/// but must not mistake that for the store being exhausted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeadFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<SessionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListHeadsParams {
    #[serde(default)]
    pub filter: HeadFilter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<SessionId>,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListHeadsResult {
    pub heads: Vec<SummaryHead>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<SessionId>,
}

/// Discover a tree's old children after a live-only attachment without
/// serializing unbounded child summaries or silently omitting descendants.
/// Results are sorted by stable id, `after` is exclusive, and `next` is the
/// last included id when more remain. An empty result terminates traversal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChildrenParams {
    pub parent: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<SessionId>,
    /// Budget for the entire JSON-RPC response. Required on this opt-in method.
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChildrenResult {
    pub children: Vec<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<SessionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenParams {
    pub selector: SessionSelector,
    #[serde(default)]
    pub options: OpenOptions,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenResult {
    pub session: SessionId,
    /// A true atomic cut. On bounded opens the transcript is only a preview;
    /// omitted fields are explicit, and trusted id/cwd/seq/status remain here.
    pub snapshot: SessionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<OpenHistory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tree: Option<TreeSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted_fields: Vec<OmittedField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TreeSnapshot {
    pub backfill: bingo_sdk::TreeBackfill,
    /// A tree attachment with no old descendant replay is never a complete
    /// history projection; discover children by parent and open them directly.
    pub descendants_complete: bool,
}

/// A ref-aware RPC client sees either a token backed by a real immutable pin
/// or an explicit resource-limit reason. The strict variants make generated
/// Zod validators reject both a token and a reason in one value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ReferenceAvailability {
    Available { token: String },
    Unavailable { reason: String },
}

/// The exact serialized JSON field value is pinned on this RPC connection.
/// The token is admitted only for its session and expires on close/reopen.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OmittedField {
    pub path: Vec<String>,
    pub availability: ReferenceAvailability,
    pub total_bytes: usize,
    /// FNV-1a 64 of the serialized UTF-8 value, lowercase 16-digit hex.
    pub checksum: String,
}

/// `session/close` and `session/delete`: a session and nothing else.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionParams {
    pub session: SessionId,
}

/// `HostApi::deliver` (ADR-0011 §3): a peer's prose into a session's queue.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeliverParams {
    pub session: SessionId,
    pub intent: IntentId,
    pub input: Input,
    pub delivery: Delivery,
}

/// `HostApi::extend` (ADR-0011 §2): a plugin's state into a session's journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExtendParams {
    pub session: SessionId,
    pub plugin: String,
    pub kind: String,
    pub payload: Value,
}

/// `HostApi::signal` (ADR-0013 §2): a plugin's live state onto a session's stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignalParams {
    pub session: SessionId,
    pub plugin: String,
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistoryParams {
    pub session: SessionId,
    #[serde(default)]
    pub page: HistoryPage,
}

/// The SDK's history facts, with connection-scoped availability added only
/// after the RPC server has admitted and pinned the exact oversized item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistoryResult {
    pub items: Vec<Item>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<ItemId>,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oversized: Option<WireOversizedItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WireOversizedItem {
    pub id: ItemId,
    pub total_bytes: usize,
    pub checksum: String,
    pub availability: ReferenceAvailability,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SerializedPart {
    /// UTF-8 JSON slice; offsets count bytes in the original unescaped JSON.
    pub data: String,
    pub next_offset: Option<usize>,
    pub total_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ItemPartParams {
    pub session: SessionId,
    pub item: ItemId,
    pub generation: u64,
    pub token: String,
    pub offset: usize,
    pub max_bytes: usize,
}

/// A connection-local pinned value is never accessible from a different
/// session, even when a caller learns its token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PinnedPartParams {
    pub session: SessionId,
    pub token: String,
    pub offset: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventsParams {
    pub session: SessionId,
    /// Frames after this one are re-sent, then the stream goes live.
    #[serde(default)]
    pub since: Seq,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubmitParams {
    pub session: SessionId,
    /// Minted by the client; also the idempotency key.
    pub intent: IntentId,
    pub input: Input,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterruptParams {
    pub session: SessionId,
    pub intent: IntentId,
    pub scope: InterruptScope,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnswerParams {
    pub session: SessionId,
    pub intent: IntentId,
    pub interaction: InteractionId,
    pub answer: Answer,
    pub activation: Activation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogParams {
    pub kind: CatalogKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewaySubscribeParams {
    /// Absent keeps legacy verbatim `gateway/event`. A bounded subscriber
    /// receives `gateway/sessionHead` for oversized SessionCreated summaries
    /// and refreshes via `session/listHeads` before trusting the new id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewaySessionHeadParams {
    pub session: SessionId,
}

/// Names a type in the schema: adds it to `$defs` and answers with its `$ref`.
pub type Ref = fn(&mut SchemaGenerator) -> Schema;

pub fn schema_of<T: JsonSchema>(generator: &mut SchemaGenerator) -> Schema {
    generator.subschema_for::<T>()
}

/// A method: its name, its params, its result.
pub type Method = (&'static str, Ref, Ref);

/// `event`: one frame, and — under a tree attachment (ADR-0010 §3) — the root
/// it was opened through, which is the stream the client routes it to. A
/// frame of the root itself carries no `root`, so the line is the frame verbatim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventParams {
    #[serde(flatten)]
    pub frame: Frame,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<SessionId>,
}

impl EventParams {
    /// Where the client routes it: the root of a tree attachment, else the frame's own session.
    pub fn route(&self) -> &SessionId {
        self.root.as_ref().unwrap_or(&self.frame.session)
    }
}

/// Opt-in transport reference only, never a synthetic SDK event. The consumer
/// records an incomplete seq and does not claim the missing event was applied.
/// Later small events may still arrive; explicitly requesting `eventPart`
/// yields the *original* serialized EventParams for viewing or export.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventRefParams {
    pub session: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<SessionId>,
    pub seq: Seq,
    pub message_id: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction: Option<InteractionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<IntentId>,
    /// A missing permission, interaction, queue or lifecycle frame may affect
    /// safe actions; the host must not assume its prior state is still certain.
    pub state_uncertain: bool,
    pub generation: u64,
    pub availability: ReferenceAvailability,
    pub total_bytes: usize,
    /// FNV-1a 64 of the original EventParams JSON UTF-8, lowercase hex.
    pub checksum: String,
}

/// A notification: its name and its params.
pub type Notification = (&'static str, Ref);

pub static METHODS: &[Method] = &[
    (
        name::INITIALIZE,
        schema_of::<InitializeParams>,
        schema_of::<InitializeResult>,
    ),
    (name::SHUTDOWN, schema_of::<Empty>, schema_of::<Empty>),
    (
        name::SESSION_LIST,
        schema_of::<ListParams>,
        schema_of::<ListResult>,
    ),
    (
        name::SESSION_LIST_HEADS,
        schema_of::<ListHeadsParams>,
        schema_of::<ListHeadsResult>,
    ),
    (
        name::SESSION_CHILDREN,
        schema_of::<ChildrenParams>,
        schema_of::<ChildrenResult>,
    ),
    (
        name::SESSION_OPEN,
        schema_of::<OpenParams>,
        schema_of::<OpenResult>,
    ),
    (
        name::SESSION_CLOSE,
        schema_of::<SessionParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_DELETE,
        schema_of::<SessionParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_DELIVER,
        schema_of::<DeliverParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_EXTEND,
        schema_of::<ExtendParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_SIGNAL,
        schema_of::<SignalParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_HISTORY,
        schema_of::<HistoryParams>,
        schema_of::<HistoryResult>,
    ),
    (
        name::SESSION_ITEM_PART,
        schema_of::<ItemPartParams>,
        schema_of::<SerializedPart>,
    ),
    (
        name::SESSION_FIELD_PART,
        schema_of::<PinnedPartParams>,
        schema_of::<SerializedPart>,
    ),
    (
        name::SESSION_EVENT_PART,
        schema_of::<PinnedPartParams>,
        schema_of::<SerializedPart>,
    ),
    (
        name::SESSION_EVENTS,
        schema_of::<EventsParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_SUBMIT,
        schema_of::<SubmitParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_INTERRUPT,
        schema_of::<InterruptParams>,
        schema_of::<Empty>,
    ),
    (
        name::SESSION_ANSWER,
        schema_of::<AnswerParams>,
        schema_of::<Empty>,
    ),
    (
        name::CATALOG_READ,
        schema_of::<CatalogParams>,
        schema_of::<Catalog>,
    ),
    (
        name::GATEWAY_SUBSCRIBE,
        schema_of::<GatewaySubscribeParams>,
        schema_of::<Empty>,
    ),
];

pub static NOTIFICATIONS: &[Notification] = &[
    (name::EVENT, schema_of::<EventParams>),
    (name::EVENT_REF, schema_of::<EventRefParams>),
    (name::GATEWAY_EVENT, schema_of::<GatewayEvent>),
    (
        name::GATEWAY_SESSION_HEAD,
        schema_of::<GatewaySessionHeadParams>,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_has_explicit_methods_and_notifications() {
        assert_eq!(METHODS.len(), 21);
        assert_eq!(NOTIFICATIONS.len(), 4);
    }

    #[test]
    fn no_name_is_used_twice() {
        let mut names: Vec<&str> = METHODS
            .iter()
            .map(|method| method.0)
            .chain(NOTIFICATIONS.iter().map(|notification| notification.0))
            .collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total);
    }

    #[test]
    fn initialize_reports_the_table_it_dispatches_from() {
        let result = InitializeResult::current();
        assert_eq!(result.protocol, PROTOCOL);
        assert_eq!(result.capabilities.methods.len(), METHODS.len());
        assert!(
            result
                .capabilities
                .notifications
                .contains(&name::EVENT.to_owned())
        );
    }
}
