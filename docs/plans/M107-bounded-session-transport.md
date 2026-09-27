# M107 — Bounded session transport

## Goal

A ref-aware RPC client can discover, open, and page a large session without
receiving an oversized NDJSON line or automatically transferring its entire
transcript. It can explicitly retrieve the exact original bytes of a large
item, non-item snapshot field, or live event. No journal is rewritten, no
completion is invented, and legacy clients retain their unbounded semantics.
The independently consumed SDK/RPC schema and failing wire fixtures precede
implementation. This plan covers the Core producer, not Rei's IPC or UI.

## Bricks, in build order

1. Apply and verify the independently reviewed SDK/RPC contract and its RED
   wire fixtures. Repair Core-owned struct literals without changing old
   default behavior. Keep the Core actor free of plugin knowledge.
2. At an actor mailbox cut, subscribe after the current sequence and clone
   only a budgeted tail of items into a bounded snapshot. Preserve real
   summary, status, and generation. Page older items by count and serialized
   byte budget; an oversized single item yields an explicit factual marker
   and progressing exclusive cursor. Reject stale generations. Fetch one
   immutable item by id/generation for an RPC pin, never repeatedly serialize
   a mutable item for successive parts.
3. On an opt-in tree attachment, atomically subscribe each live descendant
   at its current cut rather than replaying its old journal. Mark descendant
   projections incomplete; do not silently treat stored descendants as
   complete. Default tree replay remains unchanged.
4. At the RPC connection, construct a bounded open safety shell with exact
   id/cwd/seq and explicit omitted non-item fields. Admit immutable serialized
   fields and oversized items into a per-connection, per-session pin store
   only within the agreed byte/reference quota. Issue an available token only
   after successful admission and verification; otherwise expose an explicit
   unavailable reason. Close, reopen, and disconnect release their pins.
5. Send a bounded event reference instead of an oversized opt-in event, while
   retaining the original serialized notification only within the connection
   budget. Later small lifecycle frames continue in order. Serve explicit
   item/field/event parts from pinned bytes with UTF-8-aligned offsets,
   independent authorization, and whole-response line checks. Do not put a
   transport-only placeholder into the SDK's canonical Event stream.
6. Discover children and safe session heads in stable id order with exclusive
   byte-bounded cursors and a terminal empty page. Never feed an old list
   limit into the pre-pagination host/store lookup. On an opt-in gateway
   subscription, replace a giant created summary with bounded id-level
   invalidation; default list, gateway, and RPC notifications stay verbatim.
7. Check the final serialized JSON-RPC line, including echoed id and escaping,
   before every bounded response/notification. Invalid budgets, zero-progress
   pages, expired tokens, quota failures, and unrepresentable safety shells
   fail explicitly without stopping the host. Preserve old Rust RemoteKernel
   fail-closed opt-in behavior and unmodified default TUI/print semantics.

## Files

- `crates/bingo-core/src/session.rs`, `session/mailbox.rs`, focused session tests
- `crates/bingo-core/src/host.rs`, `host/tree.rs`, focused host/tree tests
- `crates/bingo-surface-rpc/src/server.rs`, `session.rs`, `codec.rs`, focused
  RPC wire tests in the runtime owner's scope
- The reviewed SDK/RPC contract, schema, and independent fixtures arrive as
  a separate contract-owner patch; do not edit their owned files here.

## Exit criteria

- [x] Contract patch applies to the isolated worktree; independent parser and
      default-compatibility tests are green, and producer RED is observed
      before behavioral changes.
- [x] A synthetic 17+ MiB item, huge non-item field, and >32 MiB live event
      open as bounded, truthful references without automatically transferring
      their bodies; explicit parts reconstruct byte-for-byte and verify length
      and FNV-1a64. One oversized item causes exactly one actor pin fetch.
- [x] A same-generation item change before pinning returns a visible stale
      error without issuing a token; quota exhaustion gives an unavailable
      reference, and subsequent small lifecycle events still arrive.
- [x] Bounded root and tree openings have an atomic cut, honest history and
      descendant gaps, discoverable paginated children, and no duplicate or
      skipped items across valid cursors. Giant titles/keys and accumulated
      summaries cannot break pre-open head listing or gateway discovery.
- [x] Invalid/too-small budgets, UTF-8 offset errors, long request ids,
      expired/foreign tokens, zero-progress pages, and generation changes
      neither leak original content nor exceed the final NDJSON line limit.
- [x] Old default TUI/print/RPC behavior and exact journal data remain intact;
      contract wire fixtures, focused Core/RPC tests, formatting, workspace
      check/clippy/tests, discipline, budget, and cargo deny are reported with
      their actual outcomes. No production binary or user journal is changed.

## Non-goals

- Bounded cold replay from persistent storage: resume still reads and folds
  the complete journal before the actor exists. Do not skip old records.
- Rei's trusted Main-to-renderer ACK stream, UI unloaded ledger, and
  user-authorized export destination; those belong to the separate client
  implementation and acceptance tests.
- Automatic rendering or transfer of arbitrary unbounded payloads. Above the
  finite pin quota, communicate unavailable content rather than pretending it
  was loaded.
- Raising the 16 MiB codec limit, changing legacy response shapes, or
  introducing dependencies solely for a checksum.

## Risks

- A mutable item can change between history metadata and pin admission;
  compare identity, generation, serialized length, and checksum before a
  token is issued. A missing frame is never silently folded as a real Event.
- At 128 MiB per connection, eight live hosts can pin about 1 GiB beyond
  their actors' state. Enforce both byte and reference caps, account for
  serialization copies, and release pins promptly.
- A 1 KiB requested line budget may not fit a true identity shell; reject it
  with `PROTOCOL_LIMIT` rather than altering the session's cwd or id. One
  child/head too large for a page must fail rather than repeat its cursor.
- A gateway discovery or a new child can race a paginated enumeration. The
  client must merge live invalidations with an incomplete-tree marker; the
  Core producer must preserve the atomic root/descendant stream boundary.
- On this machine, cargo may need the release Xcode and an unset stale
  `RUSTC_WRAPPER`; build into an isolated target, not the user's live binary.

## Verified (2026-09-23)

All Cargo commands used `env -u RUSTC_WRAPPER`, release Xcode via
`DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer`, and isolated
`CARGO_TARGET_DIR=/tmp/rei-bounded-core-target-20260923`.

- Before implementation, `cargo test -p bingo-surface-rpc --test wire bounded
  --locked`: 4 contract/parser tests passed, 12 producer tests failed, one
  100 MiB stress test ignored. Afterward: 16 passed, 0 failed, one ignored.
- Explicit `hundred_megabyte_event_is_not_pushed_on_open -- --ignored`:
  1 passed, 0 failed. New Core retry-dropped same-generation cursor and
  Rewound-generation regressions pass; RPC root-owned child pin, escaped
  four-field head, quota and reference-generation unit tests pass.
- `cargo fmt --all -- --check`, `cargo check --workspace --all-targets
  --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo test --workspace --locked --no-fail-fast -- --quiet`: exit 0.
  Core lib: 399 passed; RPC wire: 39 passed, 1 intentionally ignored;
  plugin RPC lib: 134 passed after its SDK-derived schema was regenerated.
- `cargo check -p bingo-core -p bingo-surface-rpc --all-targets --locked
  --target x86_64-pc-windows-msvc`: exit 0.
- `scripts/check_discipline.sh`: `discipline ok`, existing and Core session
  file-size warnings; no new function-size warning. `cargo deny check`:
  advisories/bans/licenses/sources ok; existing duplicate/license warnings.
- Unmodified `scripts/budget.sh` ran from source copy
  `/tmp/rei-bounded-budget-copy-20260923` to avoid touching the live tree's
  TUI; target symlinked to the isolated build. `budget ok`: deps 342/342,
  warm Core check 1s/20s, TUI touch recompiled 0 Core crates. Its isolated
  `target/debug` was 9GB against a 5GB **soft** maximum: warning, not pass
  without caveat. No shared target was cleaned or user journal read.
