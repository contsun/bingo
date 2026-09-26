//! Connection-owned immutable references for bounded RPC transport.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bingo_sdk::{ErrorCode, ItemId, KernelError, SessionId};

use crate::methods::{
    MAX_PART_BYTES, MAX_PINNED_BYTES_PER_CONNECTION, MAX_PINNED_REFERENCES_PER_CONNECTION,
    ReferenceAvailability, SerializedPart,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Item,
    Field,
    Event,
}

#[derive(Clone, Default)]
pub(crate) struct Pins(Arc<Mutex<Store>>);

#[derive(Default)]
struct Store {
    bytes: usize,
    values: HashMap<String, Pinned>,
}

struct Pinned {
    session: SessionId,
    owner: Option<SessionId>,
    kind: Kind,
    bytes: String,
    generation: Option<u64>,
    item: Option<ItemId>,
}

impl Pins {
    pub(crate) fn can_admit(&self, bytes: usize) -> bool {
        let store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        store.values.len() < MAX_PINNED_REFERENCES_PER_CONNECTION
            && bytes <= MAX_PINNED_BYTES_PER_CONNECTION.saturating_sub(store.bytes)
    }

    pub(crate) fn add(
        &self,
        session: SessionId,
        owner: Option<SessionId>,
        kind: Kind,
        bytes: String,
        generation: Option<u64>,
        item: Option<ItemId>,
    ) -> ReferenceAvailability {
        let mut store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if store.values.len() >= MAX_PINNED_REFERENCES_PER_CONNECTION
            || bytes.len() > MAX_PINNED_BYTES_PER_CONNECTION.saturating_sub(store.bytes)
        {
            return ReferenceAvailability::Unavailable {
                reason: "pinBudgetExceeded".into(),
            };
        }
        let token = loop {
            let token = SessionId::mint().to_string();
            if !store.values.contains_key(&token) {
                break token;
            }
        };
        store.bytes += bytes.len();
        store.values.insert(
            token.clone(),
            Pinned {
                session,
                owner,
                kind,
                bytes,
                generation,
                item,
            },
        );
        ReferenceAvailability::Available { token }
    }

    pub(crate) fn remove_session(&self, session: &SessionId) {
        let mut store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        store
            .values
            .retain(|_, value| value.owner.as_ref().unwrap_or(&value.session) != session);
        store.bytes = store.values.values().map(|value| value.bytes.len()).sum();
    }

    pub(crate) fn owner_for(&self, session: &SessionId, token: &str) -> Option<SessionId> {
        let store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        store
            .values
            .get(token)
            .filter(|value| &value.session == session)
            .and_then(|value| value.owner.clone())
    }

    pub(crate) fn remove_token(&self, token: &str) {
        let mut store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(value) = store.values.remove(token) {
            store.bytes = store.bytes.saturating_sub(value.bytes.len());
        }
    }

    pub(crate) fn part(
        &self,
        session: &SessionId,
        token: &str,
        kind: Kind,
        item: Option<(&ItemId, u64)>,
        offset: usize,
        max_bytes: usize,
    ) -> Result<SerializedPart, KernelError> {
        if !(1..=MAX_PART_BYTES).contains(&max_bytes) {
            return Err(KernelError::new(
                ErrorCode::InvalidInput,
                "invalid part byte budget",
            ));
        }
        let store = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        let value = store
            .values
            .get(token)
            .filter(|value| &value.session == session && value.kind == kind)
            .ok_or_else(|| KernelError::new(ErrorCode::NotFound, "no pinned value for session"))?;
        if let Some((item_id, generation)) = item
            && (value.item.as_ref() != Some(item_id) || value.generation != Some(generation))
        {
            return Err(KernelError::new(
                ErrorCode::NotFound,
                "the item token does not match",
            ));
        }
        let bytes = &value.bytes;
        if offset >= bytes.len() || !bytes.is_char_boundary(offset) {
            return Err(KernelError::new(
                ErrorCode::InvalidInput,
                "invalid UTF-8 part offset",
            ));
        }
        let mut end = offset.saturating_add(max_bytes).min(bytes.len());
        while end > offset && !bytes.is_char_boundary(end) {
            end -= 1;
        }
        if end == offset {
            return Err(KernelError::new(
                ErrorCode::InvalidInput,
                "part budget cannot advance UTF-8",
            ));
        }
        Ok(SerializedPart {
            data: bytes[offset..end].to_owned(),
            next_offset: (end < bytes.len()).then_some(end),
            total_bytes: bytes.len(),
        })
    }
}

pub(crate) fn fnv1a64(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_exhaustion_never_mints_a_fake_available_token() {
        let pins = Pins::default();
        let session = SessionId::from_raw("ses_quota");
        for _ in 0..MAX_PINNED_REFERENCES_PER_CONNECTION {
            assert!(matches!(
                pins.add(session.clone(), None, Kind::Event, "{}".into(), None, None),
                ReferenceAvailability::Available { .. }
            ));
        }
        assert!(matches!(
            pins.add(session.clone(), None, Kind::Event, "{}".into(), None, None),
            ReferenceAvailability::Unavailable { .. }
        ));
        pins.remove_session(&session);
        assert!(matches!(
            pins.add(session, None, Kind::Event, "{}".into(), None, None),
            ReferenceAvailability::Available { .. }
        ));
    }

    #[test]
    fn child_event_pin_belongs_to_its_open_root_and_exact_session() {
        let pins = Pins::default();
        let root = SessionId::from_raw("ses_root");
        let child = SessionId::from_raw("ses_child");
        let ReferenceAvailability::Available { token } = pins.add(
            child.clone(),
            Some(root.clone()),
            Kind::Event,
            "{\"text\":\"🧪\"}".into(),
            Some(3),
            None,
        ) else {
            panic!("small event is pinned")
        };
        assert_eq!(pins.owner_for(&child, &token), Some(root.clone()));
        assert_eq!(
            pins.part(&child, &token, Kind::Event, None, 0, 12)
                .unwrap()
                .next_offset,
            Some(9),
            "UTF-8 boundary, not an accidental byte cut"
        );
        assert_eq!(
            pins.part(&root, &token, Kind::Event, None, 0, 12)
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        pins.remove_session(&child);
        assert_eq!(
            pins.owner_for(&child, &token),
            Some(root.clone()),
            "a direct child open does not invalidate its still-open tree reference"
        );
        pins.remove_session(&root);
        assert!(pins.owner_for(&child, &token).is_none());
        assert_eq!(
            pins.part(&child, &token, Kind::Event, None, 0, 12)
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
    }
}
