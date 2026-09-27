//! Exact omitted-field references around a small, trustworthy open response.

use super::*;

pub(super) fn fit_open(
    id: &Id,
    session: &SessionId,
    result: &OpenResult,
    budget: usize,
    pins: &Pins,
) -> Result<Value, RpcError> {
    let mut wire = encode(result)?;
    while response_len(id, &wire)? > budget {
        let Some((path, _)) = largest_field(&wire["snapshot"])? else {
            if trim_oldest(&mut wire) {
                continue;
            }
            pins.remove_session(session);
            return Err(protocol_limit(
                "the true session identity cannot fit this line budget",
            ));
        };
        omit(&mut wire, session, path, pins)?;
    }
    Ok(wire)
}

fn largest_field(snapshot: &Value) -> Result<Option<(Vec<String>, usize)>, RpcError> {
    let mut candidates = Vec::new();
    for key in ["key", "title", "model", "provider", "systemExtra", "tools"] {
        candidates.push(vec!["summary".into(), key.into()]);
    }
    for key in [
        "config",
        "queue",
        "interactions",
        "context",
        "lastTurn",
        "turn",
    ] {
        candidates.push(vec![key.into()]);
    }
    for section in ["extensions", "signals"] {
        if let Some(plugins) = snapshot.get(section).and_then(Value::as_object) {
            for (plugin, kinds) in plugins {
                if let Some(kinds) = kinds.as_object() {
                    for kind in kinds.keys() {
                        candidates.push(vec![section.into(), plugin.clone(), kind.clone()]);
                    }
                }
            }
        }
    }
    candidates
        .into_iter()
        .filter_map(|path| at_path(snapshot, &path).map(|value| (path, value)))
        .map(|(path, value)| {
            serde_json::to_vec(value)
                .map(|raw| (path, raw.len()))
                .map_err(|error| {
                    RpcError::new(
                        KERNEL_ERROR,
                        format!("unserialisable snapshot field: {error}"),
                    )
                })
        })
        .max_by_key(|result| result.as_ref().map_or(0, |(_, size)| *size))
        .transpose()
}

fn at_path<'a>(mut value: &'a Value, path: &[String]) -> Option<&'a Value> {
    for segment in path {
        value = value.get(segment)?;
    }
    Some(value)
}

fn omit(
    wire: &mut Value,
    session: &SessionId,
    path: Vec<String>,
    pins: &Pins,
) -> Result<(), RpcError> {
    let Some(field) = take_path(&mut wire["snapshot"], &path) else {
        return Err(protocol_limit("a snapshot field disappeared while fitting"));
    };
    let bytes = serde_json::to_string(&field).map_err(|error| {
        RpcError::new(
            KERNEL_ERROR,
            format!("unserialisable snapshot field: {error}"),
        )
    })?;
    let omitted = OmittedField {
        path,
        total_bytes: bytes.len(),
        checksum: fnv1a64(bytes.as_bytes()),
        availability: pins.add(session.clone(), None, Kind::Field, bytes, None, None),
    };
    let entry = encode(&omitted)?;
    let fields = wire
        .as_object_mut()
        .ok_or_else(|| protocol_limit("invalid open result"))?
        .entry("omittedFields")
        .or_insert_with(|| Value::Array(Vec::new()));
    if let Value::Array(fields) = fields {
        fields.push(entry);
    }
    Ok(())
}

fn take_path(mut value: &mut Value, path: &[String]) -> Option<Value> {
    let (last, parents) = path.split_last()?;
    for segment in parents {
        value = value.get_mut(segment)?;
    }
    value.as_object_mut()?.remove(last)
}

fn trim_oldest(wire: &mut Value) -> bool {
    let Some(items) = wire["snapshot"]["items"].as_array_mut() else {
        return false;
    };
    if items.is_empty() {
        return false;
    }
    items.remove(0);
    let before = items
        .first()
        .and_then(|item| item.get("id"))
        .cloned()
        .unwrap_or(Value::Null);
    if let Some(history) = wire.get_mut("history").and_then(Value::as_object_mut) {
        history.insert("before".into(), before);
        history.insert("hasMore".into(), Value::Bool(true));
    }
    true
}
