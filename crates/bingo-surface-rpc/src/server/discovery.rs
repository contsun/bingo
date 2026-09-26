//! Stable, byte-bounded discovery without moving whole stored summaries.

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;

use super::*;

impl Server {
    pub(super) async fn children(&self, params: Value, id: &Id) -> Result<Reply, RpcError> {
        let params: ChildrenParams = parse(params)?;
        validate_budget(params.max_bytes)?;
        let summaries = self
            .host
            .sessions(SessionFilter {
                parent: Some(params.parent),
                ..SessionFilter::default()
            })
            .await?;
        let mut ids: Vec<_> = summaries.into_iter().map(|summary| summary.id).collect();
        ids.sort();
        let candidates: Vec<_> = ids
            .into_iter()
            .filter(|child| params.after.as_ref().is_none_or(|after| child > after))
            .collect();
        let mut result = ChildrenResult {
            children: Vec::new(),
            next: None,
        };
        for (index, child) in candidates.iter().enumerate() {
            result.children.push(child.clone());
            result.next = (index + 1 < candidates.len()).then(|| child.clone());
            if response_len(id, &result)? > params.max_bytes {
                result.children.pop();
                if result.children.is_empty() {
                    return Err(protocol_limit("a child id cannot fit this page"));
                }
                result.next = result.children.last().cloned();
                break;
            }
        }
        Reply::of(&result)
    }

    pub(super) async fn list_heads(&self, params: Value, id: &Id) -> Result<Reply, RpcError> {
        let params: ListHeadsParams = parse(params)?;
        validate_budget(params.max_bytes)?;
        let filter = SessionFilter {
            cwd: params.filter.cwd,
            parent: params.filter.parent,
            limit: None,
        };
        let mut sessions = self.host.sessions(filter).await?;
        sessions.sort_by(|a, b| a.id.cmp(&b.id));
        let candidates: Vec<_> = sessions
            .into_iter()
            .filter(|summary| {
                params
                    .after
                    .as_ref()
                    .is_none_or(|after| &summary.id > after)
            })
            .collect();
        let mut result = ListHeadsResult {
            heads: Vec::new(),
            next: None,
        };
        for (index, summary) in candidates.iter().enumerate() {
            result.heads.push(head(summary, params.max_bytes));
            result.next = (index + 1 < candidates.len()).then(|| summary.id.clone());
            while response_len(id, &result)? > params.max_bytes {
                if result
                    .heads
                    .last_mut()
                    .is_some_and(omit_largest_display_field)
                {
                    continue;
                }
                result.heads.pop();
                if result.heads.is_empty() {
                    return Err(protocol_limit(
                        "the true session identity cannot fit this page",
                    ));
                }
                result.next = result.heads.last().map(|head| head.id.clone());
                break;
            }
            if result
                .heads
                .last()
                .is_some_and(|head| head.id != summary.id)
            {
                break;
            }
        }
        Reply::of(&result)
    }
}

fn head(summary: &SessionSummary, budget: usize) -> SummaryHead {
    let mut omitted = Vec::new();
    let cap = budget / 4;
    SummaryHead {
        id: summary.id.clone(),
        cwd: summary.cwd.clone(),
        parent: summary.parent.clone(),
        driver: summary.driver,
        created_at: summary.created_at.to_string(),
        updated_at: summary.updated_at.to_string(),
        busy: summary.busy,
        messages: summary.messages,
        key: preview(summary.key.as_deref(), HeadField::Key, cap, &mut omitted),
        title: preview(
            summary.title.as_deref(),
            HeadField::Title,
            cap,
            &mut omitted,
        ),
        model: preview(
            summary.model.as_deref(),
            HeadField::Model,
            cap,
            &mut omitted,
        ),
        provider: preview(
            summary.provider.as_deref(),
            HeadField::Provider,
            cap,
            &mut omitted,
        ),
        omitted,
    }
}

fn omit_largest_display_field(head: &mut SummaryHead) -> bool {
    let largest = [
        (HeadField::Key, head.key.as_ref()),
        (HeadField::Title, head.title.as_ref()),
        (HeadField::Model, head.model.as_ref()),
        (HeadField::Provider, head.provider.as_ref()),
    ]
    .into_iter()
    .filter_map(|(field, value)| {
        value.map(|value| {
            (
                field,
                serde_json::to_vec(value).map_or(value.len(), |s| s.len()),
            )
        })
    })
    .max_by_key(|(_, size)| *size)
    .map(|(field, _)| field);
    let Some(field) = largest else { return false };
    let value = match field {
        HeadField::Key => head.key.take(),
        HeadField::Title => head.title.take(),
        HeadField::Model => head.model.take(),
        HeadField::Provider => head.provider.take(),
    };
    let Some(value) = value else { return false };
    head.omitted.push(HeadOmission {
        field,
        total_bytes: value.len(),
        reason: "openSessionToReadField".into(),
    });
    true
}

fn preview(
    value: Option<&str>,
    field: HeadField,
    cap: usize,
    omitted: &mut Vec<HeadOmission>,
) -> Option<String> {
    let value = value?;
    if value.len() <= cap {
        return Some(value.to_owned());
    }
    omitted.push(HeadOmission {
        field,
        total_bytes: value.len(),
        reason: "openSessionToReadField".into(),
    });
    None
}
