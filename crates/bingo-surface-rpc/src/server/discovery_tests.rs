use super::*;

#[test]
fn escaped_display_fields_fit_by_omission_before_rejecting_the_true_head() {
    let original = "\\\"".repeat(1000);
    let mut result = ListHeadsResult {
        heads: vec![SummaryHead {
            id: SessionId::from_raw("ses_1"),
            cwd: "/work".into(),
            parent: None,
            driver: bingo_sdk::Driver::Model,
            created_at: "2026-09-23T00:00:00Z".into(),
            updated_at: "2026-09-23T00:00:00Z".into(),
            busy: false,
            messages: Some(2),
            key: Some(original.clone()),
            title: Some(original.clone()),
            model: Some(original.clone()),
            provider: Some(original.clone()),
            omitted: Vec::new(),
        }],
        next: None,
    };
    let id = Id::Number(42);
    let budget = 8 * 1024;
    assert!(result.heads[0].key.as_ref().unwrap().len() <= budget / 4);
    assert!(response_len(&id, &result).unwrap() > budget);
    while response_len(&id, &result).unwrap() > budget {
        assert!(omit_largest_display_field(&mut result.heads[0]));
    }
    assert!(!result.heads[0].omitted.is_empty());
    assert_eq!(result.heads[0].cwd, "/work");
    assert_eq!(result.heads[0].id, SessionId::from_raw("ses_1"));
    assert!(response_len(&id, &result).unwrap() <= budget);
    for omission in &result.heads[0].omitted {
        assert_eq!(omission.total_bytes, original.len());
    }
}
