//! The final line of a Feishu answer, derived from the session's folded facts.

use bingo_sdk::{Effort, SessionState, Usage};

use crate::limits::Limits;

const SEPARATOR: &str = "\n\n---\n";

pub(super) fn append(text: &str, state: &SessionState, limits: &Limits) -> String {
    if text.is_empty() {
        return String::new();
    }
    let footer = line(state, Language::of(text));
    let mut answer_limits = limits.clone();
    answer_limits.max_text.0 = answer_limits
        .max_text
        .0
        .saturating_sub(SEPARATOR.len() + footer.len());
    let answer = answer_limits.clip(text);
    format!("{answer}{SEPARATOR}{footer}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Language {
    Chinese,
    English,
}

impl Language {
    fn of(text: &str) -> Self {
        let (mut han, mut english_words) = (0, 0);
        let mut fenced = false;
        for line in text.lines() {
            let start = line.trim_start();
            if start.starts_with("```") || start.starts_with("~~~") {
                fenced = !fenced;
                continue;
            }
            if fenced {
                continue;
            }
            let (mut inline, mut word) = (false, false);
            for character in line.chars() {
                if character == '`' {
                    inline = !inline;
                    word = false;
                } else if !inline && is_han(character) {
                    han += 1;
                    word = false;
                } else if !inline && character.is_ascii_alphabetic() {
                    if !word {
                        english_words += 1;
                    }
                    word = true;
                } else {
                    word = false;
                }
            }
        }
        if han > 0 && han >= english_words {
            Self::Chinese
        } else {
            Self::English
        }
    }
}

fn is_han(character: char) -> bool {
    ('\u{3400}'..='\u{4dbf}').contains(&character)
        || ('\u{4e00}'..='\u{9fff}').contains(&character)
        || ('\u{f900}'..='\u{faff}').contains(&character)
        || ('\u{20000}'..='\u{2a6df}').contains(&character)
}

fn line(state: &SessionState, language: Language) -> String {
    let mut parts = vec![
        state
            .summary
            .model
            .as_deref()
            .unwrap_or("bingo")
            .to_string(),
    ];
    if let Some(level) = state
        .config
        .kernel
        .get("thinking")
        .and_then(|value| serde_json::from_value::<Effort>(value.clone()).ok())
    {
        parts.push(match language {
            Language::Chinese => format!("思考强度：{}", chinese_effort(level)),
            Language::English => format!("effort:{}", level.name()),
        });
    }
    let usage = state
        .turn
        .as_ref()
        .map(|turn| turn.usage)
        .or_else(|| state.last_turn.as_ref().map(|turn| turn.usage));
    if let Some(usage) = usage.filter(|usage| *usage != Usage::default()) {
        match language {
            Language::Chinese => {
                parts.push(format!("输出 {}", count(usage.output_tokens)));
                parts.push(format!(
                    "累计输入 {} 缓存写 {} 缓存读 {}",
                    count(usage.input_total()),
                    count(usage.cache_write_tokens),
                    count(usage.cache_read_tokens)
                ));
            }
            Language::English => {
                parts.push(format!("out {}", count(usage.output_tokens)));
                parts.push(format!(
                    "in {} cw {} cr {}",
                    count(usage.input_total()),
                    count(usage.cache_write_tokens),
                    count(usage.cache_read_tokens)
                ));
            }
        }
    }
    if let Some(context) = state.context.filter(|context| context.window > 0) {
        parts.push(match language {
            Language::Chinese => format!("上下文 {}%", context.percent()),
            Language::English => format!("ctx {}%", context.percent()),
        });
    }
    parts.join(" · ")
}

fn chinese_effort(level: Effort) -> &'static str {
    match level {
        Effort::Minimal => "最低",
        Effort::Low => "低",
        Effort::Medium => "中",
        Effort::High => "高",
        Effort::XHigh => "极高",
        Effort::Max => "最高",
    }
}

fn count(value: u64) -> String {
    match value {
        1_000_000.. => short(value, 1_000_000, "m"),
        1_000.. => short(value, 1_000, "k"),
        _ => value.to_string(),
    }
}

fn short(value: u64, unit: u64, suffix: &str) -> String {
    let tenths = value / (unit / 10);
    match tenths % 10 {
        0 => format!("{}{suffix}", tenths / 10),
        decimal => format!("{}.{decimal}{suffix}", tenths / 10),
    }
}

#[cfg(test)]
mod tests {
    use bingo_sdk::{ContextUsage, Event, TurnId, TurnStatus};
    use serde_json::json;

    use super::*;
    use crate::fixtures;
    use crate::limits::{Dialect, Encoding};

    fn limits(max: usize) -> Limits {
        Limits {
            max_text: (max, Encoding::Utf8Bytes),
            dialect: Dialect::Markdown,
            max_actions: 4,
            max_label: 30,
        }
    }

    #[test]
    fn a_completed_turn_shows_its_model_effort_usage_and_context() {
        let mut state = fixtures::state();
        state.summary.model = Some("gpt-6-sol".into());
        state.config.kernel = json!({ "thinking": "xHigh" });
        state.apply(&fixtures::turn_started(1));
        state.apply(&fixtures::frame(
            2,
            Event::TurnUsage {
                turn: TurnId::from_raw(fixtures::TURN),
                usage: Usage::default(),
                context: ContextUsage {
                    used: 150_000,
                    window: 200_000,
                    trigger: 180_000,
                },
            },
        ));
        state.apply(&fixtures::frame(
            3,
            Event::TurnCompleted {
                turn: TurnId::from_raw(fixtures::TURN),
                status: TurnStatus::Completed,
                usage: Usage {
                    input_tokens: 400,
                    output_tokens: 328,
                    cache_write_tokens: 0,
                    cache_read_tokens: 193_000,
                    reasoning_tokens: 0,
                },
            },
        ));
        assert_eq!(
            append("Done.", &state, &limits(20_000)),
            "Done.\n\n---\ngpt-6-sol · effort:xhigh · out 328 · in 193.4k cw 0 cr 193k · ctx 75%"
        );
        assert_eq!(
            append("已经完成。", &state, &limits(20_000)),
            "已经完成。\n\n---\ngpt-6-sol · 思考强度：极高 · 输出 328 · 累计输入 193.4k 缓存写 0 缓存读 193k · 上下文 75%"
        );
    }

    #[test]
    fn the_answer_language_ignores_code_and_short_quotes() {
        assert_eq!(
            Language::of("Tests passed. `你好` means hello."),
            Language::English
        );
        assert_eq!(
            Language::of("The phrase 你好 means hello.\n```text\n中文代码\n```"),
            Language::English
        );
        assert_eq!(
            Language::of("测试完成，`cargo test` 全部通过。"),
            Language::Chinese
        );
        assert_eq!(
            Language::of("Great!\n```sh\necho 中文\n```"),
            Language::English
        );
    }

    #[test]
    fn a_live_turn_does_not_use_the_previous_turns_usage() {
        let mut state = fixtures::state();
        state.apply(&fixtures::turn_started(1));
        state.apply(&fixtures::frame(
            2,
            Event::TurnCompleted {
                turn: TurnId::from_raw(fixtures::TURN),
                status: TurnStatus::Completed,
                usage: Usage {
                    output_tokens: 20,
                    ..Usage::default()
                },
            },
        ));
        state.apply(&fixtures::turn_started(3));
        state.config.kernel = json!({ "thinking": null });
        state.context = Some(ContextUsage {
            used: 1,
            window: 0,
            trigger: 0,
        });
        assert_eq!(
            append("Question?", &state, &limits(20_000)),
            "Question?\n\n---\nfake-1"
        );
        assert_eq!(append("", &state, &limits(20_000)), "");
    }

    #[test]
    fn a_long_answer_keeps_the_footer_inside_feishus_text_limit() {
        let state = fixtures::state();
        let result = append(&"a".repeat(20_000), &state, &limits(60));
        assert!(result.len() <= 60);
        assert!(result.ends_with("\n\n---\nfake-1"));
        assert!(result.starts_with('a'));
    }
}
