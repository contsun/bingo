//! A command's result, in the words this chat reads.
//!
//! Every command's answer is written by the plugin that owns it, in the
//! language the kernel is written in — English. A chat is not: the person
//! typing `/status` is reading Chinese, and the words should match the ones
//! the same answer carries in an answer's footer. So the labels, the table
//! headers and the one-line answers are said again here.
//!
//! Only the shapes this surface knows are translated, and they are matched
//! whole rather than by substring: a memory the person named `type` must not
//! come back as `类型`. Anything unrecognised is left exactly as the plugin
//! wrote it, which is the honest degrade.

/// A folded `View` puts a label in front of a value; these are the labels of
/// the commands this chat offers. A label not listed is left alone.
const LABELS: [(&str, &str); 7] = [
    ("session", "会话"),
    ("cwd", "工作目录"),
    ("provider", "模型服务"),
    ("model", "模型"),
    ("mode", "权限模式"),
    ("context", "上下文"),
    ("tokens", "tokens"),
];

/// A table's header line, whole. `View::Table` joins its headers with ` · `,
/// so this is what an empty table folds to as well — which is why the empties
/// below are matched first.
const HEADERS: [(&str, &str); 2] = [
    ("scope · name · type · description", "范围 · 名称 · 类型 · 说明"),
    ("server · status · tools · auth", "服务 · 状态 · 工具 · 认证"),
];

/// Whole lines a command answers with, said again.
const PHRASES: [(&str, &str); 3] = [
    ("not measured yet", "尚未测量"),
    ("no schedules yet", "暂无定时任务"),
    ("schedules: held by this process", "定时任务由当前进程持有"),
];

/// The one answer that is a sentence rather than a view.
const NOTHING_REMEMBERED: &str = "nothing is remembered yet; memories go in";

/// A command's result as this chat should read it.
///
/// `source` is the command that answered — `/mcp` with no servers and an empty
/// `/schedule` both fold to something with nothing under it, and only the
/// command itself can say which emptiness this is.
pub(super) fn command(source: &str, text: &str) -> String {
    if let Some(said) = emptiness(source, text) {
        return said.to_string();
    }
    if let Some(rest) = text.strip_prefix(NOTHING_REMEMBERED) {
        return format!("尚未记录任何记忆；记忆存放在{rest}");
    }
    text.lines().map(said).collect::<Vec<_>>().join("\n")
}

/// Nothing to show, said out loud: a bare header row reads as a failure
/// rather than as an answer.
fn emptiness(source: &str, text: &str) -> Option<&'static str> {
    let bare = text.trim();
    if source != "/mcp" {
        return None;
    }
    // Headers only: no row under them, and no `label: value` either.
    let rows = bare.lines().count();
    (rows == 1 && !bare.contains(": ")).then_some("暂无 MCP 服务")
}

/// One line — a header row, a labelled value, or a sentence.
fn said(line: &str) -> String {
    if let Some((_, to)) = HEADERS.iter().find(|(from, _)| *from == line.trim()) {
        return (*to).to_string();
    }
    match line.split_once(": ") {
        Some((label, value)) => match LABELS.iter().find(|(from, _)| *from == label) {
            Some((_, to)) => format!("{to}: {}", phrase(value)),
            None => phrase(line),
        },
        None => phrase(line),
    }
}

/// A line with any whole phrase it is, said again.
fn phrase(text: &str) -> String {
    let mut said = text.to_string();
    for (from, to) in PHRASES {
        if said == from {
            said = to.to_string();
        }
    }
    said
}
