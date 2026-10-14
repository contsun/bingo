//! What a chat does with a line the person typed: a reply that answers the
//! open question, a command that runs as one, and the words this surface
//! says in its own voice — in the language the conversation reads.

use super::*;

/// A conversation that reads Chinese says so, and this surface's own copy —
/// and only its own: what a command answers is the plugin's to write.
#[tokio::test]
async fn a_chat_that_reads_chinese_gets_this_surfaces_own_words_in_chinese() {
    let chat = Chat::with(loopback::Config {
        limits: crate::Limits {
            locale: crate::limits::Locale::Zh,
            ..loopback::Config::default().limits
        },
        ..loopback::Config::default()
    });
    chat.say(hello("oc_1")).await;
    chat.say(said(Conversation::direct("oc_1"), "/new", true))
        .await;
    chat.until(|| {
        chat.loopback
            .records()
            .into_iter()
            .any(|record| {
                matches!(record,
                    Record::Send { text, .. } | Record::Reply { text, .. }
                        if text == "新会话已开启。"
                )
            })
            .then_some(())
    })
    .await;
}

/// A question that takes words of one's own — a pasted key, an MCP code —
/// which is where a command is most easily mistaken for an answer.
fn pasting() -> bingo_sdk::Interaction {
    bingo_sdk::Interaction {
        answers: vec![bingo_sdk::AnswerSpec::Text, bingo_sdk::AnswerSpec::Cancel],
        kind: bingo_sdk::InteractionKind::Question(bingo_sdk::Question {
            question: "paste the key".into(),
            header: None,
            options: Vec::new(),
            free_text: true,
            multi: false,
        }),
        ..fixtures::permission(None)
    }
}

/// A command typed while a question that accepts words of one's own is
/// waiting is the command, not the answer: the line is as long as the command
/// and runs as one, and the dialog stays open for the key it asked for.
#[tokio::test]
async fn a_typed_command_is_a_command_even_where_words_of_ones_own_are_an_answer() {
    let chat = Chat::open();
    // `/status` is a command this kernel has; `/tmp` is a directory and is
    // not a command at all, however much it looks like one.
    chat.host.knows(&["/status"]);
    chat.say(hello("oc_1")).await;
    let session = chat.session("loopback/oc_1").await;
    session.publish(Event::TurnStarted {
        turn: bingo_sdk::TurnId::from_raw(fixtures::TURN),
        inputs: Vec::new(),
        origin: bingo_sdk::TurnOrigin::Submit,
    });
    session.publish(Event::InteractionOpened {
        interaction: pasting(),
    });
    chat.records(1).await;

    chat.say(said(Conversation::direct("oc_1"), "/status", true))
        .await;
    chat.until(|| {
        session
            .prompts()
            .contains(&"/status".to_string())
            .then_some(())
    })
    .await;
    assert!(
        session.prompts().contains(&"/status".to_string()),
        "the command reaches the session instead of answering the question: {:?}",
        session.prompts()
    );
    assert!(
        session.answers().is_empty(),
        "and the dialog keeps waiting for the key it asked for"
    );
}

/// The other half: a directory the person is asking about is prose. `/tmp` is
/// not a command, so it still answers the question that invited words.
#[tokio::test]
async fn prose_that_starts_with_a_slash_still_answers_the_question() {
    let chat = Chat::open();
    chat.host.knows(&["/status"]);
    chat.say(hello("oc_1")).await;
    let session = chat.session("loopback/oc_1").await;
    session.publish(Event::TurnStarted {
        turn: bingo_sdk::TurnId::from_raw(fixtures::TURN),
        inputs: Vec::new(),
        origin: bingo_sdk::TurnOrigin::Submit,
    });
    session.publish(Event::InteractionOpened {
        interaction: pasting(),
    });
    chat.records(1).await;

    chat.say(said(
        Conversation::direct("oc_1"),
        "/tmp is where it is",
        true,
    ))
    .await;
    let answered = chat.until(|| session.answers().first().cloned()).await;
    assert_eq!(
        answered.1,
        Answer::Text {
            text: "/tmp is where it is".into()
        },
        "an unknown name is not a command, so it is the answer"
    );
}
