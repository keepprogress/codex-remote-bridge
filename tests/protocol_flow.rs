use codex_app_server_protocol::RequestId;
use codex_remote_bridge::rpc::{send_notification, send_server_request};
use serde_json::json;

#[tokio::test]
async fn translated_turn_and_approval_match_codex_0145_schema() {
    let (writer, mut receiver) = tokio::sync::mpsc::channel(64);
    let thread = json!({
        "id": "thr_test",
        "extra": null,
        "sessionId": "thr_test",
        "forkedFromId": null,
        "parentThreadId": null,
        "preview": "",
        "ephemeral": false,
        "historyMode": "legacy",
        "modelProvider": "cursor",
        "createdAt": 1,
        "updatedAt": 1,
        "recencyAt": 1,
        "status": {"type": "idle"},
        "path": null,
        "cwd": "/work/project",
        "cliVersion": "0.1.0",
        "source": "appServer",
        "canAcceptDirectInput": true,
        "threadSource": null,
        "agentNickname": null,
        "agentRole": null,
        "gitInfo": null,
        "name": null,
        "turns": []
    });
    send_notification(&writer, "thread/started", json!({"thread": thread}))
        .await
        .unwrap();
    let turn = json!({
        "id": "turn_test",
        "items": [],
        "itemsView": "full",
        "status": "inProgress",
        "error": null,
        "startedAt": 1,
        "completedAt": null,
        "durationMs": null
    });
    send_notification(
        &writer,
        "turn/started",
        json!({"threadId": "thr_test", "turn": turn}),
    )
    .await
    .unwrap();
    send_notification(
        &writer,
        "item/started",
        json!({
            "item": {
                "type": "agentMessage",
                "id": "item_test",
                "text": "",
                "phase": null,
                "memoryCitation": null
            },
            "threadId": "thr_test",
            "turnId": "turn_test",
            "startedAtMs": 1000
        }),
    )
    .await
    .unwrap();
    send_notification(
        &writer,
        "item/agentMessage/delta",
        json!({
            "threadId": "thr_test",
            "turnId": "turn_test",
            "itemId": "item_test",
            "delta": "hello"
        }),
    )
    .await
    .unwrap();
    send_notification(
        &writer,
        "item/started",
        json!({
            "item": {
                "type": "dynamicToolCall",
                "id": "tool_test",
                "namespace": "cursor",
                "tool": "Run tests",
                "arguments": {},
                "status": "inProgress",
                "contentItems": null,
                "success": null,
                "durationMs": null
            },
            "threadId": "thr_test",
            "turnId": "turn_test",
            "startedAtMs": 1000
        }),
    )
    .await
    .unwrap();
    send_notification(
        &writer,
        "item/completed",
        json!({
            "item": {
                "type": "dynamicToolCall",
                "id": "tool_test",
                "namespace": "cursor",
                "tool": "Run tests",
                "arguments": {},
                "status": "completed",
                "contentItems": null,
                "success": true,
                "durationMs": null
            },
            "threadId": "thr_test",
            "turnId": "turn_test",
            "completedAtMs": 1001
        }),
    )
    .await
    .unwrap();
    send_server_request(
        &writer,
        RequestId::Integer(99),
        "item/commandExecution/requestApproval",
        json!({
            "threadId": "thr_test",
            "turnId": "turn_test",
            "itemId": "tool_test",
            "startedAtMs": 1000,
            "approvalId": null,
            "environmentId": null,
            "reason": "Run tests",
            "networkApprovalContext": null,
            "command": "cargo test",
            "cwd": "/work/project",
            "commandActions": [{"type": "unknown", "command": "cargo test"}],
            "additionalPermissions": null,
            "proposedExecpolicyAmendment": null,
            "proposedNetworkPolicyAmendments": null,
            "availableDecisions": null
        }),
    )
    .await
    .unwrap();

    for _ in 0..7 {
        receiver.recv().await.expect("translated frame");
    }
}

#[tokio::test]
async fn malformed_translation_is_rejected_before_relay() {
    let (writer, _receiver) = tokio::sync::mpsc::channel(1);
    let error = send_notification(&writer, "turn/started", json!({"threadId": "missing-turn"}))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("turn/started"));
}
