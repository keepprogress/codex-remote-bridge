#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use codex_app_server_protocol::{JSONRPCMessage, JSONRPCRequest, JSONRPCResponse, RequestId};
use codex_app_server_transport::ConnectionId;
use codex_remote_bridge::acp::AcpClient;
use codex_remote_bridge::bridge::Bridge;
use codex_remote_bridge::state::StateStore;
use serde_json::{Value, json};
use tokio::time::{Duration, timeout};

fn fake_agent() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fake-agent");
    std::fs::write(
        &path,
        r#"#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1}
    elif method == "authenticate":
        result = {}
    elif method == "session/new":
        result = {"sessionId": "sess_bridge"}
    elif method == "session/prompt":
        print(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "sess_bridge",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "from Cursor"}
                }
            }
        }), flush=True)
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    if "id" in msg:
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    (temp, path)
}

fn fake_approval_agent() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fake-approval-agent");
    std::fs::write(
        &path,
        r#"#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1}
    elif method == "authenticate":
        result = {}
    elif method == "session/new":
        result = {"sessionId": "sess_approval"}
    elif method == "session/prompt":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": "permission_1",
            "method": "session/request_permission",
            "params": {
                "sessionId": "sess_approval",
                "toolCall": {
                    "toolCallId": "tool_1",
                    "title": "touch approved.txt",
                    "kind": "execute"
                },
                "options": [
                    {"optionId": "opaque_yes", "name": "Allow", "kind": "allow_once"},
                    {"optionId": "opaque_no", "name": "Reject", "kind": "reject_once"}
                ]
            }
        }), flush=True)
        permission = json.loads(sys.stdin.readline())
        selected = permission["result"]["outcome"].get("optionId")
        text = "approved" if selected == "opaque_yes" else "rejected"
        print(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "sess_approval",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": text}
                }
            }
        }), flush=True)
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    if "id" in msg:
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    (temp, path)
}

fn request(id: i64, method: &str, params: Value) -> JSONRPCMessage {
    JSONRPCMessage::Request(JSONRPCRequest {
        id: RequestId::Integer(id),
        method: method.to_owned(),
        params: Some(params),
        trace: None,
    })
}

#[tokio::test]
async fn fake_remote_and_acp_complete_a_streamed_turn() {
    let (_agent_temp, agent) = fake_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(64);
    let connection = ConnectionId(1);

    bridge
        .handle(
            connection,
            request(1, "thread/start", json!({})),
            writer.clone(),
        )
        .await;
    let thread_response = receiver.recv().await.unwrap();
    let response_json = serde_json::to_value(thread_response.message).unwrap();
    let thread_id = response_json
        .pointer("/result/thread/id")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    receiver.recv().await.expect("thread/started notification");

    bridge
        .handle(
            connection,
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "say hello"}]
                }),
            ),
            writer,
        )
        .await;

    let mut saw_delta = false;
    let mut saw_completion = false;
    let mut methods = Vec::new();
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            let method = value.get("method").and_then(Value::as_str);
            if let Some(method) = method {
                methods.push(method.to_owned());
            }
            match method {
                Some("item/agentMessage/delta") => {
                    saw_delta = value.pointer("/params/delta") == Some(&Value::from("from Cursor"));
                }
                Some("turn/completed") => {
                    saw_completion = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("turn should complete");

    assert!(saw_delta, "frames: {methods:?}");
    assert!(saw_completion, "frames: {methods:?}");
}

#[tokio::test]
async fn mobile_approval_maps_back_to_opaque_acp_option() {
    let (_agent_temp, agent) = fake_approval_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(64);
    let connection = ConnectionId(2);

    bridge
        .handle(
            connection,
            request(1, "thread/start", json!({})),
            writer.clone(),
        )
        .await;
    let response = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
    let thread_id = response["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    receiver.recv().await.unwrap();
    bridge
        .handle(
            connection,
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "create a file"}]
                }),
            ),
            writer.clone(),
        )
        .await;

    let mut approved_text = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            match value.get("method").and_then(Value::as_str) {
                Some("item/commandExecution/requestApproval") => {
                    let id = value["id"].as_i64().unwrap();
                    bridge
                        .handle(
                            connection,
                            JSONRPCMessage::Response(JSONRPCResponse {
                                id: RequestId::Integer(id),
                                result: json!({"decision": "accept"}),
                            }),
                            writer.clone(),
                        )
                        .await;
                }
                Some("item/agentMessage/delta") => {
                    approved_text =
                        value.pointer("/params/delta") == Some(&Value::from("approved"));
                }
                Some("turn/completed") => break,
                _ => {}
            }
        }
    })
    .await
    .expect("approved turn should complete");

    assert!(approved_text);
}
