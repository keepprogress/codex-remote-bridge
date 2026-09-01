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

fn fake_compaction_agent() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fake-compaction-agent");
    std::fs::write(
        &path,
        r#"#!/usr/bin/env python3
import json, sys
session_count = 0
replacement_prompt_count = 0
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1}
    elif method == "authenticate":
        result = {}
    elif method == "session/new":
        session_count += 1
        result = {"sessionId": "sess_before" if session_count == 1 else "sess_after"}
    elif method == "session/prompt":
        session_id = msg["params"]["sessionId"]
        if session_id == "sess_before":
            text = "Confirmed decisions and pending work from the original session."
        else:
            replacement_prompt_count += 1
            text = "CONTEXT_READY" if replacement_prompt_count == 1 else "from replacement session"
        print(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
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

async fn process_bridge() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<Bridge>,
) {
    let (agent_temp, agent) = fake_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            codex_home.path().to_path_buf(),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    (agent_temp, state_temp, codex_home, bridge)
}

#[tokio::test]
async fn config_read_uses_codex_config_schema() {
    let (_agent_temp, agent) = fake_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            codex_home.path().to_path_buf(),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(8);
    bridge
        .handle(
            ConnectionId(10),
            request(2, "config/read", json!({})),
            writer,
        )
        .await;

    let frame = receiver.recv().await.expect("config/read response");
    let value = serde_json::to_value(frame.message).unwrap();
    let config = value.pointer("/result/config").expect("config object");
    assert_eq!(
        config.get("approval_policy").and_then(Value::as_str),
        Some("on-request")
    );
    assert_eq!(
        config.get("sandbox_mode").and_then(Value::as_str),
        Some("workspace-write")
    );
    assert!(value.pointer("/result/layers").is_some());
}

#[tokio::test]
async fn mobile_bootstrap_endpoints_return_empty_defaults() {
    let (_agent_temp, agent) = fake_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            codex_home.path().to_path_buf(),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(8);

    for (req_id, method) in [
        (3, "configRequirements/read"),
        (4, "collaborationMode/list"),
        (5, "plugin/installed"),
        (6, "skills/extraRoots/set"),
        (7, "model/list"),
        (8, "thread/goal/get"),
    ] {
        bridge
            .handle(
                ConnectionId(11),
                request(req_id, method, json!({})),
                writer.clone(),
            )
            .await;
        let frame = receiver.recv().await.expect("bootstrap response");
        let value = serde_json::to_value(frame.message).unwrap();
        assert!(
            value.get("error").is_none(),
            "{method} should succeed: {value}"
        );
        if method == "model/list" {
            assert_eq!(
                value
                    .pointer("/result/data/0/defaultReasoningEffort")
                    .and_then(Value::as_str),
                Some("medium")
            );
        } else if method == "thread/goal/get" {
            assert_eq!(value.pointer("/result/goal"), Some(&Value::Null));
        }
    }
}

#[tokio::test]
async fn request_failures_return_json_rpc_errors_instead_of_hanging() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(8);
    bridge
        .handle(
            ConnectionId(20),
            request(20, "fs/createDirectory", json!({})),
            writer,
        )
        .await;

    let frame = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("request error should not hang")
        .expect("request error response");
    let value = serde_json::to_value(frame.message).unwrap();
    assert_eq!(
        value.pointer("/error/code").and_then(Value::as_i64),
        Some(-32_603)
    );
}

#[tokio::test]
async fn process_spawn_buffers_output_and_honors_per_stream_caps() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(16);
    bridge
        .handle(
            ConnectionId(21),
            request(
                21,
                "process/spawn",
                json!({
                    "command": ["/bin/sh", "-c", "printf abcde; printf 12345 >&2"],
                    "processHandle": "buffered",
                    "cwd": "/tmp",
                    "outputBytesCap": 3,
                    "timeoutMs": null
                }),
            ),
            writer,
        )
        .await;

    let response = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
    assert_eq!(response.get("result"), Some(&json!({})));
    let exited = timeout(Duration::from_secs(2), async {
        loop {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("process/exited") {
                break value;
            }
        }
    })
    .await
    .expect("process should exit");
    assert_eq!(exited.pointer("/params/stdout"), Some(&Value::from("abc")));
    assert_eq!(exited.pointer("/params/stderr"), Some(&Value::from("123")));
    assert_eq!(
        exited.pointer("/params/stdoutCapReached"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        exited.pointer("/params/stderrCapReached"),
        Some(&Value::Bool(true))
    );
}

#[tokio::test]
async fn process_spawn_streams_before_exit_and_can_be_killed() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(32);
    let connection = ConnectionId(22);
    bridge
        .handle(
            connection,
            request(
                22,
                "process/spawn",
                json!({
                    "command": ["/bin/sh", "-c", "printf ready; sleep 30"],
                    "processHandle": "streamed",
                    "cwd": "/tmp",
                    "streamStdoutStderr": true,
                    "timeoutMs": null
                }),
            ),
            writer.clone(),
        )
        .await;
    let response = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
    assert_eq!(response.get("result"), Some(&json!({})));

    let delta = timeout(Duration::from_secs(2), async {
        loop {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("process/outputDelta") {
                break value;
            }
        }
    })
    .await
    .expect("stdout should stream while the process is still running");
    assert_eq!(
        delta.pointer("/params/deltaBase64"),
        Some(&Value::from("cmVhZHk="))
    );

    bridge
        .handle(
            connection,
            request(23, "process/kill", json!({"processHandle": "streamed"})),
            writer,
        )
        .await;
    let mut saw_kill_response = false;
    let mut saw_exit = false;
    timeout(Duration::from_secs(3), async {
        while !saw_kill_response || !saw_exit {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("id").and_then(Value::as_i64) == Some(23) {
                saw_kill_response = value.get("result") == Some(&json!({}));
            } else if value.get("method").and_then(Value::as_str) == Some("process/exited") {
                saw_exit = true;
                assert_ne!(
                    value.pointer("/params/exitCode").and_then(Value::as_i64),
                    Some(0)
                );
            }
        }
    })
    .await
    .expect("killed process should exit");
}

#[tokio::test]
async fn process_write_stdin_round_trips_bytes_and_closes_stdin() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(32);
    let connection = ConnectionId(23);
    bridge
        .handle(
            connection,
            request(
                24,
                "process/spawn",
                json!({
                    "command": ["/bin/cat"],
                    "processHandle": "stdin",
                    "cwd": "/tmp",
                    "streamStdin": true,
                    "timeoutMs": null
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("spawn response");
    bridge
        .handle(
            connection,
            request(
                25,
                "process/writeStdin",
                json!({
                    "processHandle": "stdin",
                    "deltaBase64": "aGVsbG8=",
                    "closeStdin": true
                }),
            ),
            writer,
        )
        .await;

    let mut saw_write_response = false;
    let mut stdout = None;
    timeout(Duration::from_secs(3), async {
        while !saw_write_response || stdout.is_none() {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("id").and_then(Value::as_i64) == Some(25) {
                saw_write_response = value.get("result") == Some(&json!({}));
            } else if value.get("method").and_then(Value::as_str) == Some("process/exited") {
                stdout = value
                    .pointer("/params/stdout")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
        }
    })
    .await
    .expect("cat should exit after stdin closes");
    assert_eq!(stdout.as_deref(), Some("hello"));
}

#[tokio::test]
async fn connection_close_terminates_owned_processes() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(16);
    let connection = ConnectionId(24);
    bridge
        .handle(
            connection,
            request(
                26,
                "process/spawn",
                json!({
                    "command": ["/bin/sh", "-c", "sleep 30"],
                    "processHandle": "disconnect",
                    "cwd": "/tmp",
                    "timeoutMs": null
                }),
            ),
            writer,
        )
        .await;
    receiver.recv().await.expect("spawn response");
    bridge.connection_closed(connection).await;
    let exited = timeout(Duration::from_secs(3), async {
        loop {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("process/exited") {
                break value;
            }
        }
    })
    .await
    .expect("disconnect should terminate owned processes");
    assert_ne!(
        exited.pointer("/params/exitCode").and_then(Value::as_i64),
        Some(0)
    );
}

#[tokio::test]
async fn process_timeout_terminates_and_reports_exit_124() {
    let (_agent_temp, _state_temp, _codex_home, bridge) = process_bridge().await;
    let (writer, mut receiver) = tokio::sync::mpsc::channel(16);
    bridge
        .handle(
            ConnectionId(25),
            request(
                27,
                "process/spawn",
                json!({
                    "command": ["/bin/sh", "-c", "sleep 30"],
                    "processHandle": "timeout",
                    "cwd": "/tmp",
                    "timeoutMs": 50
                }),
            ),
            writer,
        )
        .await;
    receiver.recv().await.expect("spawn response");
    let exited = timeout(Duration::from_secs(3), async {
        loop {
            let value = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("process/exited") {
                break value;
            }
        }
    })
    .await
    .expect("timed out process should terminate");
    assert_eq!(
        exited.pointer("/params/exitCode").and_then(Value::as_i64),
        Some(124)
    );
}

#[tokio::test]
async fn initialize_response_matches_codex_0145_schema() {
    let (_agent_temp, agent) = fake_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            codex_home.path().to_path_buf(),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(8);
    bridge
        .handle(
            ConnectionId(9),
            request(
                1,
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "chatgpt_remote",
                        "title": "ChatGPT Remote",
                        "version": "1.0.0"
                    }
                }),
            ),
            writer,
        )
        .await;

    let frame = receiver.recv().await.expect("initialize response");
    let value = serde_json::to_value(frame.message).unwrap();
    let result = value.get("result").expect("result");
    assert_eq!(
        result.get("userAgent").and_then(Value::as_str),
        Some("codex-remote-bridge/0.1.0")
    );
    assert_eq!(
        result.get("platformFamily").and_then(Value::as_str),
        Some(std::env::consts::FAMILY)
    );
    assert_eq!(
        result.get("platformOs").and_then(Value::as_str),
        Some(std::env::consts::OS)
    );
    assert!(result.get("codexHome").is_some());
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
            std::path::PathBuf::from("/tmp/codex-home"),
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
async fn compact_rolls_cursor_context_into_a_replacement_session() {
    let (_agent_temp, agent) = fake_compaction_agent();
    let state_temp = tempfile::tempdir().unwrap();
    let acp = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            acp,
            std::path::PathBuf::from("/tmp"),
            std::path::PathBuf::from("/tmp/codex-home"),
            "auto".into(),
            StateStore::new(state_temp.path()),
            false,
        )
        .await
        .unwrap(),
    );
    let (writer, mut receiver) = tokio::sync::mpsc::channel(64);
    let connection = ConnectionId(12);

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
    receiver.recv().await.expect("thread/started notification");

    bridge
        .handle(
            connection,
            request(2, "thread/compact/start", json!({"threadId": thread_id})),
            writer.clone(),
        )
        .await;

    let response = serde_json::to_value(receiver.recv().await.unwrap().message).unwrap();
    assert_eq!(response.pointer("/result"), Some(&json!({})));
    let mut methods = Vec::new();
    let mut saw_context_item = false;
    let mut saw_compacted = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            methods.push(method.to_owned());
            if method == "item/started" {
                saw_context_item =
                    value.pointer("/params/item/type") == Some(&Value::from("contextCompaction"));
            }
            if method == "thread/compacted" {
                saw_compacted = true;
            }
            if method == "turn/completed" {
                assert_eq!(
                    value.pointer("/params/turn/status"),
                    Some(&Value::from("completed"))
                );
                break;
            }
        }
    })
    .await
    .expect("compaction should complete");
    assert!(saw_context_item, "frames: {methods:?}");
    assert!(saw_compacted, "frames: {methods:?}");

    bridge
        .handle(
            connection,
            request(
                3,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "continue"}]
                }),
            ),
            writer,
        )
        .await;
    let mut used_replacement = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            match value.get("method").and_then(Value::as_str) {
                Some("item/agentMessage/delta") => {
                    used_replacement = value.pointer("/params/delta")
                        == Some(&Value::from("from replacement session"));
                }
                Some("turn/completed") => break,
                _ => {}
            }
        }
    })
    .await
    .expect("post-compaction turn should complete");
    assert!(used_replacement);
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
            std::path::PathBuf::from("/tmp/codex-home"),
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
