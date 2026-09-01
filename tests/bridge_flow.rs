#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use codex_app_server_protocol::{JSONRPCMessage, JSONRPCRequest, JSONRPCResponse, RequestId};
use codex_app_server_transport::{ConnectionId, QueuedOutgoingMessage};
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
import json, os, sys
session_count = 0
before_prompt_count = 0
here = os.path.dirname(os.path.abspath(__file__))
emit_todos = os.path.exists(os.path.join(here, "emit_todos"))
summary = """```yaml
objective: Confirmed decisions and pending work from the original session.
decisions: []
failed_approaches: []
verification:
  passed: []
  failing: []
next:
  - inspect case03
```"""

def prompt_text(msg):
    blocks = msg.get("params", {}).get("prompt", [])
    return "".join(block.get("text", "") for block in blocks if block.get("type") == "text")

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
        incoming = prompt_text(msg)
        if session_id == "sess_before":
            before_prompt_count += 1
            with open(os.path.join(here, "before.count"), "w", encoding="utf-8") as fh:
                fh.write(str(before_prompt_count))
            if emit_todos:
                print(json.dumps({
                    "jsonrpc": "2.0",
                    "method": "cursor/update_todos",
                    "params": {
                        "sessionId": session_id,
                        "toolCallId": "todo_1",
                        "merge": False,
                        "todos": [{
                            "id": "t1",
                            "content": "compare TAX_TYPE",
                            "status": "in_progress"
                        }]
                    }
                }), flush=True)
            text = "from original session" if incoming.strip() == "continue" else summary
        elif "BEGIN COMPACTED CONTEXT" in incoming:
            with open(os.path.join(here, "seed.log"), "w", encoding="utf-8") as fh:
                fh.write(incoming)
            if os.path.exists(os.path.join(here, "fail_seed")):
                text = "MISSING_FIELDS"
            elif "git_state" in incoming and "objective" in incoming:
                text = "CONTEXT_READY"
            else:
                text = "MISSING_FIELDS"
        else:
            text = "from replacement session"
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
    let (agent_temp, agent) = fake_compaction_agent();
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
    let seed = std::fs::read_to_string(agent_temp.path().join("seed.log")).unwrap();
    assert!(seed.contains("git_state"), "{seed}");
    assert!(seed.contains("objective"), "{seed}");

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

async fn collect_until_turn_completed(
    receiver: &mut tokio::sync::mpsc::Receiver<QueuedOutgoingMessage>,
) -> (Vec<String>, String, bool) {
    let mut methods = Vec::new();
    let mut agent_text = String::new();
    let mut saw_compacted = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !method.is_empty() {
                methods.push(method.to_owned());
            }
            if method == "item/agentMessage/delta"
                && let Some(delta) = value.pointer("/params/delta").and_then(Value::as_str)
            {
                agent_text.push_str(delta);
            }
            if method == "thread/compacted" {
                saw_compacted = true;
            }
            if method == "turn/completed" {
                break;
            }
        }
    })
    .await
    .expect("turn should complete");
    (methods, agent_text, saw_compacted)
}

#[tokio::test]
async fn compact_preview_does_not_remap_until_apply() {
    let (agent_temp, agent) = fake_compaction_agent();
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
    let connection = ConnectionId(13);

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
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview"}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("turn/start response");
    let (_methods, preview, saw_compacted) = collect_until_turn_completed(&mut receiver).await;
    assert!(preview.contains("Compaction preview"), "{preview}");
    assert!(preview.contains("objective"), "{preview}");
    assert!(!saw_compacted);
    assert!(!agent_temp.path().join("seed.log").exists());

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
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("continue response");
    let (_methods, continued, _) = collect_until_turn_completed(&mut receiver).await;
    assert_eq!(continued, "from original session");

    bridge
        .handle(
            connection,
            request(
                4,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview apply"}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("apply response");
    let (_methods, applied, saw_compacted) = collect_until_turn_completed(&mut receiver).await;
    assert!(applied.contains("Compaction applied"), "{applied}");
    assert!(!saw_compacted);
    let seed = std::fs::read_to_string(agent_temp.path().join("seed.log")).unwrap();
    assert!(seed.contains("git_state"), "{seed}");

    bridge
        .handle(
            connection,
            request(
                5,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "continue"}]
                }),
            ),
            writer,
        )
        .await;
    receiver.recv().await.expect("post-apply response");
    let (_methods, after, _) = collect_until_turn_completed(&mut receiver).await;
    assert_eq!(after, "from replacement session");
}

#[tokio::test]
async fn official_compact_reuses_pending_preview_without_a_second_summary() {
    let (agent_temp, agent) = fake_compaction_agent();
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
    let connection = ConnectionId(14);

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
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview keep \"tax calculation reasoning\""}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("preview response");
    collect_until_turn_completed(&mut receiver).await;
    let before = std::fs::read_to_string(agent_temp.path().join("before.count")).unwrap();
    assert_eq!(before.trim(), "1");

    bridge
        .handle(
            connection,
            request(3, "thread/compact/start", json!({"threadId": thread_id})),
            writer,
        )
        .await;
    receiver.recv().await.expect("compact response");
    let mut saw_compacted = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("thread/compacted") {
                saw_compacted = true;
            }
            if value.get("method").and_then(Value::as_str) == Some("turn/completed") {
                break;
            }
        }
    })
    .await
    .expect("official compact should complete");
    assert!(saw_compacted);
    let before = std::fs::read_to_string(agent_temp.path().join("before.count")).unwrap();
    assert_eq!(before.trim(), "1");
    let seed = std::fs::read_to_string(agent_temp.path().join("seed.log")).unwrap();
    assert!(seed.contains("objective"), "{seed}");
}

#[tokio::test]
async fn compact_seed_includes_update_todos() {
    let (agent_temp, agent) = fake_compaction_agent();
    std::fs::write(agent_temp.path().join("emit_todos"), "").unwrap();
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
    let connection = ConnectionId(15);

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
            writer,
        )
        .await;
    receiver.recv().await.expect("compact response");
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            if value.get("method").and_then(Value::as_str) == Some("turn/completed") {
                break;
            }
        }
    })
    .await
    .expect("todo compact should complete");
    let seed = std::fs::read_to_string(agent_temp.path().join("seed.log")).unwrap();
    assert!(seed.contains("compare TAX_TYPE"), "{seed}");
}

#[tokio::test]
async fn compact_failure_completes_item_and_keeps_pending_preview() {
    let (agent_temp, agent) = fake_compaction_agent();
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
    let connection = ConnectionId(16);

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
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview"}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("preview response");
    collect_until_turn_completed(&mut receiver).await;

    std::fs::write(agent_temp.path().join("fail_seed"), "").unwrap();
    bridge
        .handle(
            connection,
            request(3, "thread/compact/start", json!({"threadId": thread_id})),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("compact response");
    let mut completed_compaction_item = false;
    let mut saw_compacted = false;
    let mut failed = false;
    timeout(Duration::from_secs(3), async {
        while let Some(frame) = receiver.recv().await {
            let value = serde_json::to_value(frame.message).unwrap();
            let method = value.get("method").and_then(Value::as_str);
            if method == Some("item/completed")
                && value.pointer("/params/item/type") == Some(&Value::from("contextCompaction"))
            {
                completed_compaction_item = true;
            }
            if method == Some("thread/compacted") {
                saw_compacted = true;
            }
            if method == Some("turn/completed") {
                failed = value.pointer("/params/turn/status") == Some(&Value::from("failed"));
                break;
            }
        }
    })
    .await
    .expect("failed compact should finish");
    assert!(failed);
    assert!(completed_compaction_item);
    assert!(!saw_compacted);

    std::fs::remove_file(agent_temp.path().join("fail_seed")).unwrap();
    bridge
        .handle(
            connection,
            request(
                4,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview apply"}]
                }),
            ),
            writer,
        )
        .await;
    receiver.recv().await.expect("apply response");
    let (_methods, applied, _) = collect_until_turn_completed(&mut receiver).await;
    assert!(applied.contains("Compaction applied"), "{applied}");
}

#[tokio::test]
async fn preview_survives_failed_pin() {
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
    let connection = ConnectionId(17);

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
            request(
                2,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview"}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("preview response");
    collect_until_turn_completed(&mut receiver).await;

    bridge
        .handle(
            connection,
            request(
                3,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview pin missing-file.rs"}]
                }),
            ),
            writer.clone(),
        )
        .await;
    receiver.recv().await.expect("pin response");
    let (_methods, pin_failed, _) = collect_until_turn_completed(&mut receiver).await;
    assert!(
        pin_failed.contains("Compact preview failed"),
        "{pin_failed}"
    );

    bridge
        .handle(
            connection,
            request(
                4,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "/compact-preview apply"}]
                }),
            ),
            writer,
        )
        .await;
    receiver.recv().await.expect("apply response");
    let (_methods, applied, _) = collect_until_turn_completed(&mut receiver).await;
    assert!(applied.contains("Compaction applied"), "{applied}");
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
