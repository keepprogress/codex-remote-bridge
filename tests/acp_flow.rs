#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use codex_remote_bridge::acp::AcpClient;
use serde_json::json;

fn fake_agent(script_body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fake-agent");
    std::fs::write(
        &path,
        format!(
            "#!/usr/bin/env python3\nimport json, sys\n{}\n",
            script_body
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    (temp, path)
}

#[tokio::test]
async fn fake_acp_child_completes_session_and_streams() {
    let (_temp, agent) = fake_agent(
        r#"
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        result = {"protocolVersion": 1}
    elif method == "authenticate":
        result = {}
    elif method == "session/new":
        result = {
            "sessionId": "sess_fake",
            "modes": {
                "currentModeId": "agent",
                "availableModes": [{"id": "agent", "name": "Agent"}]
            }
        }
    elif method == "session/set_mode":
        result = {}
    elif method == "session/prompt":
        print(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "sess_fake",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "hello"}
                }
            }
        }), flush=True)
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    if "id" in msg:
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
"#,
    );

    let client = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .unwrap();
    let session = client
        .new_session(std::path::Path::new("/tmp"))
        .await
        .unwrap();
    assert_eq!(session, "sess_fake");

    let mut events = client.subscribe();
    let result = client
        .request(
            "session/prompt",
            json!({
                "sessionId": session,
                "prompt": [{"type": "text", "text": "test"}]
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["stopReason"], "end_turn");
    let event = events.recv().await.unwrap();
    assert_eq!(
        event.pointer("/params/update/content/text"),
        Some(&json!("hello"))
    );
}

#[tokio::test]
async fn child_exit_during_handshake_is_reported() {
    let (_temp, agent) = fake_agent("sys.exit(0)");
    let error = AcpClient::spawn(&agent, "auto", std::path::Path::new("/tmp"))
        .await
        .err()
        .expect("handshake should fail");
    assert!(!format!("{error:#}").is_empty());
}
