#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn executable(path: &std::path::Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

#[test]
fn doctor_verifies_versions_auth_model_and_remote_capability() {
    let temp = tempfile::tempdir().unwrap();
    let agent = temp.path().join("agent");
    let codex = temp.path().join("codex");
    executable(
        &agent,
        r#"
case "$*" in
  "--version") echo "Cursor Agent 2026.09" ;;
  "acp --help") echo "ACP v1 server" ;;
  "models") echo "auto cursor-model" ;;
  "status") echo "Logged in" ;;
  *) exit 2 ;;
esac
"#,
    );
    executable(
        &codex,
        r#"
case "$*" in
  "--version") echo "codex-cli 0.145.0" ;;
  "app-server --remote-control --help") echo "app-server help" ;;
  "login status") echo "Logged in using ChatGPT" ;;
  *) exit 2 ;;
esac
"#,
    );

    let original_path = std::env::var_os("PATH").unwrap_or_default();
    let joined_path = std::env::join_paths(
        std::iter::once(temp.path().to_path_buf()).chain(std::env::split_paths(&original_path)),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_codex-remote-bridge"))
        .args([
            "doctor",
            "--workspace",
            "/tmp",
            "--agent-bin",
            agent.to_str().unwrap(),
            "--model",
            "cursor-model",
        ])
        .env("PATH", joined_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Codex CLI: codex-cli 0.145.0"));
    assert!(stdout.contains("Remote Control capability: available"));
    assert!(!stdout.to_ascii_lowercase().contains("bearer"));
    assert!(!stdout.to_ascii_lowercase().contains("oauth token"));
}
