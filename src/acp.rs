use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::{Duration, timeout};
use tracing::{debug, warn};

type PendingResponse = oneshot::Sender<std::result::Result<Value, String>>;

struct Inner {
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    pending: Mutex<HashMap<String, PendingResponse>>,
    events: broadcast::Sender<Value>,
    next_id: AtomicI64,
}

#[derive(Clone)]
pub struct AcpClient {
    inner: Arc<Inner>,
}

impl AcpClient {
    pub async fn spawn(agent_bin: &Path, model: &str, workspace: &Path) -> Result<Self> {
        let mut command = Command::new(agent_bin);
        command
            .arg("--model")
            .arg(model)
            .arg("acp")
            .current_dir(workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().with_context(|| {
            format!(
                "cannot start Cursor ACP with {}",
                agent_bin.to_string_lossy()
            )
        })?;
        let stdin = child.stdin.take().context("Cursor ACP stdin unavailable")?;
        let stdout = child.stdout.take().context("Cursor ACP stdout unavailable")?;
        let stderr = child.stderr.take().context("Cursor ACP stderr unavailable")?;
        let (events, _) = broadcast::channel(256);

        let inner = Arc::new(Inner {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            pending: Mutex::new(HashMap::new()),
            events,
            next_id: AtomicI64::new(1),
        });

        Self::start_stdout_reader(&inner, stdout);
        Self::start_stderr_reader(stderr);

        let client = Self { inner };
        timeout(Duration::from_secs(30), client.initialize())
            .await
            .context("Cursor ACP initialize timed out")??;
        Ok(client)
    }

    fn start_stdout_reader(inner: &Arc<Inner>, stdout: tokio::process::ChildStdout) {
        let inner = Arc::clone(inner);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let message: Value = match serde_json::from_str(&line) {
                            Ok(value) => value,
                            Err(err) => {
                                warn!(%err, "ignored malformed Cursor ACP stdout frame");
                                continue;
                            }
                        };

                        if message.get("method").is_some() {
                            let _ = inner.events.send(message);
                            continue;
                        }

                        let Some(id) = message.get("id") else {
                            warn!("ignored Cursor ACP response without id");
                            continue;
                        };
                        let key = id_key(id);
                        if let Some(sender) = inner.pending.lock().await.remove(&key) {
                            let result = match message.get("error") {
                                Some(error) => Err(error.to_string()),
                                None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                            };
                            let _ = sender.send(result);
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(%err, "Cursor ACP stdout reader stopped");
                        break;
                    }
                }
            }

            let mut pending = inner.pending.lock().await;
            for (_, sender) in pending.drain() {
                let _ = sender.send(Err("Cursor ACP process closed stdout".into()));
            }
        });
    }

    fn start_stderr_reader(stderr: tokio::process::ChildStderr) {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                debug!(target: "cursor_acp", "{line}");
            }
        });
    }

    async fn initialize(&self) -> Result<()> {
        let response = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": false, "writeTextFile": false},
                        "terminal": false
                    },
                    "clientInfo": {
                        "name": "codex-remote-bridge",
                        "version": crate::BRIDGE_VERSION
                    }
                }),
            )
            .await?;
        if response.get("protocolVersion").and_then(Value::as_i64) != Some(1) {
            bail!("Cursor negotiated unsupported ACP protocol: {response}");
        }
        self.request("authenticate", json!({"methodId": "cursor_login"}))
            .await?;
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.events.subscribe()
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let key = id.to_string();
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(key.clone(), sender);

        if let Err(err) = self
            .write_frame(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }))
            .await
        {
            self.inner.pending.lock().await.remove(&key);
            return Err(err);
        }

        receiver
            .await
            .context("Cursor ACP response channel closed")?
            .map_err(|error| anyhow!("Cursor ACP request {method} failed: {error}"))
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.write_frame(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await
    }

    pub async fn respond(&self, id: Value, result: Value) -> Result<()> {
        self.write_frame(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
        .await
    }

    async fn write_frame(&self, frame: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(frame)?;
        bytes.push(b'\n');
        let mut stdin = self.inner.stdin.lock().await;
        stdin
            .write_all(&bytes)
            .await
            .context("cannot write Cursor ACP frame")?;
        stdin.flush().await.context("cannot flush Cursor ACP frame")
    }

    pub async fn new_session(&self, workspace: &Path) -> Result<String> {
        let result = self
            .request(
                "session/new",
                json!({
                    "cwd": workspace,
                    "mcpServers": []
                }),
            )
            .await?;
        let session_id = result
            .get("sessionId")
            .and_then(Value::as_str)
            .context("Cursor ACP session/new omitted sessionId")?
            .to_owned();

        if let Some(mode_id) = result
            .pointer("/modes/availableModes")
            .and_then(Value::as_array)
            .and_then(|modes| {
                modes.iter().find_map(|mode| {
                    (mode.get("id").and_then(Value::as_str) == Some("agent"))
                        .then_some("agent")
                })
            })
        {
            let _ = self
                .request(
                    "session/set_mode",
                    json!({"sessionId": session_id, "modeId": mode_id}),
                )
                .await;
        }
        Ok(session_id)
    }

    pub async fn load_session(&self, session_id: &str, workspace: &Path) -> Result<()> {
        self.request(
            "session/load",
            json!({
                "sessionId": session_id,
                "cwd": workspace,
                "mcpServers": []
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn cancel(&self, session_id: &str) -> Result<()> {
        self.notify("session/cancel", json!({"sessionId": session_id}))
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut child = self.inner.child.lock().await;
        child.kill().await.context("cannot stop Cursor ACP process")
    }
}

fn id_key(id: &Value) -> String {
    match id {
        Value::String(value) => format!("s:{value}"),
        _ => id.to_string(),
    }
}

pub fn default_agent_path() -> PathBuf {
    std::env::var_os("CURSOR_AGENT").map_or_else(
        || {
            std::env::var_os("HOME").map_or_else(
                || PathBuf::from("agent"),
                |home| PathBuf::from(home).join(".local/bin/agent"),
            )
        },
        PathBuf::from,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_keep_string_and_integer_namespaces_separate() {
        assert_eq!(id_key(&json!(7)), "7");
        assert_eq!(id_key(&json!("7")), "s:7");
    }
}

