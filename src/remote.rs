use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use codex_app_server_protocol::{RemoteControlConnectionStatus, RemoteControlPairingStartParams};
use codex_app_server_transport::{
    ConnectionId, RemoteControlHandle, RemoteControlPolicy, RemoteControlStartConfig,
    RemoteControlStartupMode, TransportEvent, start_remote_control,
};
use codex_login::{AuthCredentialsStoreMode, AuthKeyringBackendKind, AuthManager};
use codex_state::StateRuntime;
use serde_json::Value;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::bridge::Bridge;
use crate::rpc::RemoteWriter;

const DEFAULT_REMOTE_URL: &str = "https://chatgpt.com/backend-api";

pub struct RemoteRuntime {
    event_rx: mpsc::Receiver<TransportEvent>,
    handle: RemoteControlHandle,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl RemoteRuntime {
    pub async fn start(codex_home: &Path, bridge_home: &Path) -> Result<Self> {
        let auth_manager = Arc::new(
            AuthManager::new(
                codex_home.to_path_buf(),
                false,
                AuthCredentialsStoreMode::File,
                None,
                Some(DEFAULT_REMOTE_URL.to_owned()),
                AuthKeyringBackendKind::default(),
                None,
            )
            .await,
        );
        let state_db = StateRuntime::init(bridge_home.to_path_buf(), "openai".to_owned())
            .await
            .context("cannot initialize isolated remote-control state database")?;
        let installation_id = load_installation_id(bridge_home).await?;
        let (event_tx, event_rx) = mpsc::channel(128);
        let cancellation = CancellationToken::new();

        let (task, handle) = start_remote_control(
            RemoteControlStartConfig {
                remote_control_url: DEFAULT_REMOTE_URL.to_owned(),
                installation_id,
                policy: RemoteControlPolicy::Allowed,
            },
            Some(state_db),
            auth_manager,
            event_tx,
            cancellation.clone(),
            None,
            RemoteControlStartupMode::EnabledEphemeral,
        )
        .await
        .context("cannot start Codex remote-control transport")?;

        Ok(Self {
            event_rx,
            handle,
            cancellation,
            task,
        })
    }

    pub async fn wait_until_connected(&self) -> Result<()> {
        if self.handle.status().status == RemoteControlConnectionStatus::Connected {
            return Ok(());
        }
        let mut statuses = self.handle.status_receiver();
        timeout(Duration::from_secs(45), async {
            loop {
                statuses.changed().await.context("status channel closed")?;
                let status = statuses.borrow().clone();
                match status.status {
                    RemoteControlConnectionStatus::Connected => return Ok(()),
                    RemoteControlConnectionStatus::Errored => {
                        bail!("OpenAI remote-control relay reported an error")
                    }
                    RemoteControlConnectionStatus::Disabled => {
                        bail!("OpenAI remote-control relay is disabled")
                    }
                    RemoteControlConnectionStatus::Connecting => {}
                }
            }
        })
        .await
        .context("timed out connecting to OpenAI remote-control relay")?
    }

    pub async fn start_pairing(&self) -> Result<Value> {
        let response = self
            .handle
            .start_pairing(RemoteControlPairingStartParams { manual_code: true }, None)
            .await
            .context("cannot create remote-control pairing code")?;
        serde_json::to_value(response).context("cannot serialize pairing response")
    }

    pub async fn run(mut self, bridge: Arc<Bridge>) -> Result<()> {
        let mut writers: HashMap<ConnectionId, RemoteWriter> = HashMap::new();
        while let Some(event) = self.event_rx.recv().await {
            match event {
                TransportEvent::ConnectionOpened {
                    connection_id,
                    writer,
                    ..
                } => {
                    writers.insert(connection_id, writer);
                }
                TransportEvent::ConnectionClosed { connection_id } => {
                    writers.remove(&connection_id);
                    bridge.connection_closed(connection_id).await;
                }
                TransportEvent::IncomingMessage {
                    connection_id,
                    message,
                } => {
                    if let Some(writer) = writers.get(&connection_id).cloned() {
                        bridge.handle(connection_id, message, writer).await;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(self) {
        self.cancellation.cancel();
        let _ = self.task.await;
    }
}

async fn load_installation_id(home: &Path) -> Result<String> {
    fs::create_dir_all(home).await?;
    let path = home.join("installation_id");
    if let Ok(raw) = fs::read_to_string(&path).await {
        let trimmed = raw.trim();
        if let Ok(id) = Uuid::parse_str(trimmed) {
            return Ok(id.to_string());
        }
    }

    let id = Uuid::new_v4().to_string();
    fs::write(&path, &id)
        .await
        .with_context(|| format!("cannot persist installation id at {}", path.display()))?;
    Ok(id)
}

pub fn default_codex_home() -> PathBuf {
    std::env::var_os("CODEX_HOME").map_or_else(
        || {
            std::env::var_os("HOME")
                .map_or_else(|| PathBuf::from("."), PathBuf::from)
                .join(".codex")
        },
        PathBuf::from,
    )
}

pub fn default_bridge_home() -> PathBuf {
    std::env::var_os("CODEX_REMOTE_BRIDGE_HOME").map_or_else(
        || {
            std::env::var_os("HOME")
                .map_or_else(|| PathBuf::from("."), PathBuf::from)
                .join(".codex-remote-bridge")
        },
        PathBuf::from,
    )
}
