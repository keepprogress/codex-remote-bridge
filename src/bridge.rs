use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use codex_app_server_protocol::{
    JSONRPCError, JSONRPCMessage, JSONRPCNotification, JSONRPCRequest, RequestId,
    SkillsExtraRootsSetResponse,
};
use codex_app_server_transport::ConnectionId;
use serde_json::{Value, json};
use tokio::sync::{Mutex, oneshot};
use tokio::time::{Duration, timeout};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::acp::AcpClient;
use crate::approval::{acp_permission_result, permission_kind, permission_title};
use crate::compact::{
    Capsule, CompactDirectives, CompactPreviewCommand, PendingCapsule, PinnedFile, PreviewArgs,
    TodoItem, harvest_git, merge_todos, overlay_harvested, parse_capsule_yaml,
    parse_compact_preview, pin_file, preview_message, seed_prompt, summary_prompt,
    todos_from_event, unpin_directive, unpin_files,
};
use crate::process::ProcessManager;
use crate::rpc::{
    RemoteWriter, now_millis, now_seconds, send_error, send_notification, send_response,
    send_server_request, trace_summary,
};
use crate::state::{PersistedState, PersistedThread, StateStore};

struct PromptGuard<'a> {
    active: &'a std::sync::Mutex<HashSet<String>>,
    session_id: String,
}

impl Drop for PromptGuard<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.session_id);
    }
}

struct PendingMobileRequest {
    connection_id: ConnectionId,
    sender: oneshot::Sender<Value>,
}

const COMPACTION_TIMEOUT: Duration = Duration::from_secs(120);
const COMPACTION_SUMMARY_LIMIT: usize = 256 * 1024;

pub struct Bridge {
    acp: AcpClient,
    workspace: PathBuf,
    codex_home: PathBuf,
    model: String,
    store: StateStore,
    state: Mutex<PersistedState>,
    mobile_requests: Mutex<HashMap<String, PendingMobileRequest>>,
    compactions: Mutex<HashSet<String>>,
    pending_capsules: Mutex<HashMap<String, PendingCapsule>>,
    todos: Mutex<HashMap<String, Vec<TodoItem>>>,
    active_prompts: std::sync::Mutex<HashSet<String>>,
    processes: ProcessManager,
    next_mobile_id: AtomicI64,
    trace_wire: bool,
}

impl Bridge {
    pub async fn new(
        acp: AcpClient,
        workspace: PathBuf,
        codex_home: PathBuf,
        model: String,
        store: StateStore,
        trace_wire: bool,
    ) -> Result<Self> {
        let state = store.load().await?;
        Ok(Self {
            acp,
            workspace,
            codex_home,
            model,
            store,
            state: Mutex::new(state),
            mobile_requests: Mutex::new(HashMap::new()),
            compactions: Mutex::new(HashSet::new()),
            pending_capsules: Mutex::new(HashMap::new()),
            todos: Mutex::new(HashMap::new()),
            active_prompts: std::sync::Mutex::new(HashSet::new()),
            processes: ProcessManager::default(),
            next_mobile_id: AtomicI64::new(1_000_000),
            trace_wire,
        })
    }

    pub async fn handle(
        self: &Arc<Self>,
        connection_id: ConnectionId,
        message: JSONRPCMessage,
        writer: RemoteWriter,
    ) {
        if self.trace_wire
            && let Ok(value) = serde_json::to_value(&message)
        {
            info!(wire = %trace_summary("remote->bridge", &value));
        }

        let result = match message {
            JSONRPCMessage::Request(request) => {
                let request_id = request.id.clone();
                match self
                    .handle_request(connection_id, request, writer.clone())
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        error!(%err, "bridge request failed before a usable response");
                        send_error(
                            &writer,
                            request_id,
                            -32_603,
                            format!("bridge request failed: {err}"),
                        )
                        .await
                    }
                }
            }
            JSONRPCMessage::Notification(notification) => self.handle_notification(notification),
            JSONRPCMessage::Response(response) => {
                self.resolve_mobile_request(response.id, response.result)
                    .await;
                Ok(())
            }
            JSONRPCMessage::Error(error) => {
                self.resolve_mobile_error(error).await;
                Ok(())
            }
        };
        if let Err(err) = result {
            error!(%err, "bridge failed to handle remote message");
        }
    }

    fn handle_notification(&self, notification: JSONRPCNotification) -> Result<()> {
        match notification.method.as_str() {
            "initialized" => Ok(()),
            method => {
                warn!(method, "ignored unsupported client notification");
                Ok(())
            }
        }
    }

    async fn handle_request(
        self: &Arc<Self>,
        connection_id: ConnectionId,
        request: JSONRPCRequest,
        writer: RemoteWriter,
    ) -> Result<()> {
        let id = request.id;
        let params = request.params.unwrap_or_else(|| json!({}));
        match request.method.as_str() {
            "initialize" => {
                let codex_home = self
                    .codex_home
                    .canonicalize()
                    .unwrap_or_else(|_| self.codex_home.clone());
                send_response(
                    &writer,
                    id,
                    json!({
                        "userAgent": format!("codex-remote-bridge/{}", crate::BRIDGE_VERSION),
                        "codexHome": codex_home,
                        "platformFamily": std::env::consts::FAMILY,
                        "platformOs": std::env::consts::OS,
                    }),
                )
                .await
            }
            "thread/start" => self.thread_start(id, writer).await,
            "thread/resume" => self.thread_resume(id, params, writer).await,
            "thread/read" => self.thread_read(id, params, writer).await,
            "thread/list" => self.thread_list(id, writer).await,
            "thread/goal/get" => send_response(&writer, id, json!({"goal": null})).await,
            "thread/compact/start" => self.thread_compact_start(id, params, writer).await,
            "turn/start" => self.turn_start(connection_id, id, params, writer).await,
            "turn/interrupt" => self.turn_interrupt(id, params, writer).await,
            "turn/steer" => {
                send_error(
                    &writer,
                    id,
                    -32_001,
                    "turn/steer is unavailable because ACP v1 has no steering primitive",
                )
                .await
            }
            "model/list" => send_response(&writer, id, model_list_result(&self.model)).await,
            "skills/extraRoots/set" => {
                send_response(&writer, id, protocol_json(&SkillsExtraRootsSetResponse {})).await
            }
            "skills/list" | "app/list" | "mcpServer/list" | "threadSection/list" => {
                send_response(&writer, id, json!({"data": [], "nextCursor": null})).await
            }
            "config/read" => {
                send_response(
                    &writer,
                    id,
                    json!({
                        "config": {
                            "model": self.model,
                            "model_provider": "cursor",
                            "approval_policy": "on-request",
                            "approvals_reviewer": "user",
                            "sandbox_mode": "workspace-write"
                        },
                        "origins": {},
                        "layers": null
                    }),
                )
                .await
            }
            "configRequirements/read" => {
                send_response(&writer, id, json!({"requirements": null})).await
            }
            "collaborationMode/list" => {
                send_response(
                    &writer,
                    id,
                    json!({
                        "data": [
                            {
                                "name": "Plan",
                                "mode": "plan",
                                "model": null,
                                "reasoning_effort": "medium"
                            },
                            {
                                "name": "Default",
                                "mode": "default",
                                "model": null
                            }
                        ]
                    }),
                )
                .await
            }
            "plugin/installed" => {
                send_response(
                    &writer,
                    id,
                    json!({
                        "marketplaces": [],
                        "marketplaceLoadErrors": []
                    }),
                )
                .await
            }
            "config/batchWrite" => {
                let file_path = params
                    .get("filePath")
                    .and_then(Value::as_str)
                    .map(PathBuf::from)
                    .unwrap_or_else(|| self.codex_home.join("config.toml"));
                send_response(
                    &writer,
                    id,
                    json!({
                        "status": "ok",
                        "version": "codex-remote-bridge",
                        "filePath": file_path,
                        "overriddenMetadata": null
                    }),
                )
                .await
            }
            "process/spawn" => {
                self.processes
                    .spawn(connection_id, id, params, writer)
                    .await
            }
            "process/writeStdin" => {
                self.processes
                    .write_stdin(connection_id, id, params, writer)
                    .await
            }
            "process/kill" => self.processes.kill(connection_id, id, params, writer).await,
            "process/resizePty" => {
                self.processes
                    .resize_pty(connection_id, id, params, writer)
                    .await
            }
            "fs/createDirectory" => {
                let path = PathBuf::from(required_string(&params, "path")?);
                if params
                    .get("recursive")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                {
                    tokio::fs::create_dir_all(&path).await?;
                } else {
                    tokio::fs::create_dir(&path).await?;
                }
                send_response(&writer, id, json!({})).await
            }
            "thread/unsubscribe" => send_response(&writer, id, json!({})).await,
            method => {
                warn!(method, "remote client called unsupported method");
                send_error(
                    &writer,
                    id,
                    -32_601,
                    format!("method not implemented by Cursor ACP bridge: {method}"),
                )
                .await
            }
        }
    }

    async fn thread_start(&self, id: RequestId, writer: RemoteWriter) -> Result<()> {
        let session_id = self.acp.new_session(&self.workspace).await?;
        let thread_id = Uuid::now_v7().to_string();
        let now = now_seconds();
        {
            let mut state = self.state.lock().await;
            state.threads.insert(
                thread_id.clone(),
                PersistedThread {
                    acp_session_id: session_id,
                    workspace: self.workspace.clone(),
                    created_at: now,
                    updated_at: now,
                },
            );
            self.store.save(&state).await?;
        }
        let thread = self.thread_json(&thread_id, false).await?;
        send_response(
            &writer,
            id,
            json!({
                "thread": thread,
                "model": self.model,
                "modelProvider": "cursor",
                "serviceTier": null,
                "cwd": self.workspace,
                "runtimeWorkspaceRoots": [self.workspace],
                "instructionSources": [],
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": {"type": "workspaceWrite"},
                "activePermissionProfile": null,
                "reasoningEffort": null,
                "multiAgentMode": "explicitRequestOnly"
            }),
        )
        .await?;
        send_notification(&writer, "thread/started", json!({"thread": thread})).await
    }

    async fn thread_resume(
        &self,
        id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> Result<()> {
        let thread_id = required_string(&params, "threadId")?;
        let persisted = self.persisted_thread(&thread_id).await?;
        if self
            .acp
            .load_session(&persisted.acp_session_id, &persisted.workspace)
            .await
            .is_err()
        {
            let new_session = self.acp.new_session(&persisted.workspace).await?;
            let mut state = self.state.lock().await;
            let thread = state
                .threads
                .get_mut(&thread_id)
                .context("thread disappeared while resuming")?;
            thread.acp_session_id = new_session;
            thread.updated_at = now_seconds();
            self.store.save(&state).await?;
        }
        let thread = self.thread_json(&thread_id, true).await?;
        send_response(&writer, id, json!({"thread": thread})).await
    }

    async fn thread_read(&self, id: RequestId, params: Value, writer: RemoteWriter) -> Result<()> {
        let thread_id = required_string(&params, "threadId")?;
        let include_turns = params
            .get("includeTurns")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let thread = self.thread_json(&thread_id, include_turns).await?;
        send_response(&writer, id, json!({"thread": thread})).await
    }

    async fn thread_list(&self, id: RequestId, writer: RemoteWriter) -> Result<()> {
        let ids = {
            let state = self.state.lock().await;
            let mut ids: Vec<_> = state.threads.keys().cloned().collect();
            ids.sort();
            ids
        };
        let mut data = Vec::with_capacity(ids.len());
        for thread_id in ids {
            data.push(self.thread_json(&thread_id, false).await?);
        }
        send_response(&writer, id, json!({"data": data, "nextCursor": null})).await
    }

    async fn thread_compact_start(
        self: &Arc<Self>,
        id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> Result<()> {
        let thread_id = required_string(&params, "threadId")?;
        let persisted = self.persisted_thread(&thread_id).await?;
        {
            let mut compactions = self.compactions.lock().await;
            if !compactions.insert(thread_id.clone()) {
                return send_error(
                    &writer,
                    id,
                    -32_001,
                    format!("thread {thread_id} is already being compacted"),
                )
                .await;
            }
        }

        let turn_id = Uuid::now_v7().to_string();
        let item_id = Uuid::now_v7().to_string();
        let turn = turn_json(&turn_id, "inProgress", None);
        let item = json!({"type": "contextCompaction", "id": item_id});
        let setup = async {
            send_response(&writer, id, json!({})).await?;
            send_notification(
                &writer,
                "turn/started",
                json!({"threadId": thread_id, "turn": turn}),
            )
            .await?;
            send_notification(
                &writer,
                "item/started",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": now_millis()
                }),
            )
            .await
        }
        .await;
        if let Err(err) = setup {
            self.compactions.lock().await.remove(&thread_id);
            return Err(err);
        }

        let bridge = Arc::clone(self);
        tokio::spawn(async move {
            let result = bridge.compact_cursor_session(&thread_id, &persisted).await;
            bridge.compactions.lock().await.remove(&thread_id);

            match result {
                Ok(()) => {
                    let _ = send_notification(
                        &writer,
                        "item/completed",
                        json!({
                            "item": item,
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "completedAtMs": now_millis()
                        }),
                    )
                    .await;
                    let _ = send_notification(
                        &writer,
                        "thread/compacted",
                        json!({"threadId": thread_id, "turnId": turn_id}),
                    )
                    .await;
                    let completed = turn_json(&turn_id, "completed", None);
                    let _ = send_notification(
                        &writer,
                        "turn/completed",
                        json!({"threadId": thread_id, "turn": completed}),
                    )
                    .await;
                }
                Err(err) => {
                    error!(%err, %thread_id, %turn_id, "Cursor session compaction failed");
                    let _ = send_notification(
                        &writer,
                        "item/completed",
                        json!({
                            "item": item,
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "completedAtMs": now_millis()
                        }),
                    )
                    .await;
                    let failed = turn_json(&turn_id, "failed", Some(err.to_string()));
                    let _ = send_notification(
                        &writer,
                        "turn/completed",
                        json!({"threadId": thread_id, "turn": failed}),
                    )
                    .await;
                }
            }
        });
        Ok(())
    }

    async fn compact_cursor_session(
        &self,
        thread_id: &str,
        persisted: &PersistedThread,
    ) -> Result<()> {
        let pending = self
            .pending_capsules
            .lock()
            .await
            .get(thread_id)
            .map(|pending| pending.capsule.clone());
        let capsule = match pending {
            Some(capsule) => capsule,
            None => {
                self.build_capsule(persisted, &CompactDirectives::default(), Vec::new())
                    .await?
            }
        };
        self.commit_rollover(thread_id, persisted, &capsule).await?;
        Ok(())
    }

    async fn build_capsule(
        &self,
        persisted: &PersistedThread,
        directives: &CompactDirectives,
        pinned_files: Vec<PinnedFile>,
    ) -> Result<Capsule> {
        let git_state = harvest_git(&persisted.workspace).await;
        let todos = self.session_todos(&persisted.acp_session_id).await;
        let prompt = summary_prompt(&git_state, &todos, directives)?;
        let summary = self
            .run_hidden_prompt(&persisted.acp_session_id, &prompt)
            .await
            .context("Cursor failed to summarize the existing session")?;
        if summary.trim().is_empty() {
            return Err(anyhow!("Cursor returned an empty compaction summary"));
        }
        let todos = self.session_todos(&persisted.acp_session_id).await;
        let parsed = parse_capsule_yaml(&summary)?;
        Ok(overlay_harvested(parsed, git_state, todos, pinned_files))
    }

    async fn commit_rollover(
        &self,
        thread_id: &str,
        persisted: &PersistedThread,
        capsule: &Capsule,
    ) -> Result<String> {
        let replacement_session = self.acp.new_session(&persisted.workspace).await?;
        let seeded = self
            .run_hidden_prompt(&replacement_session, &seed_prompt(capsule)?)
            .await
            .context("Cursor failed to seed the replacement session")?;
        if seeded.trim() != "CONTEXT_READY" {
            return Err(anyhow!(
                "replacement Cursor session did not acknowledge compacted context"
            ));
        }

        let mut state = self.state.lock().await;
        let current = state
            .threads
            .get(thread_id)
            .context("thread disappeared while compaction was running")?;
        if current.acp_session_id != persisted.acp_session_id {
            return Err(anyhow!(
                "thread backend changed while compaction was running"
            ));
        }
        let mut next_state = state.clone();
        let replacement = next_state
            .threads
            .get_mut(thread_id)
            .context("thread disappeared while committing compaction")?;
        replacement.acp_session_id = replacement_session.clone();
        replacement.updated_at = now_seconds();
        self.store.save(&next_state).await?;
        *state = next_state;
        self.transfer_todos(&persisted.acp_session_id, &replacement_session)
            .await;
        self.pending_capsules.lock().await.remove(thread_id);
        Ok(replacement_session)
    }

    async fn session_todos(&self, session_id: &str) -> Vec<TodoItem> {
        self.todos
            .lock()
            .await
            .get(session_id)
            .cloned()
            .unwrap_or_default()
    }

    async fn transfer_todos(&self, from: &str, to: &str) {
        let mut todos = self.todos.lock().await;
        if let Some(list) = todos.remove(from) {
            todos.insert(to.to_owned(), list);
        }
    }

    async fn ingest_todo_event(&self, fallback_session: &str, event: &Value) -> bool {
        let params = event.get("params").cloned().unwrap_or(Value::Null);
        let Some(session) = self.resolve_todo_session(fallback_session, &params) else {
            return false;
        };
        let Some((merge, incoming)) = todos_from_event(&params) else {
            return false;
        };
        let mut todos = self.todos.lock().await;
        merge_todos(todos.entry(session.clone()).or_default(), incoming, merge);
        session == fallback_session
    }

    fn resolve_todo_session(&self, fallback_session: &str, params: &Value) -> Option<String> {
        if let Some(session) = params.get("sessionId").and_then(Value::as_str) {
            return Some(session.to_owned());
        }
        let active = self
            .active_prompts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.len() == 1 {
            return active.iter().next().cloned();
        }
        if active.is_empty() {
            return Some(fallback_session.to_owned());
        }
        warn!(
            fallback_session,
            active = active.len(),
            "ignored sessionless cursor/update_todos while multiple ACP prompts are active"
        );
        None
    }

    fn enter_prompt(&self, session_id: &str) -> PromptGuard<'_> {
        self.active_prompts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_owned());
        PromptGuard {
            active: &self.active_prompts,
            session_id: session_id.to_owned(),
        }
    }

    async fn take_todo_event(&self, session_id: &str, event: &Value) -> Result<()> {
        let claimed = self.ingest_todo_event(session_id, event).await;
        if claimed && let Some(acp_id) = event.get("id").cloned() {
            self.acp.respond(acp_id, json!({})).await?;
        }
        Ok(())
    }

    async fn take_sessionless_todos(&self, session_id: &str, event: &Value) -> Result<()> {
        if event.get("method").and_then(Value::as_str) != Some("cursor/update_todos") {
            return Ok(());
        }
        if event
            .pointer("/params/sessionId")
            .and_then(Value::as_str)
            .is_some()
        {
            return Ok(());
        }
        self.take_todo_event(session_id, event).await
    }

    async fn try_lock_compaction(&self, thread_id: &str) -> Result<()> {
        let mut compactions = self.compactions.lock().await;
        if !compactions.insert(thread_id.to_owned()) {
            bail!("thread {thread_id} is already being compacted");
        }
        Ok(())
    }

    async fn unlock_compaction(&self, thread_id: &str) {
        self.compactions.lock().await.remove(thread_id);
    }

    async fn run_hidden_prompt(&self, session_id: &str, prompt: &str) -> Result<String> {
        let _prompt_guard = self.enter_prompt(session_id);
        let mut events = self.acp.subscribe();
        let request = self.acp.request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": prompt}]
            }),
        );
        tokio::pin!(request);
        let deadline = tokio::time::sleep(COMPACTION_TIMEOUT);
        tokio::pin!(deadline);
        let mut text = String::new();

        loop {
            tokio::select! {
                biased;
                event = events.recv() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                            return Err(anyhow!("Cursor ACP compaction event receiver lagged by {count}"));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(anyhow!("Cursor ACP event stream closed during compaction"));
                        }
                    };
                    if !event_belongs_to_session(&event, session_id) {
                        self.take_sessionless_todos(session_id, &event).await?;
                        continue;
                    }
                    match event.get("method").and_then(Value::as_str) {
                        Some("session/update") => {
                            let update = event.pointer("/params/update").unwrap_or(&Value::Null);
                            if update.get("sessionUpdate").and_then(Value::as_str)
                                == Some("agent_message_chunk")
                                && let Some(delta) = update.pointer("/content/text").and_then(Value::as_str)
                            {
                                text.push_str(delta);
                                if text.len() > COMPACTION_SUMMARY_LIMIT {
                                    self.acp.cancel(session_id).await?;
                                    return Err(anyhow!(
                                        "Cursor compaction output exceeded {COMPACTION_SUMMARY_LIMIT} bytes"
                                    ));
                                }
                            }
                        }
                        Some("session/request_permission") => {
                            if let Some(acp_id) = event.get("id").cloned() {
                                self.acp.respond(
                                    acp_id,
                                    json!({"outcome": {"outcome": "cancelled", "reason": "tools are disabled during compaction"}}),
                                ).await?;
                            }
                        }
                        Some("cursor/create_plan" | "cursor/ask_question") => {
                            if let Some(acp_id) = event.get("id").cloned() {
                                self.acp.respond(
                                    acp_id,
                                    json!({"outcome": {"outcome": "cancelled", "reason": "interactive requests are disabled during compaction"}}),
                                ).await?;
                            }
                        }
                        Some("cursor/update_todos") => {
                            self.take_todo_event(session_id, &event).await?;
                        }
                        _ => {}
                    }
                }
                result = &mut request => {
                    result?;
                    return Ok(text);
                }
                _ = &mut deadline => {
                    self.acp.cancel(session_id).await?;
                    return Err(anyhow!("Cursor session compaction timed out after 120 seconds"));
                }
            }
        }
    }

    async fn turn_start(
        self: &Arc<Self>,
        connection_id: ConnectionId,
        id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> Result<()> {
        let thread_id = required_string(&params, "threadId")?;
        if self.compactions.lock().await.contains(&thread_id) {
            return send_error(
                &writer,
                id,
                -32_001,
                format!("thread {thread_id} is being compacted"),
            )
            .await;
        }
        let persisted = self.persisted_thread(&thread_id).await?;
        let prompt = extract_prompt(&params)?;
        let turn_id = Uuid::now_v7().to_string();
        let turn = turn_json(&turn_id, "inProgress", None);
        send_response(&writer, id, json!({"turn": turn})).await?;
        send_notification(
            &writer,
            "turn/started",
            json!({"threadId": thread_id, "turn": turn}),
        )
        .await?;

        let user_item = json!({
            "type": "userMessage",
            "id": Uuid::now_v7().to_string(),
            "clientId": params.get("clientUserMessageId").cloned().unwrap_or(Value::Null),
            "content": params.get("input").cloned().unwrap_or_else(|| json!([]))
        });
        send_notification(
            &writer,
            "item/started",
            json!({
                "item": user_item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": now_millis()
            }),
        )
        .await?;
        send_notification(
            &writer,
            "item/completed",
            json!({
                "item": user_item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": now_millis()
            }),
        )
        .await?;

        if let Some(parsed) = parse_compact_preview(&prompt) {
            let bridge = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(err) = bridge
                    .run_compact_preview_turn(
                        persisted,
                        thread_id.clone(),
                        turn_id.clone(),
                        parsed,
                        writer.clone(),
                    )
                    .await
                {
                    error!(%err, %thread_id, %turn_id, "compact preview turn failed");
                    let failed = turn_json(&turn_id, "failed", Some(err.to_string()));
                    let _ = send_notification(
                        &writer,
                        "turn/completed",
                        json!({"threadId": thread_id, "turn": failed}),
                    )
                    .await;
                }
            });
            return Ok(());
        }

        let bridge = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = bridge
                .run_turn(
                    connection_id,
                    persisted.acp_session_id,
                    thread_id.clone(),
                    turn_id.clone(),
                    prompt,
                    writer.clone(),
                )
                .await
            {
                error!(%err, %thread_id, %turn_id, "Cursor turn failed");
                let failed = turn_json(&turn_id, "failed", Some(err.to_string()));
                let _ = send_notification(
                    &writer,
                    "turn/completed",
                    json!({"threadId": thread_id, "turn": failed}),
                )
                .await;
            }
        });
        Ok(())
    }

    async fn run_compact_preview_turn(
        &self,
        persisted: PersistedThread,
        thread_id: String,
        turn_id: String,
        parsed: Result<CompactPreviewCommand>,
        writer: RemoteWriter,
    ) -> Result<()> {
        let command = match parsed {
            Ok(command) => command,
            Err(err) => {
                return self
                    .finish_local_turn(
                        &writer,
                        &thread_id,
                        &turn_id,
                        &format!("Compact preview failed: {err}"),
                    )
                    .await;
            }
        };
        let lock = !matches!(command, CompactPreviewCommand::Cancel);
        if lock && let Err(err) = self.try_lock_compaction(&thread_id).await {
            return self
                .finish_local_turn(
                    &writer,
                    &thread_id,
                    &turn_id,
                    &format!("Compact preview failed: {err}"),
                )
                .await;
        }
        let message = match self
            .execute_compact_preview(&thread_id, &persisted, command)
            .await
        {
            Ok(text) => text,
            Err(err) => format!("Compact preview failed: {err}"),
        };
        if lock {
            self.unlock_compaction(&thread_id).await;
        }
        self.finish_local_turn(&writer, &thread_id, &turn_id, &message)
            .await
    }

    async fn execute_compact_preview(
        &self,
        thread_id: &str,
        persisted: &PersistedThread,
        command: CompactPreviewCommand,
    ) -> Result<String> {
        match command {
            CompactPreviewCommand::Cancel => {
                self.pending_capsules.lock().await.remove(thread_id);
                Ok("Compaction preview cancelled. The Cursor session was not rolled over.".into())
            }
            CompactPreviewCommand::Apply => {
                let capsule = self
                    .pending_capsules
                    .lock()
                    .await
                    .get(thread_id)
                    .map(|pending| pending.capsule.clone())
                    .ok_or_else(|| {
                        anyhow!("no pending compaction preview; run /compact-preview first")
                    })?;
                self.commit_rollover(thread_id, persisted, &capsule).await?;
                Ok(
                    "Compaction applied. This ChatGPT thread now maps to a replacement Cursor session."
                        .into(),
                )
            }
            CompactPreviewCommand::Set(yaml) => {
                let parsed = parse_capsule_yaml(&yaml)?;
                let git_state = harvest_git(&persisted.workspace).await;
                let todos = self.session_todos(&persisted.acp_session_id).await;
                let mut pending = self
                    .pending_capsules
                    .lock()
                    .await
                    .get(thread_id)
                    .cloned()
                    .unwrap_or_default();
                let capsule = overlay_harvested(
                    parsed,
                    git_state,
                    todos,
                    pending.capsule.pinned_files.clone(),
                );
                pending.capsule = capsule.clone();
                self.pending_capsules
                    .lock()
                    .await
                    .insert(thread_id.to_owned(), pending);
                preview_message(&capsule)
            }
            CompactPreviewCommand::Preview(args) => {
                self.refresh_preview(thread_id, persisted, args).await
            }
        }
    }

    async fn refresh_preview(
        &self,
        thread_id: &str,
        persisted: &PersistedThread,
        args: PreviewArgs,
    ) -> Result<String> {
        let previous = self.pending_capsules.lock().await.get(thread_id).cloned();
        let had_pending = previous.is_some();
        let mut pending = previous.unwrap_or_default();
        pending.directives.merge(&CompactDirectives {
            keep: args.keep.clone(),
            drop: args.drop.clone(),
            pins: args.pins.clone(),
        });
        for path in &args.unpins {
            unpin_directive(&mut pending.directives.pins, &persisted.workspace, path);
            unpin_files(
                &mut pending.capsule.pinned_files,
                &persisted.workspace,
                path,
            );
        }

        let mut pinned = pending.capsule.pinned_files.clone();
        for path in &args.pins {
            let file = pin_file(&persisted.workspace, path).await?;
            unpin_files(&mut pinned, &persisted.workspace, &file.path);
            pinned.push(file);
        }

        let rerun = !had_pending || args.conversation_changed();
        let capsule = if rerun {
            self.build_capsule(persisted, &pending.directives, pinned)
                .await?
        } else {
            let git_state = harvest_git(&persisted.workspace).await;
            let todos = self.session_todos(&persisted.acp_session_id).await;
            overlay_harvested(pending.capsule, git_state, todos, pinned)
        };
        pending.capsule = capsule.clone();
        self.pending_capsules
            .lock()
            .await
            .insert(thread_id.to_owned(), pending);
        preview_message(&capsule)
    }

    async fn finish_local_turn(
        &self,
        writer: &RemoteWriter,
        thread_id: &str,
        turn_id: &str,
        text: &str,
    ) -> Result<()> {
        let item_id = Uuid::now_v7().to_string();
        send_notification(
            writer,
            "item/started",
            json!({
                "item": {
                    "type": "agentMessage",
                    "id": item_id,
                    "text": "",
                    "phase": null,
                    "memoryCitation": null
                },
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": now_millis()
            }),
        )
        .await?;
        send_notification(
            writer,
            "item/agentMessage/delta",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "delta": text
            }),
        )
        .await?;
        send_notification(
            writer,
            "item/completed",
            json!({
                "item": {
                    "type": "agentMessage",
                    "id": item_id,
                    "text": text,
                    "phase": null,
                    "memoryCitation": null
                },
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": now_millis()
            }),
        )
        .await?;
        send_notification(
            writer,
            "turn/completed",
            json!({"threadId": thread_id, "turn": turn_json(turn_id, "completed", None)}),
        )
        .await?;
        let mut state = self.state.lock().await;
        if let Some(thread) = state.threads.get_mut(thread_id) {
            thread.updated_at = now_seconds();
            self.store.save(&state).await?;
        }
        Ok(())
    }

    async fn run_turn(
        &self,
        connection_id: ConnectionId,
        session_id: String,
        thread_id: String,
        turn_id: String,
        prompt: String,
        writer: RemoteWriter,
    ) -> Result<()> {
        let _prompt_guard = self.enter_prompt(&session_id);
        let mut events = self.acp.subscribe();
        let prompt_request = self.acp.request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": prompt}]
            }),
        );
        tokio::pin!(prompt_request);

        let agent_item_id = Uuid::now_v7().to_string();
        let reasoning_item_id = Uuid::now_v7().to_string();
        let mut agent_started = false;
        let mut reasoning_started = false;
        let mut agent_text = String::new();
        let mut reasoning_text = String::new();
        let mut tools: HashMap<String, Value> = HashMap::new();

        let prompt_result = loop {
            tokio::select! {
                biased;
                event = events.recv() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                            warn!(count, "Cursor ACP event receiver lagged");
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(anyhow!("Cursor ACP event stream closed"));
                        }
                    };
                    if !event_belongs_to_session(&event, &session_id) {
                        self.take_sessionless_todos(&session_id, &event).await?;
                        continue;
                    }
                    match event.get("method").and_then(Value::as_str) {
                        Some("session/update") => {
                            let update = event.pointer("/params/update").cloned().unwrap_or(Value::Null);
                            self.handle_session_update(
                                &writer,
                                &thread_id,
                                &turn_id,
                                &agent_item_id,
                                &reasoning_item_id,
                                &mut agent_started,
                                &mut reasoning_started,
                                &mut agent_text,
                                &mut reasoning_text,
                                &mut tools,
                                update,
                            ).await?;
                        }
                        Some("session/request_permission") => {
                            self.handle_permission(
                                connection_id,
                                &writer,
                                &thread_id,
                                &turn_id,
                                &event,
                            ).await?;
                        }
                        Some("cursor/create_plan" | "cursor/ask_question") => {
                            if let Some(acp_id) = event.get("id").cloned() {
                                self.acp.respond(
                                    acp_id,
                                    json!({"outcome": {"outcome": "cancelled", "reason": "ChatGPT Remote cannot safely represent this Cursor interaction"}})
                                ).await?;
                            }
                        }
                        Some("cursor/update_todos") => {
                            self.take_todo_event(&session_id, &event).await?;
                        }
                        _ => {}
                    }
                },
                result = &mut prompt_request => break result,
            }
        }?;

        if agent_started {
            send_notification(
                &writer,
                "item/completed",
                json!({
                    "item": {
                        "type": "agentMessage",
                        "id": agent_item_id,
                        "text": agent_text,
                        "phase": null,
                        "memoryCitation": null
                    },
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": now_millis()
                }),
            )
            .await?;
        }
        if reasoning_started {
            send_notification(
                &writer,
                "item/completed",
                json!({
                    "item": {
                        "type": "reasoning",
                        "id": reasoning_item_id,
                        "summary": [reasoning_text],
                        "content": []
                    },
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": now_millis()
                }),
            )
            .await?;
        }

        let stop_reason = prompt_result
            .get("stopReason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn");
        let status = if stop_reason == "cancelled" {
            "interrupted"
        } else {
            "completed"
        };
        let completed = turn_json(&turn_id, status, None);
        send_notification(
            &writer,
            "turn/completed",
            json!({"threadId": thread_id, "turn": completed}),
        )
        .await?;

        let mut state = self.state.lock().await;
        if let Some(thread) = state.threads.get_mut(&thread_id) {
            thread.updated_at = now_seconds();
            self.store.save(&state).await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_session_update(
        &self,
        writer: &RemoteWriter,
        thread_id: &str,
        turn_id: &str,
        agent_item_id: &str,
        reasoning_item_id: &str,
        agent_started: &mut bool,
        reasoning_started: &mut bool,
        agent_text: &mut String,
        reasoning_text: &mut String,
        tools: &mut HashMap<String, Value>,
        update: Value,
    ) -> Result<()> {
        match update.get("sessionUpdate").and_then(Value::as_str) {
            Some("agent_message_chunk") => {
                let delta = update
                    .pointer("/content/text")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !*agent_started {
                    *agent_started = true;
                    send_notification(
                        writer,
                        "item/started",
                        json!({
                            "item": {
                                "type": "agentMessage",
                                "id": agent_item_id,
                                "text": "",
                                "phase": null,
                                "memoryCitation": null
                            },
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "startedAtMs": now_millis()
                        }),
                    )
                    .await?;
                }
                agent_text.push_str(delta);
                send_notification(
                    writer,
                    "item/agentMessage/delta",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": agent_item_id,
                        "delta": delta
                    }),
                )
                .await?;
            }
            Some("agent_thought_chunk") => {
                let delta = update
                    .pointer("/content/text")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !*reasoning_started {
                    *reasoning_started = true;
                    send_notification(
                        writer,
                        "item/started",
                        json!({
                            "item": {
                                "type": "reasoning",
                                "id": reasoning_item_id,
                                "summary": [],
                                "content": []
                            },
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "startedAtMs": now_millis()
                        }),
                    )
                    .await?;
                }
                reasoning_text.push_str(delta);
                send_notification(
                    writer,
                    "item/reasoning/summaryTextDelta",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": reasoning_item_id,
                        "delta": delta,
                        "summaryIndex": 0
                    }),
                )
                .await?;
            }
            Some("tool_call" | "tool_call_update") => {
                self.handle_tool_update(writer, thread_id, turn_id, tools, update)
                    .await?;
            }
            Some("plan") => {
                let steps = update
                    .get("entries")
                    .or_else(|| update.get("plan"))
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|entry| {
                                json!({
                                    "step": entry.get("content")
                                        .or_else(|| entry.get("step"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("Cursor plan step"),
                                    "status": entry.get("status")
                                        .and_then(Value::as_str)
                                        .unwrap_or("pending")
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                send_notification(
                    writer,
                    "turn/plan/updated",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "explanation": null,
                        "plan": steps
                    }),
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_tool_update(
        &self,
        writer: &RemoteWriter,
        thread_id: &str,
        turn_id: &str,
        tools: &mut HashMap<String, Value>,
        update: Value,
    ) -> Result<()> {
        let tool_id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .context("ACP tool update omitted toolCallId")?
            .to_owned();
        let merged = tools.entry(tool_id.clone()).or_insert_with(|| json!({}));
        merge_object(merged, &update);

        let status = merged
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("in_progress");
        let terminal = matches!(status, "completed" | "failed" | "cancelled");
        let item = dynamic_tool_item(&tool_id, merged, terminal);
        let method = if terminal {
            "item/completed"
        } else {
            "item/started"
        };
        let params = if terminal {
            json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": now_millis()
            })
        } else {
            json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": now_millis()
            })
        };
        send_notification(writer, method, params).await
    }

    async fn handle_permission(
        &self,
        connection_id: ConnectionId,
        writer: &RemoteWriter,
        thread_id: &str,
        turn_id: &str,
        event: &Value,
    ) -> Result<()> {
        let acp_id = event
            .get("id")
            .cloned()
            .context("ACP permission omitted id")?;
        let params = event.get("params").cloned().unwrap_or_else(|| json!({}));
        let title = permission_title(&params);
        let kind = permission_kind(&params);
        let item_id = params
            .pointer("/toolCall/toolCallId")
            .and_then(Value::as_str)
            .map_or_else(|| Uuid::now_v7().to_string(), str::to_owned);
        let mobile_id = self.next_mobile_id.fetch_add(1, Ordering::Relaxed);
        let request_id = RequestId::Integer(mobile_id);
        let (sender, receiver) = oneshot::channel();
        self.mobile_requests.lock().await.insert(
            mobile_id.to_string(),
            PendingMobileRequest {
                connection_id,
                sender,
            },
        );

        let method = if matches!(kind, "edit" | "delete" | "move") {
            "item/fileChange/requestApproval"
        } else {
            "item/commandExecution/requestApproval"
        };
        let request_params = if method == "item/fileChange/requestApproval" {
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "startedAtMs": now_millis(),
                "reason": title,
                "grantRoot": null
            })
        } else {
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "startedAtMs": now_millis(),
                "approvalId": null,
                "environmentId": null,
                "reason": title,
                "networkApprovalContext": null,
                "command": title,
                "cwd": self.workspace,
                "commandActions": [{"type": "unknown", "command": title}],
                "additionalPermissions": null,
                "proposedExecpolicyAmendment": null,
                "proposedNetworkPolicyAmendments": null,
                "availableDecisions": null
            })
        };

        if let Err(err) = send_server_request(writer, request_id, method, request_params).await {
            self.mobile_requests
                .lock()
                .await
                .remove(&mobile_id.to_string());
            self.acp
                .respond(acp_id, acp_permission_result(&params, None))
                .await?;
            return Err(err);
        }

        let mobile_result = timeout(Duration::from_secs(300), receiver)
            .await
            .ok()
            .and_then(std::result::Result::ok);
        self.acp
            .respond(
                acp_id,
                acp_permission_result(&params, mobile_result.as_ref()),
            )
            .await
    }

    async fn turn_interrupt(
        &self,
        id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> Result<()> {
        let thread_id = required_string(&params, "threadId")?;
        let persisted = self.persisted_thread(&thread_id).await?;
        self.acp.cancel(&persisted.acp_session_id).await?;
        send_response(&writer, id, json!({})).await
    }

    async fn persisted_thread(&self, thread_id: &str) -> Result<PersistedThread> {
        self.state
            .lock()
            .await
            .threads
            .get(thread_id)
            .cloned()
            .with_context(|| format!("unknown thread {thread_id}"))
    }

    async fn thread_json(&self, thread_id: &str, _include_turns: bool) -> Result<Value> {
        let thread = self.persisted_thread(thread_id).await?;
        Ok(json!({
            "id": thread_id,
            "extra": null,
            "sessionId": thread_id,
            "forkedFromId": null,
            "parentThreadId": null,
            "preview": "",
            "ephemeral": false,
            "historyMode": "legacy",
            "modelProvider": "cursor",
            "createdAt": thread.created_at,
            "updatedAt": thread.updated_at,
            "recencyAt": thread.updated_at,
            "status": {"type": "idle"},
            "path": null,
            "cwd": thread.workspace,
            "cliVersion": crate::BRIDGE_VERSION,
            "source": "appServer",
            "canAcceptDirectInput": true,
            "threadSource": null,
            "agentNickname": null,
            "agentRole": null,
            "gitInfo": null,
            "name": null,
            "turns": []
        }))
    }

    async fn resolve_mobile_request(&self, id: RequestId, result: Value) {
        let key = request_id_key(&id);
        if let Some(pending) = self.mobile_requests.lock().await.remove(&key) {
            let _ = pending.sender.send(result);
        }
    }

    async fn resolve_mobile_error(&self, error: JSONRPCError) {
        let key = request_id_key(&error.id);
        if let Some(pending) = self.mobile_requests.lock().await.remove(&key) {
            let _ = pending.sender.send(json!({"decision": "decline"}));
        }
    }

    pub async fn connection_closed(&self, connection_id: ConnectionId) {
        {
            let mut pending = self.mobile_requests.lock().await;
            let keys: Vec<_> = pending
                .iter()
                .filter(|(_, request)| request.connection_id == connection_id)
                .map(|(key, _)| key.clone())
                .collect();
            for key in keys {
                pending.remove(&key);
            }
        }
        self.processes.connection_closed(connection_id).await;
    }
}

fn protocol_json<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value)
        .unwrap_or_else(|err| panic!("protocol value should serialize: {err}"))
}

fn model_list_result(model: &str) -> Value {
    json!({
        "data": [{
            "id": model,
            "model": model,
            "displayName": format!("Cursor {model}"),
            "description": "Cursor model pinned by codex-remote-bridge",
            "hidden": false,
            "supportedReasoningEfforts": [],
            "defaultReasoningEffort": "medium",
            "isDefault": true
        }],
        "nextCursor": null
    })
}

fn request_id_key(id: &RequestId) -> String {
    match id {
        RequestId::Integer(value) => value.to_string(),
        RequestId::String(value) => format!("s:{value}"),
    }
}

fn required_string(params: &Value, key: &str) -> Result<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("missing string parameter {key}"))
}

fn event_belongs_to_session(event: &Value, session_id: &str) -> bool {
    event.pointer("/params/sessionId").and_then(Value::as_str) == Some(session_id)
}

fn extract_prompt(params: &Value) -> Result<String> {
    let input = params
        .get("input")
        .and_then(Value::as_array)
        .context("turn/start requires input array")?;
    let text = input
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        return Err(anyhow!("this POC currently accepts text input only"));
    }
    Ok(text)
}

fn turn_json(id: &str, status: &str, error: Option<String>) -> Value {
    let now = now_seconds();
    json!({
        "id": id,
        "items": [],
        "itemsView": "full",
        "status": status,
        "error": error.map(|message| json!({
            "message": message,
            "codexErrorInfo": null,
            "additionalDetails": null
        })),
        "startedAt": now,
        "completedAt": if status == "inProgress" { Value::Null } else { json!(now) },
        "durationMs": null
    })
}

fn dynamic_tool_item(id: &str, update: &Value, terminal: bool) -> Value {
    let title = update
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Cursor tool");
    let failed = update
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "failed" | "cancelled"));
    json!({
        "type": "dynamicToolCall",
        "id": id,
        "namespace": "cursor",
        "tool": title,
        "arguments": update.get("rawInput").cloned().unwrap_or_else(|| json!({})),
        "status": if terminal {
            if failed { "failed" } else { "completed" }
        } else {
            "inProgress"
        },
        "contentItems": update.get("content").cloned(),
        "success": if terminal { Some(!failed) } else { None },
        "durationMs": null
    })
}

fn merge_object(target: &mut Value, patch: &Value) {
    let Some(target) = target.as_object_mut() else {
        return;
    };
    if let Some(patch) = patch.as_object() {
        for (key, value) in patch {
            target.insert(key.clone(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_extraction_joins_all_text_blocks() {
        let prompt = extract_prompt(&json!({
            "input": [
                {"type": "text", "text": "first"},
                {"type": "image", "url": "https://example.invalid/a.png"},
                {"type": "text", "text": "second"}
            ]
        }))
        .unwrap();
        assert_eq!(prompt, "first\nsecond");
    }

    #[test]
    fn tool_updates_preserve_prior_fields() {
        let mut target = json!({"toolCallId": "x", "title": "Run tests"});
        merge_object(&mut target, &json!({"status": "completed"}));
        assert_eq!(target["title"], "Run tests");
        assert_eq!(target["status"], "completed");
    }

    #[test]
    fn completed_tool_item_is_fail_closed_on_cancel() {
        let item = dynamic_tool_item("x", &json!({"title": "write", "status": "cancelled"}), true);
        assert_eq!(item["status"], "failed");
        assert_eq!(item["success"], false);
    }
}
