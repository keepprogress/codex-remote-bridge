use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_app_server_protocol::{
    ProcessExitedNotification, ProcessKillParams, ProcessOutputDeltaNotification,
    ProcessOutputStream, ProcessResizePtyParams, ProcessSpawnParams, ProcessTerminalSize,
    ProcessWriteStdinParams, RequestId,
};
use codex_app_server_transport::ConnectionId;
use codex_utils_pty::{
    DEFAULT_OUTPUT_BYTES_CAP, ProcessHandle, SpawnedProcess, TerminalSize, spawn_pipe_process,
    spawn_pipe_process_no_stdin, spawn_pty_process,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, warn};

use crate::rpc::{RemoteWriter, send_error, send_notification, send_response};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const IO_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const EXEC_TIMEOUT_EXIT_CODE: i32 = 124;

#[derive(Clone, Default)]
pub(crate) struct ProcessManager {
    sessions: Arc<Mutex<HashMap<ConnectionProcessHandle, ProcessSession>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConnectionProcessHandle {
    connection_id: ConnectionId,
    process_handle: String,
}

#[derive(Clone)]
struct ProcessSession {
    control_tx: mpsc::Sender<ProcessControlRequest>,
}

enum ProcessControl {
    Write { delta: Vec<u8>, close_stdin: bool },
    Resize { size: TerminalSize },
    Kill,
}

struct ProcessControlRequest {
    control: ProcessControl,
    response_tx: Option<oneshot::Sender<Result<(), RpcFailure>>>,
}

struct StartProcess {
    connection_id: ConnectionId,
    request_id: RequestId,
    params: ProcessSpawnParams,
    writer: RemoteWriter,
}

struct RunProcess {
    process_handle: String,
    spawned: SpawnedProcess,
    control_rx: mpsc::Receiver<ProcessControlRequest>,
    stream_stdin: bool,
    stream_stdout_stderr: bool,
    timeout: Option<Duration>,
    output_bytes_cap: Option<usize>,
    writer: RemoteWriter,
}

struct CollectOutput {
    process_handle: String,
    output_rx: mpsc::Receiver<Vec<u8>>,
    writer: RemoteWriter,
    stream: ProcessOutputStream,
    stream_output: bool,
    output_bytes_cap: Option<usize>,
}

#[derive(Default)]
struct ProcessOutputCapture {
    text: String,
    cap_reached: bool,
}

#[derive(Debug)]
struct RpcFailure {
    code: i64,
    message: String,
}

impl RpcFailure {
    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32_600,
            message: message.into(),
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32_602,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32_603,
            message: message.into(),
        }
    }
}

impl ProcessManager {
    pub(crate) async fn spawn(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> anyhow::Result<()> {
        let params = match decode_params::<ProcessSpawnParams>(params, "process/spawn") {
            Ok(params) => params,
            Err(failure) => return reply_failure(&writer, request_id, failure).await,
        };
        match self
            .start(StartProcess {
                connection_id,
                request_id: request_id.clone(),
                params,
                writer: writer.clone(),
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(failure) => reply_failure(&writer, request_id, failure).await,
        }
    }

    pub(crate) async fn write_stdin(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> anyhow::Result<()> {
        let params = match decode_params::<ProcessWriteStdinParams>(params, "process/writeStdin") {
            Ok(params) => params,
            Err(failure) => return reply_failure(&writer, request_id, failure).await,
        };
        if params.delta_base64.is_none() && !params.close_stdin {
            return reply_failure(
                &writer,
                request_id,
                RpcFailure::invalid_params("process/writeStdin requires deltaBase64 or closeStdin"),
            )
            .await;
        }
        let delta = match params.delta_base64 {
            Some(delta) => match STANDARD.decode(delta) {
                Ok(delta) => delta,
                Err(err) => {
                    return reply_failure(
                        &writer,
                        request_id,
                        RpcFailure::invalid_params(format!("invalid deltaBase64: {err}")),
                    )
                    .await;
                }
            },
            None => Vec::new(),
        };
        let result = self
            .send_control(
                connection_id,
                params.process_handle,
                ProcessControl::Write {
                    delta,
                    close_stdin: params.close_stdin,
                },
            )
            .await;
        reply_control_result(&writer, request_id, result).await
    }

    pub(crate) async fn kill(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> anyhow::Result<()> {
        let params = match decode_params::<ProcessKillParams>(params, "process/kill") {
            Ok(params) => params,
            Err(failure) => return reply_failure(&writer, request_id, failure).await,
        };
        let result = self
            .send_control(connection_id, params.process_handle, ProcessControl::Kill)
            .await;
        reply_control_result(&writer, request_id, result).await
    }

    pub(crate) async fn resize_pty(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Value,
        writer: RemoteWriter,
    ) -> anyhow::Result<()> {
        let params = match decode_params::<ProcessResizePtyParams>(params, "process/resizePty") {
            Ok(params) => params,
            Err(failure) => return reply_failure(&writer, request_id, failure).await,
        };
        let size = match terminal_size(params.size) {
            Ok(size) => size,
            Err(failure) => return reply_failure(&writer, request_id, failure).await,
        };
        let result = self
            .send_control(
                connection_id,
                params.process_handle,
                ProcessControl::Resize { size },
            )
            .await;
        reply_control_result(&writer, request_id, result).await
    }

    pub(crate) async fn connection_closed(&self, connection_id: ConnectionId) {
        let sessions = {
            let mut active = self.sessions.lock().await;
            let keys = active
                .keys()
                .filter(|key| key.connection_id == connection_id)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| active.remove(&key))
                .collect::<Vec<_>>()
        };
        for session in sessions {
            let _ = session
                .control_tx
                .send(ProcessControlRequest {
                    control: ProcessControl::Kill,
                    response_tx: None,
                })
                .await;
        }
    }

    async fn start(&self, start: StartProcess) -> Result<(), RpcFailure> {
        let StartProcess {
            connection_id,
            request_id,
            params,
            writer,
        } = start;
        let ProcessSpawnParams {
            command,
            process_handle,
            cwd,
            tty,
            stream_stdin,
            stream_stdout_stderr,
            output_bytes_cap,
            timeout_ms,
            env: env_overrides,
            size,
        } = params;
        let (program, args) = command
            .split_first()
            .ok_or_else(|| RpcFailure::invalid_request("command must not be empty"))?;
        if process_handle.is_empty() {
            return Err(RpcFailure::invalid_request(
                "processHandle must not be empty",
            ));
        }
        if size.is_some() && !tty {
            return Err(RpcFailure::invalid_params(
                "process/spawn size requires tty: true",
            ));
        }
        let size = size.map(terminal_size).transpose()?;
        let timeout = match timeout_ms {
            None => Some(DEFAULT_TIMEOUT),
            Some(None) => None,
            Some(Some(value)) => Some(Duration::from_millis(u64::try_from(value).map_err(
                |_| {
                    RpcFailure::invalid_params(format!(
                        "process/spawn timeoutMs must be non-negative, got {value}"
                    ))
                },
            )?)),
        };
        let output_bytes_cap = output_bytes_cap.unwrap_or(Some(DEFAULT_OUTPUT_BYTES_CAP));
        let stream_stdin = tty || stream_stdin;
        let stream_stdout_stderr = tty || stream_stdout_stderr;
        let mut env = std::env::vars().collect::<HashMap<_, _>>();
        if let Some(overrides) = env_overrides {
            for (key, value) in overrides {
                match value {
                    Some(value) => {
                        env.insert(key, value);
                    }
                    None => {
                        env.remove(&key);
                    }
                }
            }
        }

        let (control_tx, control_rx) = mpsc::channel(32);
        let process_key = ConnectionProcessHandle {
            connection_id,
            process_handle: process_handle.clone(),
        };
        {
            let mut active = self.sessions.lock().await;
            match active.entry(process_key.clone()) {
                Entry::Occupied(_) => {
                    return Err(RpcFailure::invalid_request(format!(
                        "duplicate active process handle: {process_handle:?}"
                    )));
                }
                Entry::Vacant(entry) => {
                    entry.insert(ProcessSession { control_tx });
                }
            }
        }

        debug!(
            program,
            tty, stream_stdin, stream_stdout_stderr, "spawning process"
        );
        let arg0 = None;
        let spawned = if tty {
            spawn_pty_process(
                program,
                args,
                cwd.as_path(),
                &env,
                &arg0,
                size.unwrap_or_default(),
                &[],
            )
            .await
        } else if stream_stdin {
            spawn_pipe_process(program, args, cwd.as_path(), &env, &arg0, &[]).await
        } else {
            spawn_pipe_process_no_stdin(program, args, cwd.as_path(), &env, &arg0, &[]).await
        };
        let spawned = match spawned {
            Ok(spawned) => spawned,
            Err(err) => {
                self.sessions.lock().await.remove(&process_key);
                return Err(RpcFailure::internal(format!(
                    "failed to spawn process: {err}"
                )));
            }
        };

        if let Err(err) = send_response(&writer, request_id, serde_json::json!({})).await {
            self.sessions.lock().await.remove(&process_key);
            return Err(RpcFailure::internal(format!(
                "failed to send process/spawn response: {err}"
            )));
        }

        let sessions = Arc::clone(&self.sessions);
        tokio::spawn(async move {
            run_process(RunProcess {
                process_handle,
                spawned,
                control_rx,
                stream_stdin,
                stream_stdout_stderr,
                timeout,
                output_bytes_cap,
                writer,
            })
            .await;
            sessions.lock().await.remove(&process_key);
        });
        Ok(())
    }

    async fn send_control(
        &self,
        connection_id: ConnectionId,
        process_handle: String,
        control: ProcessControl,
    ) -> Result<(), RpcFailure> {
        let key = ConnectionProcessHandle {
            connection_id,
            process_handle,
        };
        let session = self
            .sessions
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                RpcFailure::invalid_request(format!(
                    "no active process for process handle {:?}",
                    key.process_handle
                ))
            })?;
        let (response_tx, response_rx) = oneshot::channel();
        session
            .control_tx
            .send(ProcessControlRequest {
                control,
                response_tx: Some(response_tx),
            })
            .await
            .map_err(|_| {
                RpcFailure::invalid_request(format!(
                    "process {:?} is no longer running",
                    key.process_handle
                ))
            })?;
        response_rx.await.map_err(|_| {
            RpcFailure::invalid_request(format!(
                "process {:?} is no longer running",
                key.process_handle
            ))
        })?
    }
}

async fn run_process(run: RunProcess) {
    let RunProcess {
        process_handle,
        spawned,
        mut control_rx,
        stream_stdin,
        stream_stdout_stderr,
        timeout,
        output_bytes_cap,
        writer,
    } = run;
    let SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    } = spawned;
    tokio::pin!(exit_rx);
    let mut stdout_handle = tokio::spawn(collect_output(CollectOutput {
        process_handle: process_handle.clone(),
        output_rx: stdout_rx,
        writer: writer.clone(),
        stream: ProcessOutputStream::Stdout,
        stream_output: stream_stdout_stderr,
        output_bytes_cap,
    }));
    let mut stderr_handle = tokio::spawn(collect_output(CollectOutput {
        process_handle: process_handle.clone(),
        output_rx: stderr_rx,
        writer: writer.clone(),
        stream: ProcessOutputStream::Stderr,
        stream_output: stream_stdout_stderr,
        output_bytes_cap,
    }));
    let expiration = async move {
        match timeout {
            Some(duration) => tokio::time::sleep(duration).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(expiration);
    let mut timed_out = false;
    let mut control_open = true;
    let exit_code = loop {
        tokio::select! {
            control = control_rx.recv(), if control_open => match control {
                Some(ProcessControlRequest { control, response_tx }) => {
                    let result = match control {
                        ProcessControl::Write { delta, close_stdin } => {
                            handle_write(&session, stream_stdin, delta, close_stdin).await
                        }
                        ProcessControl::Resize { size } => session.resize(size).map_err(|err| {
                            RpcFailure::invalid_request(format!("failed to resize PTY: {err}"))
                        }),
                        ProcessControl::Kill => {
                            session.request_terminate();
                            Ok(())
                        }
                    };
                    if let Some(response_tx) = response_tx {
                        let _ = response_tx.send(result);
                    }
                }
                None => {
                    control_open = false;
                    session.request_terminate();
                }
            },
            _ = &mut expiration, if !timed_out => {
                timed_out = true;
                session.request_terminate();
            },
            exit = &mut exit_rx => {
                break if timed_out {
                    EXEC_TIMEOUT_EXIT_CODE
                } else {
                    exit.unwrap_or(-1)
                };
            }
        }
    };

    let drained = tokio::time::timeout(IO_DRAIN_TIMEOUT, async {
        tokio::join!(&mut stdout_handle, &mut stderr_handle)
    })
    .await;
    let (stdout, stderr) = match drained {
        Ok((stdout, stderr)) => (stdout.unwrap_or_default(), stderr.unwrap_or_default()),
        Err(_) => {
            stdout_handle.abort();
            stderr_handle.abort();
            warn!(%process_handle, "timed out draining process output");
            (
                ProcessOutputCapture::default(),
                ProcessOutputCapture::default(),
            )
        }
    };
    let notification = ProcessExitedNotification {
        process_handle,
        exit_code,
        stdout: stdout.text,
        stdout_cap_reached: stdout.cap_reached,
        stderr: stderr.text,
        stderr_cap_reached: stderr.cap_reached,
    };
    if let Err(err) = send_notification(
        &writer,
        "process/exited",
        serde_json::to_value(notification).expect("process exit should serialize"),
    )
    .await
    {
        debug!(%err, "could not deliver process/exited notification");
    }
}

async fn collect_output(collect: CollectOutput) -> ProcessOutputCapture {
    let CollectOutput {
        process_handle,
        mut output_rx,
        writer,
        stream,
        stream_output,
        output_bytes_cap,
    } = collect;
    let mut buffer = Vec::new();
    let mut observed = 0usize;
    let mut cap_reached = false;
    while let Some(chunk) = output_rx.recv().await {
        let allowed = output_bytes_cap
            .map(|cap| cap.saturating_sub(observed).min(chunk.len()))
            .unwrap_or(chunk.len());
        observed += allowed;
        cap_reached = output_bytes_cap.is_some_and(|cap| observed == cap);
        let chunk = &chunk[..allowed];
        if stream_output {
            if !chunk.is_empty() {
                let notification = ProcessOutputDeltaNotification {
                    process_handle: process_handle.clone(),
                    stream,
                    delta_base64: STANDARD.encode(chunk),
                    cap_reached,
                };
                if let Err(err) = send_notification(
                    &writer,
                    "process/outputDelta",
                    serde_json::to_value(notification).expect("process output should serialize"),
                )
                .await
                {
                    debug!(%err, "could not deliver process/outputDelta notification");
                }
            }
        } else {
            buffer.extend_from_slice(chunk);
        }
        if cap_reached {
            break;
        }
    }
    ProcessOutputCapture {
        text: String::from_utf8_lossy(&buffer).into_owned(),
        cap_reached,
    }
}

async fn handle_write(
    session: &ProcessHandle,
    stream_stdin: bool,
    delta: Vec<u8>,
    close_stdin: bool,
) -> Result<(), RpcFailure> {
    if !stream_stdin {
        return Err(RpcFailure::invalid_request(
            "stdin streaming is not enabled for this process",
        ));
    }
    if !delta.is_empty() {
        session
            .writer_sender()
            .send(delta)
            .await
            .map_err(|_| RpcFailure::invalid_request("stdin is already closed"))?;
    }
    if close_stdin {
        session.close_stdin();
    }
    Ok(())
}

fn terminal_size(size: ProcessTerminalSize) -> Result<TerminalSize, RpcFailure> {
    if size.rows == 0 || size.cols == 0 {
        return Err(RpcFailure::invalid_params(
            "process size rows and cols must be greater than 0",
        ));
    }
    Ok(TerminalSize {
        rows: size.rows,
        cols: size.cols,
    })
}

fn decode_params<T: DeserializeOwned>(params: Value, method: &str) -> Result<T, RpcFailure> {
    serde_json::from_value(params)
        .map_err(|err| RpcFailure::invalid_params(format!("invalid {method} params: {err}")))
}

async fn reply_control_result(
    writer: &RemoteWriter,
    request_id: RequestId,
    result: Result<(), RpcFailure>,
) -> anyhow::Result<()> {
    match result {
        Ok(()) => send_response(writer, request_id, serde_json::json!({})).await,
        Err(failure) => reply_failure(writer, request_id, failure).await,
    }
}

async fn reply_failure(
    writer: &RemoteWriter,
    request_id: RequestId,
    failure: RpcFailure,
) -> anyhow::Result<()> {
    send_error(writer, request_id, failure.code, failure.message).await
}
