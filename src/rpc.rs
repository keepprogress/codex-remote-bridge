use anyhow::{Context, Result};
use codex_app_server_protocol::{
    JSONRPCNotification, JSONRPCRequest, JSONRPCErrorError, RequestId, ServerNotification,
    ServerNotificationEnvelope, ServerRequest,
};
use codex_app_server_transport::{
    OutgoingError, OutgoingMessage, OutgoingResponse, QueuedOutgoingMessage,
};
use serde_json::Value;
use tokio::sync::mpsc;

pub type RemoteWriter = mpsc::Sender<QueuedOutgoingMessage>;

pub async fn send_response(writer: &RemoteWriter, id: RequestId, result: Value) -> Result<()> {
    send(
        writer,
        OutgoingMessage::Response(OutgoingResponse { id, result }),
    )
    .await
}

pub async fn send_error(
    writer: &RemoteWriter,
    id: RequestId,
    code: i64,
    message: impl Into<String>,
) -> Result<()> {
    send(
        writer,
        OutgoingMessage::Error(OutgoingError {
            id,
            error: JSONRPCErrorError {
                code,
                message: message.into(),
                data: None,
            },
        }),
    )
    .await
}

pub async fn send_notification(
    writer: &RemoteWriter,
    method: &str,
    params: Value,
) -> Result<()> {
    let notification = ServerNotification::try_from(JSONRPCNotification {
        method: method.to_owned(),
        params: Some(params),
    })
    .with_context(|| format!("invalid Codex notification payload for {method}"))?;
    send(
        writer,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification,
            emitted_at_ms: Some(now_millis()),
        }),
    )
    .await
}

pub async fn send_server_request(
    writer: &RemoteWriter,
    id: RequestId,
    method: &str,
    params: Value,
) -> Result<()> {
    let request = ServerRequest::try_from(JSONRPCRequest {
        id,
        method: method.to_owned(),
        params: Some(params),
        trace: None,
    })
    .with_context(|| format!("invalid Codex server request payload for {method}"))?;
    send(writer, OutgoingMessage::Request(request)).await
}

async fn send(writer: &RemoteWriter, message: OutgoingMessage) -> Result<()> {
    writer
        .send(QueuedOutgoingMessage::new(message))
        .await
        .context("remote controller disconnected")
}

pub fn now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

pub fn trace_summary(direction: &str, value: &Value) -> Value {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("response");
    serde_json::json!({
        "direction": direction,
        "method": method,
        "hasId": value.get("id").is_some(),
        "bytes": serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_trace_does_not_include_params() {
        let source = serde_json::json!({
            "id": 3,
            "method": "turn/start",
            "params": {"input": [{"type": "text", "text": "secret prompt"}]}
        });
        let trace = trace_summary("remote->bridge", &source).to_string();
        assert!(!trace.contains("secret prompt"));
        assert!(trace.contains("turn/start"));
    }
}

