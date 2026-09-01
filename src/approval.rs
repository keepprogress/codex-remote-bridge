use serde_json::Value;

/// Maps a Codex approval decision back to one of the opaque ACP options.
///
/// Unknown or malformed decisions fail closed by selecting a reject option, or
/// by cancelling when the agent did not offer one.
pub fn acp_permission_result(params: &Value, codex_result: Option<&Value>) -> Value {
    let decision = codex_result
        .and_then(|value| value.get("decision"))
        .and_then(Value::as_str)
        .unwrap_or("decline");

    let wanted_kinds: &[&str] = match decision {
        "acceptForSession" => &["allow_always", "allow_once"],
        "accept" => &["allow_once", "allow_always"],
        _ => &["reject_once", "reject_always"],
    };

    let selected = params
        .get("options")
        .and_then(Value::as_array)
        .and_then(|options| {
            wanted_kinds.iter().find_map(|wanted| {
                options.iter().find(|option| {
                    option.get("kind").and_then(Value::as_str) == Some(*wanted)
                })
            })
        })
        .and_then(|option| option.get("optionId"))
        .and_then(Value::as_str);

    selected.map_or_else(
        || serde_json::json!({"outcome": {"outcome": "cancelled"}}),
        |option_id| {
            serde_json::json!({
                "outcome": {
                    "outcome": "selected",
                    "optionId": option_id
                }
            })
        },
    )
}

pub fn permission_title(params: &Value) -> String {
    params
        .pointer("/toolCall/title")
        .and_then(Value::as_str)
        .or_else(|| params.get("title").and_then(Value::as_str))
        .unwrap_or("Cursor requested permission")
        .to_owned()
}

pub fn permission_kind(params: &Value) -> &str {
    params
        .pointer("/toolCall/kind")
        .and_then(Value::as_str)
        .unwrap_or("other")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> Value {
        serde_json::json!({
            "options": [
                {"optionId": "yes", "kind": "allow_once"},
                {"optionId": "always", "kind": "allow_always"},
                {"optionId": "no", "kind": "reject_once"}
            ]
        })
    }

    #[test]
    fn maps_accept_without_assuming_option_ids() {
        let result =
            acp_permission_result(&params(), Some(&serde_json::json!({"decision": "accept"})));
        assert_eq!(result.pointer("/outcome/optionId"), Some(&Value::from("yes")));
    }

    #[test]
    fn malformed_decision_fails_closed() {
        let result = acp_permission_result(&params(), None);
        assert_eq!(result.pointer("/outcome/optionId"), Some(&Value::from("no")));
    }
}

