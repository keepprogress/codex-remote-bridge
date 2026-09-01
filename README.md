# codex-remote-bridge

Experimental proof of concept that lets the ChatGPT iPhone Remote UI drive
Cursor Agent inside a Linux or WSL workspace:

```text
ChatGPT Remote → OpenAI WHAM relay → Codex remote-control transport
               → App Server/ACP translation → Cursor Agent → local files
```

The bridge does not launch Codex core or send model turns to Codex. OpenAI is
used only for its Remote Control relay; generation and tool usage happen in
`agent acp` and count against the signed-in Cursor account's quota.

## Status and risk

This is a POC, not a supported OpenAI or Cursor integration. The Codex
App Server protocol is public, but its WHAM Remote Control transport remains
experimental and can change without notice. This crate pins the OpenAI Codex
source tag `rust-v0.145.0`, matching the version used during development.

Public host-side code proves TLS/WebSocket transport and bearer-token
authentication. It does not establish application-level end-to-end encryption,
so assume OpenAI's relay can process the App Server messages.

The bridge reuses Codex's authentication loader and remote transport. It does
not parse, copy, persist, or print ChatGPT OAuth and remote-control bearer
tokens. Pairing codes are printed only when `--pair` is requested.

## Requirements

- Linux or WSL
- Rust 1.95.0 (selected automatically by `rust-toolchain.toml`)
- OpenSSL development headers (`libssl-dev` and `pkg-config` on Ubuntu/WSL)
- Cursor Agent CLI, logged in with `agent login`
- Codex CLI `0.145.0`, logged in with ChatGPT rather than an API key
- ChatGPT Remote access on the same ChatGPT account/workspace

## Build and diagnose

```bash
cd codex-remote-bridge
cargo build

cargo run -- doctor --workspace /home/me/project --model auto
```

`doctor` checks executable versions, login status, the requested model, and the
canonical workspace path. It never reads credential values.

## Run and pair

Stop any other Codex remote-control daemon using the same enrollment first, then
run:

```bash
cd codex-remote-bridge
RUST_LOG=info cargo run -- \
  serve \
  --workspace /home/me/project \
  --model auto \
  --pair
```

Enter the short-lived code in ChatGPT Remote on the iPhone. Leave the bridge and
host awake. `--model` is passed at Cursor ACP process startup because Cursor's
ACP runtime model picker is not currently reliable.

Use `--trace-wire` only while diagnosing mobile compatibility. It logs method
names, request-ID presence, and encoded frame sizes. Prompt bodies, tool
arguments, results, pairing codes, and tokens are excluded.

State is stored under `~/.codex-remote-bridge` by default:

- `installation_id` identifies this bridge to Remote Control.
- `bridge-state.json` maps Codex thread IDs to Cursor ACP session IDs.
- Codex's SQLite files persist only the official remote enrollment.

Set `CODEX_REMOTE_BRIDGE_HOME` to move this state. Delete that directory and
pair again to reset the POC. Revoke a paired controller from ChatGPT/Codex
connection settings.

## Implemented protocol surface

- App Server `initialize`
- `thread/start`, `thread/resume`, `thread/read`, `thread/list`
- `turn/start`, `turn/interrupt`
- Agent message, reasoning, plan, and generic tool-call events
- Bidirectional command/file approvals with fail-closed timeout behavior
- Cursor ACP v1 `initialize`, `authenticate`, session lifecycle, prompt,
  streaming updates, cancellation, and permission responses

Known limitations:

- `turn/steer` returns an explicit unsupported error because ACP v1 has no
  equivalent mid-turn steering operation.
- Text prompts are supported; image and audio translation are not implemented.
- Cursor's `ask_question` and `create_plan` extensions are cancelled because
  ChatGPT Remote cannot safely represent their full response schemas.
- A resumed Cursor session may be unavailable even when the CLI advertised
  `loadSession`; the bridge creates a replacement ACP session in that case.
- The ChatGPT mobile client's complete request set is not public. Unknown
  methods return JSON-RPC method-not-found and can be identified safely with
  `--trace-wire`.

## Test

```bash
cd codex-remote-bridge
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

