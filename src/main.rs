use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use codex_remote_bridge::acp::{AcpClient, default_agent_path};
use codex_remote_bridge::bridge::Bridge;
use codex_remote_bridge::remote::{RemoteRuntime, default_bridge_home, default_codex_home};
use codex_remote_bridge::state::StateStore;
use serde_json::Value;
use tokio::process::Command;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Verify local binaries, authentication, and workspace paths.
    Doctor {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        agent_bin: Option<PathBuf>,
        #[arg(long, default_value = "auto")]
        model: String,
    },
    /// Connect ChatGPT Remote Control to a Cursor ACP process.
    Serve {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        agent_bin: Option<PathBuf>,
        #[arg(long, default_value = "auto")]
        model: String,
        #[arg(long)]
        codex_home: Option<PathBuf>,
        #[arg(long)]
        bridge_home: Option<PathBuf>,
        /// Print a short-lived code for pairing ChatGPT Remote.
        #[arg(long)]
        pair: bool,
        /// Log method names and frame sizes, never prompt bodies or tokens.
        #[arg(long)]
        trace_wire: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Commands::Doctor {
            workspace,
            agent_bin,
            model,
        } => {
            let agent_bin = agent_bin.unwrap_or_else(default_agent_path);
            doctor(&workspace, &agent_bin, &model).await
        }
        Commands::Serve {
            workspace,
            agent_bin,
            model,
            codex_home,
            bridge_home,
            pair,
            trace_wire,
        } => {
            let agent_bin = agent_bin.unwrap_or_else(default_agent_path);
            let codex_home = codex_home.unwrap_or_else(default_codex_home);
            let bridge_home = bridge_home.unwrap_or_else(default_bridge_home);
            serve(
                workspace,
                agent_bin,
                model,
                codex_home,
                bridge_home,
                pair,
                trace_wire,
            )
            .await
        }
    }
}

async fn serve(
    workspace: PathBuf,
    agent_bin: PathBuf,
    model: String,
    codex_home: PathBuf,
    bridge_home: PathBuf,
    pair: bool,
    trace_wire: bool,
) -> Result<()> {
    let workspace = workspace
        .canonicalize()
        .with_context(|| format!("workspace does not exist: {}", workspace.display()))?;
    doctor(&workspace, &agent_bin, &model).await?;

    info!(
        workspace = %workspace.display(),
        model,
        "starting Cursor ACP backend"
    );
    let acp = AcpClient::spawn(&agent_bin, &model, &workspace).await?;
    let bridge = std::sync::Arc::new(
        Bridge::new(
            acp,
            workspace,
            codex_home.clone(),
            model,
            StateStore::new(&bridge_home),
            trace_wire,
        )
        .await?,
    );

    let remote = RemoteRuntime::start(&codex_home, &bridge_home).await?;
    remote.wait_until_connected().await?;
    info!("connected to OpenAI remote-control relay");

    if pair {
        let pairing = remote.start_pairing().await?;
        print_pairing(&pairing);
    }

    tokio::select! {
        result = remote.run(bridge) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("cannot listen for Ctrl-C")?;
            info!("stopping bridge");
            Ok(())
        }
    }
}

async fn doctor(workspace: &Path, agent_bin: &Path, model: &str) -> Result<()> {
    let workspace = workspace
        .canonicalize()
        .with_context(|| format!("workspace does not exist: {}", workspace.display()))?;
    if !workspace.is_dir() {
        bail!("workspace is not a directory: {}", workspace.display());
    }
    if !agent_bin.exists() && agent_bin.components().count() > 1 {
        bail!(
            "Cursor agent binary does not exist: {}",
            agent_bin.display()
        );
    }

    let version = run_checked(agent_bin, &["--version"]).await?;
    run_checked(agent_bin, &["acp", "--help"])
        .await
        .context("Cursor Agent does not advertise ACP support")?;
    let models = run_checked(agent_bin, &["models"]).await?;
    if model != "auto" && !models.split_whitespace().any(|word| word == model) {
        bail!("Cursor model is not available: {model}");
    }

    let status = run_checked(agent_bin, &["status"]).await?;
    if status.to_ascii_lowercase().contains("not logged in") {
        bail!("Cursor Agent is not logged in");
    }
    let codex_version = run_checked(Path::new("codex"), &["--version"]).await?;
    if !codex_version.contains("0.145.0") {
        bail!(
            "Codex CLI 0.145.0 is required by the pinned transport, found: {}",
            one_line(&codex_version)
        );
    }
    run_checked(
        Path::new("codex"),
        &["app-server", "--remote-control", "--help"],
    )
    .await
    .context("Codex CLI does not accept the hidden Remote Control option")?;
    let codex_status = run_checked(Path::new("codex"), &["login", "status"]).await?;
    if codex_status.to_ascii_lowercase().contains("not logged in") {
        bail!("Codex CLI is not logged in with ChatGPT");
    }

    println!("Cursor Agent: {}", version.trim());
    println!("Cursor auth: {}", one_line(&status));
    println!("Cursor model: {model}");
    println!("Workspace: {}", workspace.display());
    println!("Codex CLI: {}", one_line(&codex_version));
    println!("Codex auth: {}", one_line(&codex_status));
    println!("Remote Control capability: available");
    println!("Credential values were not inspected or printed.");
    Ok(())
}

async fn run_checked(program: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("cannot execute {}", program.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} {} failed: {}",
            program.display(),
            args.join(" "),
            stderr.trim()
        );
    }
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    if text.trim().is_empty() {
        text = String::from_utf8_lossy(&output.stderr).into_owned();
    }
    Ok(text)
}

fn one_line(text: &str) -> String {
    text.lines().collect::<Vec<_>>().join(" ")
}

fn print_pairing(pairing: &Value) {
    let code = pairing
        .get("manualPairingCode")
        .or_else(|| pairing.get("pairingCode"))
        .and_then(Value::as_str)
        .unwrap_or("<pairing code unavailable>");
    println!("Pairing code: {code}");
    if let Some(expires) = pairing.get("expiresAt") {
        println!("Expires at: {expires}");
    }
    println!("Enter this code in ChatGPT Remote. Treat it as a short-lived secret.");
}
