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
        #[arg(long, default_value_os_t = default_agent_path())]
        agent_bin: PathBuf,
        #[arg(long, default_value = "auto")]
        model: String,
    },
    /// Connect ChatGPT Remote Control to a Cursor ACP process.
    Serve {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long, default_value_os_t = default_agent_path())]
        agent_bin: PathBuf,
        #[arg(long, default_value = "auto")]
        model: String,
        #[arg(long, default_value_os_t = default_codex_home())]
        codex_home: PathBuf,
        #[arg(long, default_value_os_t = default_bridge_home())]
        bridge_home: PathBuf,
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
        } => doctor(&workspace, &agent_bin, &model).await,
        Commands::Serve {
            workspace,
            agent_bin,
            model,
            codex_home,
            bridge_home,
            pair,
            trace_wire,
        } => {
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
        bail!("Cursor agent binary does not exist: {}", agent_bin.display());
    }

    let version = run_checked(agent_bin, &["--version"]).await?;
    let models = run_checked(agent_bin, &["models"]).await?;
    if model != "auto" && !models.lines().any(|line| line.starts_with(model)) {
        bail!("Cursor model is not available: {model}");
    }

    let status = run_checked(agent_bin, &["status"]).await?;
    println!("Cursor Agent: {}", version.trim());
    println!("Cursor auth: {}", one_line(&status));
    println!("Cursor model: {model}");
    println!("Workspace: {}", workspace.display());

    match run_checked(Path::new("codex"), &["login", "status"]).await {
        Ok(status) => println!("Codex auth: {}", one_line(&status)),
        Err(err) => println!("Codex auth: unavailable ({err})"),
    }
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

