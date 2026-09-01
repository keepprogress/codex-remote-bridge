pub mod acp;
pub mod approval;
pub mod bridge;
mod process;
pub mod remote;
pub mod rpc;
pub mod state;

pub const BRIDGE_VERSION: &str = env!("CARGO_PKG_VERSION");
