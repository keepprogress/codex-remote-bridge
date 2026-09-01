use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistedThread {
    pub acp_session_id: String,
    pub workspace: PathBuf,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedState {
    #[serde(default)]
    pub threads: HashMap<String, PersistedThread>,
}

#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(home: impl AsRef<Path>) -> Self {
        Self {
            path: home.as_ref().join("bridge-state.json"),
        }
    }

    pub async fn load(&self) -> Result<PersistedState> {
        match fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid bridge state at {}", self.path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(PersistedState::default()),
            Err(err) => Err(err)
                .with_context(|| format!("cannot read bridge state at {}", self.path.display())),
        }
    }

    pub async fn save(&self, state: &PersistedState) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("bridge state path has no parent")?;
        fs::create_dir_all(parent).await?;

        let bytes = serde_json::to_vec_pretty(state)?;
        let temp = self.path.with_extension("json.tmp");
        fs::write(&temp, bytes)
            .await
            .with_context(|| format!("cannot write temporary state at {}", temp.display()))?;
        fs::rename(&temp, &self.path)
            .await
            .with_context(|| format!("cannot replace state at {}", self.path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn state_round_trip_contains_no_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let store = StateStore::new(temp.path());
        let mut state = PersistedState::default();
        state.threads.insert(
            "thr_test".into(),
            PersistedThread {
                acp_session_id: "sess_test".into(),
                workspace: PathBuf::from("/work/project"),
                created_at: 1,
                updated_at: 2,
            },
        );

        store.save(&state).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.threads["thr_test"], state.threads["thr_test"]);

        let raw = fs::read_to_string(temp.path().join("bridge-state.json"))
            .await
            .unwrap();
        assert!(!raw.contains("token"));
        assert!(!raw.contains("pairing"));
    }
}

