use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use codex_core::ThreadManager;
use codex_protocol::ThreadId;

#[derive(Debug, Clone, Default)]
pub(crate) struct ParentConfigurationSnapshot {
    pub(super) agent_role: Option<String>,
    pub(super) proposal_bearing: Option<bool>,
    pub(super) personality: Option<String>,
    pub(super) multi_agent_mode: Option<String>,
    pub(super) parent_thread_id: Option<String>,
    pub(super) collaboration_mode: String,
    pub(super) session_source: String,
    pub(super) config_sources: Vec<String>,
    pub(super) lifecycle_state: String,
    pub(super) blockers: Vec<String>,
}

pub(crate) type ParentConfigurationFuture<'a> =
    Pin<Box<dyn Future<Output = ParentConfigurationSnapshot> + Send + 'a>>;

pub(crate) trait ParentConfigurationAdapter: Send + Sync {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a>;
}
pub(crate) struct ThreadManagerParentConfigurationAdapter {
    thread_manager: Arc<ThreadManager>,
}

impl ThreadManagerParentConfigurationAdapter {
    pub(crate) fn new(thread_manager: Arc<ThreadManager>) -> Self {
        Self { thread_manager }
    }
}

impl ParentConfigurationAdapter for ThreadManagerParentConfigurationAdapter {
    fn read_configuration<'a>(&'a self, thread_id: &'a str) -> ParentConfigurationFuture<'a> {
        Box::pin(async move {
            let parsed_thread_id = match ThreadId::from_string(thread_id) {
                Ok(thread_id) => thread_id,
                Err(error) => {
                    return ParentConfigurationSnapshot {
                        collaboration_mode: "unknown".to_string(),
                        session_source: "unavailable".to_string(),
                        lifecycle_state: "invalid_thread_id".to_string(),
                        blockers: vec![format!("invalid native thread id: {error}")],
                        ..Default::default()
                    };
                }
            };
            let thread = match self.thread_manager.get_thread(parsed_thread_id).await {
                Ok(thread) => thread,
                Err(error) => {
                    return ParentConfigurationSnapshot {
                        collaboration_mode: "unknown".to_string(),
                        session_source: "unavailable".to_string(),
                        lifecycle_state: "thread_unavailable".to_string(),
                        blockers: vec![format!("native thread unavailable: {error}")],
                        ..Default::default()
                    };
                }
            };
            let snapshot = thread.config_snapshot().await;
            let config = thread.config().await;
            let proposal_bearing = snapshot.agent_role.as_deref().and_then(|agent_role| {
                codex_core::effective_role_catalog(&config)
                    .into_iter()
                    .find(|role| role.id == agent_role)
                    .and_then(|role| role.config.planner_capabilities.as_ref())
                    .map(|capabilities| capabilities.proposal_bearing)
            });
            let mut config_sources = vec![format!("app-server://threads/{thread_id}/config")];
            if let Some(agent_role) = snapshot.agent_role.as_ref() {
                config_sources.push(format!("agent-role://{agent_role}"));
            }
            ParentConfigurationSnapshot {
                agent_role: snapshot.agent_role,
                proposal_bearing,
                personality: snapshot.personality.map(|value| value.to_string()),
                multi_agent_mode: Some(
                    format!("{:?}", thread.multi_agent_version()).to_lowercase(),
                ),
                parent_thread_id: snapshot.parent_thread_id.map(|value| value.to_string()),
                collaboration_mode: format!("{:?}", snapshot.collaboration_mode.mode)
                    .to_lowercase(),
                session_source: format!("{:?}", snapshot.session_source).to_lowercase(),
                config_sources,
                lifecycle_state: "loaded".to_string(),
                blockers: Vec::new(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_snapshot_does_not_invent_native_identity() {
        let snapshot = ParentConfigurationSnapshot::default();

        assert!(snapshot.agent_role.is_none());
        assert!(snapshot.parent_thread_id.is_none());
        assert!(snapshot.config_sources.is_empty());
        assert!(snapshot.blockers.is_empty());
    }
}
