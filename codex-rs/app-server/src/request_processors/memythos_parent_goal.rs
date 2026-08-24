use std::future::Future;
use std::pin::Pin;

use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadGoalGetParams;
use codex_app_server_protocol::ThreadGoalStatus;

use crate::request_processors::ThreadGoalRequestProcessor;

#[derive(Debug, Clone)]
pub(crate) struct ParentGoalSnapshot {
    pub(super) goal_snapshot_ref: Option<String>,
    pub(super) budget_state_ref: Option<String>,
    pub(super) goal_status: Option<ThreadGoalStatus>,
    pub(super) token_budget: Option<i64>,
    pub(super) tokens_used: Option<i64>,
    pub(super) time_used_seconds: Option<i64>,
    pub(super) evidence_refs: Vec<String>,
    pub(super) degraded_reason: Option<String>,
}

pub(crate) type ParentGoalSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = ParentGoalSnapshot> + Send + 'a>>;

pub(crate) trait ParentGoalSnapshotAdapter: Send + Sync {
    fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a>;
}
#[derive(Clone)]
pub(crate) struct ThreadGoalParentSnapshotAdapter {
    thread_goal_processor: ThreadGoalRequestProcessor,
}

impl ThreadGoalParentSnapshotAdapter {
    pub(crate) fn new(thread_goal_processor: ThreadGoalRequestProcessor) -> Self {
        Self {
            thread_goal_processor,
        }
    }
}

impl ParentGoalSnapshotAdapter for ThreadGoalParentSnapshotAdapter {
    fn current_goal_snapshot<'a>(&'a self, thread_id: &'a str) -> ParentGoalSnapshotFuture<'a> {
        Box::pin(async move {
            match self
                .thread_goal_processor
                .thread_goal_get(ThreadGoalGetParams {
                    thread_id: thread_id.to_string(),
                })
                .await
            {
                Ok(Some(ClientResponsePayload::ThreadGoalGet(response))) => {
                    parent_goal_snapshot_from_goal(thread_id, response.goal)
                }
                Ok(_) => ParentGoalSnapshot {
                    goal_snapshot_ref: None,
                    budget_state_ref: None,
                    goal_status: None,
                    token_budget: None,
                    tokens_used: None,
                    time_used_seconds: None,
                    evidence_refs: Vec::new(),
                    degraded_reason: Some("thread/goal/get returned no goal payload".to_string()),
                },
                Err(error) => ParentGoalSnapshot {
                    goal_snapshot_ref: None,
                    budget_state_ref: None,
                    goal_status: None,
                    token_budget: None,
                    tokens_used: None,
                    time_used_seconds: None,
                    evidence_refs: Vec::new(),
                    degraded_reason: Some(format!("thread/goal/get failed: {}", error.message)),
                },
            }
        })
    }
}
fn parent_goal_snapshot_from_goal(thread_id: &str, goal: Option<ThreadGoal>) -> ParentGoalSnapshot {
    let Some(goal) = goal else {
        return ParentGoalSnapshot {
            goal_snapshot_ref: None,
            budget_state_ref: None,
            goal_status: None,
            token_budget: None,
            tokens_used: None,
            time_used_seconds: None,
            evidence_refs: Vec::new(),
            degraded_reason: Some("thread/goal/get returned no active goal".to_string()),
        };
    };

    let goal_snapshot_ref = format!("app-server://threads/{thread_id}/goals/current");
    let budget_state_ref = format!("app-server://threads/{thread_id}/budget/current");
    ParentGoalSnapshot {
        goal_snapshot_ref: Some(goal_snapshot_ref.clone()),
        budget_state_ref: Some(budget_state_ref.clone()),
        goal_status: Some(goal.status),
        token_budget: goal.token_budget,
        tokens_used: Some(goal.tokens_used),
        time_used_seconds: Some(goal.time_used_seconds),
        evidence_refs: vec![goal_snapshot_ref, budget_state_ref],
        degraded_reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_goal_is_explicitly_degraded() {
        let snapshot = parent_goal_snapshot_from_goal("thread-a", None);

        assert!(snapshot.goal_snapshot_ref.is_none());
        assert_eq!(
            snapshot.degraded_reason.as_deref(),
            Some("thread/goal/get returned no active goal")
        );
    }

    #[test]
    fn active_goal_projects_native_goal_and_budget_refs() {
        let snapshot = parent_goal_snapshot_from_goal(
            "thread-a",
            Some(ThreadGoal {
                thread_id: "thread-a".to_string(),
                objective: "decide".to_string(),
                status: ThreadGoalStatus::Active,
                token_budget: Some(100),
                tokens_used: 25,
                time_used_seconds: 3,
                created_at: 1,
                updated_at: 2,
            }),
        );

        assert_eq!(snapshot.goal_status, Some(ThreadGoalStatus::Active));
        assert_eq!(snapshot.tokens_used, Some(25));
        assert_eq!(snapshot.evidence_refs.len(), 2);
        assert!(snapshot.degraded_reason.is_none());
    }
}
