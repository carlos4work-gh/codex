use crate::request_processors::memythos_contracts::validate_responses_output_schema;
use codex_app_server_protocol::JSONRPCErrorError;
use serde::Deserialize;
use std::collections::HashSet;

pub(super) fn native_judge_verdict_output_schema(
    eligible_winner_ids: &[String],
) -> Result<serde_json::Value, JSONRPCErrorError> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "winner_participant_id": { "type": "string", "enum": eligible_winner_ids },
            "ranked_alternatives": {
                "type": "array",
                "items": { "type": "string", "enum": eligible_winner_ids }
            },
            "winning_decision": { "type": "string" },
            "accepted_tradeoff": { "type": "string" },
            "next_action": {
                "type": "string",
                "enum": ["close", "targeted_refinement", "parent_rollup"]
            },
            "contribution_attribution": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "participant_id": { "type": "string", "enum": eligible_winner_ids },
                        "claim_refs": { "type": "array", "items": { "type": "string" } },
                        "disposition": {
                            "type": "string",
                            "enum": ["adopted", "conditioned", "rejected", "preserved_dissent"]
                        },
                        "rationale": { "type": "string" }
                    },
                    "required": ["participant_id", "claim_refs", "disposition", "rationale"],
                    "additionalProperties": false
                }
            },
            "dissent": { "type": "string" },
            "preserved_dissent": { "type": "array", "items": { "type": "string" } },
            "targeted_refinements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "participant_id": { "type": "string", "enum": eligible_winner_ids },
                        "tension": { "type": "string" },
                        "request": { "type": "string" },
                        "sufficiency_criterion": { "type": "string" }
                    },
                    "required": ["participant_id", "tension", "request", "sufficiency_criterion"],
                    "additionalProperties": false
                }
            },
            "reopening_signals": { "type": "array", "items": { "type": "string" } },
            "protected_decisions_status": {
                "type": "string",
                "enum": ["preserved", "reopened"]
            },
            "reopened_decision_refs": { "type": "array", "items": { "type": "string" } },
            "resume_scope_status": {
                "type": "string",
                "enum": ["not_applicable", "retained", "partially_reopened", "fully_reopened"]
            },
            "rationale": { "type": "string" }
        },
        "required": [
            "winner_participant_id", "ranked_alternatives", "winning_decision",
            "accepted_tradeoff", "next_action", "contribution_attribution", "dissent",
            "preserved_dissent", "targeted_refinements", "reopening_signals",
            "protected_decisions_status", "reopened_decision_refs", "resume_scope_status",
            "rationale"
        ],
        "additionalProperties": false
    });
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

pub(super) fn native_refinement_delta_output_schema(
    participant_id: &str,
) -> Result<serde_json::Value, JSONRPCErrorError> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "participant_id": { "type": "string", "enum": [participant_id] },
            "incorporated_attribution_refs": { "type": "array", "items": { "type": "string" } },
            "refinement_delta": { "type": "string" },
            "evidence_refs": { "type": "array", "items": { "type": "string" } },
            "remaining_tension": { "type": "string" },
            "sufficiency_criterion": { "type": "string" },
            "sufficiency_met": { "type": "boolean" },
            "sufficiency_rationale": { "type": "string" },
            "parent_rollup_required": { "type": "boolean" },
            "parent_rollup_question": { "type": "string" }
        },
        "required": [
            "participant_id", "incorporated_attribution_refs", "refinement_delta",
            "evidence_refs", "remaining_tension", "sufficiency_criterion", "sufficiency_met",
            "sufficiency_rationale", "parent_rollup_required", "parent_rollup_question"
        ],
        "additionalProperties": false
    });
    validate_responses_output_schema(&schema)?;
    Ok(schema)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJudgeVerdict {
    winner_participant_id: String,
    ranked_alternatives: Vec<String>,
    pub(super) winning_decision: String,
    pub(super) accepted_tradeoff: String,
    pub(super) next_action: String,
    pub(super) contribution_attribution: Vec<NativeJudgeContributionAttribution>,
    dissent: String,
    pub(super) preserved_dissent: Vec<String>,
    pub(super) targeted_refinements: Vec<NativeJudgeTargetedRefinement>,
    reopening_signals: Vec<String>,
    protected_decisions_status: String,
    reopened_decision_refs: Vec<String>,
    resume_scope_status: String,
    rationale: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJudgeContributionAttribution {
    pub(super) participant_id: String,
    pub(super) claim_refs: Vec<String>,
    pub(super) disposition: String,
    pub(super) rationale: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJudgeTargetedRefinement {
    pub(super) participant_id: String,
    pub(super) tension: String,
    pub(super) request: String,
    pub(super) sufficiency_criterion: String,
}

pub(super) fn is_valid_native_judge_verdict(
    text: &str,
    eligible_winner_ids: &HashSet<&str>,
) -> bool {
    let Ok(verdict) = serde_json::from_str::<NativeJudgeVerdict>(text) else {
        return false;
    };
    let _schema_required_fields = (
        &verdict.winning_decision,
        &verdict.accepted_tradeoff,
        &verdict.dissent,
        &verdict.preserved_dissent,
        &verdict.reopening_signals,
        &verdict.rationale,
        &verdict.reopened_decision_refs,
    );
    let attributed_participants = verdict
        .contribution_attribution
        .iter()
        .map(|attribution| attribution.participant_id.as_str())
        .collect::<HashSet<_>>();
    let refinement_participants = verdict
        .targeted_refinements
        .iter()
        .map(|refinement| refinement.participant_id.as_str())
        .collect::<HashSet<_>>();
    eligible_winner_ids.contains(verdict.winner_participant_id.as_str())
        && verdict.ranked_alternatives.len() + 1 == eligible_winner_ids.len()
        && verdict.ranked_alternatives.iter().all(|participant_id| {
            participant_id != &verdict.winner_participant_id
                && eligible_winner_ids.contains(participant_id.as_str())
        })
        && verdict
            .ranked_alternatives
            .iter()
            .collect::<HashSet<_>>()
            .len()
            == verdict.ranked_alternatives.len()
        && &attributed_participants == eligible_winner_ids
        && verdict.contribution_attribution.len() == eligible_winner_ids.len()
        && verdict.contribution_attribution.iter().all(|attribution| {
            let _semantic_fields = (&attribution.claim_refs, &attribution.rationale);
            matches!(
                attribution.disposition.as_str(),
                "adopted" | "conditioned" | "rejected" | "preserved_dissent"
            )
        })
        && verdict.targeted_refinements.iter().all(|refinement| {
            eligible_winner_ids.contains(refinement.participant_id.as_str())
                && !refinement.tension.trim().is_empty()
                && !refinement.request.trim().is_empty()
                && !refinement.sufficiency_criterion.trim().is_empty()
        })
        && refinement_participants.len() == verdict.targeted_refinements.len()
        && match verdict.next_action.as_str() {
            "targeted_refinement" => !verdict.targeted_refinements.is_empty(),
            "close" | "parent_rollup" => verdict.targeted_refinements.is_empty(),
            _ => false,
        }
        && matches!(
            verdict.protected_decisions_status.as_str(),
            "preserved" | "reopened"
        )
        && matches!(
            verdict.resume_scope_status.as_str(),
            "not_applicable" | "retained" | "partially_reopened" | "fully_reopened"
        )
}

pub(super) fn native_judge_next_action(
    text: &str,
    eligible_winner_ids: &HashSet<&str>,
) -> Option<String> {
    let verdict = serde_json::from_str::<NativeJudgeVerdict>(text).ok()?;
    is_valid_native_judge_verdict(text, eligible_winner_ids).then_some(verdict.next_action)
}

pub(super) fn native_judge_checkpoint_prompt(
    message: &str,
    eligible_winner_ids: &[String],
) -> String {
    format!(
        "{message}\n\nNative verdict boundary: all expected bets are now sealed in your native mailbox. The eligible winner participant ids are [{}]. Return only the JSON object required by the native output schema. Select exactly one eligible winner. In `ranked_alternatives`, rank every other eligible participant exactly once and never include the winner. State the winning decision and accepted tradeoff, preserve dissent, and state reopening signals. Attribute every eligible bettor exactly once: identify claim refs where available, classify the contribution as adopted, conditioned, rejected, or preserved_dissent, and explain why. Credit useful evidence even when its parent did not win; do not reward persistence after refutation and do not turn this theoretical verdict into a persistent reputation score. Set `next_action=close` when the decision is sufficient, `parent_rollup` when missing authority or business definition prevents closure, or `targeted_refinement` only when a named bettor can resolve one localized tension without repeating proposal, cross-read, and bet. For targeted refinement, emit one unique mandate per selected participant with the exact tension, request, and observable sufficiency criterion; otherwise return an empty targeted_refinements array. The winner identifies the contribution that best resolves this bounded arena objective; winning a round does not by itself make that participant's hypothesis the global lead diagnosis, override protected decisions, or grant authority beyond the active objective. Keep that distinction explicit whenever the evidence still requires a mixed, provisional, or unsettled posture. `protected_decisions_status` measures only whether protected guardrails, authority boundaries, or explicit invariants remain valid; changing an affected winner, hypothesis weight, or bounded decision does not reopen protected decisions. Report every bounded decision changed by this resume in `reopened_decision_refs`, using native refs when available and stable semantic refs otherwise; use an empty array when none changed. `resume_scope_status` separately describes the work scope: use `not_applicable` for an initial round, `retained` when a resume changes no hypothesis or decision scope, `partially_reopened` when new evidence reopens only affected hypotheses, weights, or bounded scope while protected decisions remain valid, and `fully_reopened` only when the whole decision scope must be reconsidered. A partial resume therefore normally reports `protected_decisions_status=preserved`, one or more `reopened_decision_refs`, and `resume_scope_status=partially_reopened`. On a resumed composition, explain only changed evidence, changed bounded decisions, and remaining dissent; reference unchanged contract constraints without reproducing them. Identify the evidence and affected authority in the rationale. Your completed parent response is returned automatically to the Room Concierge as messageKind `judge_verdict`; do not send a second verdict and do not wait for a separate verdict request.",
        eligible_winner_ids.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_schema_restricts_winners_and_unknown_fields() {
        let schema =
            native_judge_verdict_output_schema(&["bettor-a".to_string()]).expect("judge schema");
        assert_eq!(
            schema["properties"]["winner_participant_id"]["enum"],
            serde_json::json!(["bettor-a"])
        );
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    }

    #[test]
    fn legacy_text_is_not_a_native_judge_verdict() {
        let eligible = HashSet::from(["bettor-a"]);
        assert!(!is_valid_native_judge_verdict(
            "winner_participant_id: bettor-a",
            &eligible,
        ));
    }
}
