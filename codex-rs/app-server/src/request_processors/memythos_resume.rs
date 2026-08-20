use crate::error_code::invalid_params;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaCompositionContract;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaRequestParams;
use codex_app_server_protocol::MemythosArenaResumeAssessment;
use codex_app_server_protocol::MemythosArenaResumeDisposition;
use codex_app_server_protocol::MemythosArenaResumeExecutionMode;
use codex_app_server_protocol::MemythosArenaResumeExecutionPlan;
use std::collections::HashSet;

pub(super) fn build_arena_intake_prompt(
    params: &MemythosArenaRequestParams,
    contract: &MemythosArenaCompositionContract,
    execution_plan: &MemythosArenaResumeExecutionPlan,
) -> String {
    let participants = contract
        .participants
        .iter()
        .map(|participant| {
            format!(
                "- {}: role={}, stance={}, objective={}",
                participant.participant_id,
                participant.agent_role,
                participant.stance,
                participant.role_objective
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let round_policy = contract
        .coordination
        .round_policy
        .as_ref()
        .map(|policy| {
            format!(
                "minimum_competing_positions={}, cross_read_required={}, objection_required={}, explicit_bet_required={}",
                policy.minimum_competing_positions,
                policy.cross_read_required,
                policy.objection_required,
                policy.explicit_bet_required
            )
        })
        .unwrap_or_else(|| "not required by the selected method".to_string());
    let execution_instruction = match execution_plan.mode {
        MemythosArenaResumeExecutionMode::InitialRound
        | MemythosArenaResumeExecutionMode::FullRound => concat!(
            "App-server will dispatch exactly one independent peer_proposal assignment to every ",
            "proposal-bearing bettor after this intake turn completes. Your responsibility is to ",
            "frame the objective, semantic boundaries, and material exceptions without issuing ",
            "phase commands. The native phase plan aggregates all proposals, fans the sealed proposal checkpoint ",
            "out to every bettor for cross-read and objection, aggregates those responses, fans ",
            "the sealed review checkpoint out for final bets, and activates the Judge exactly ",
            "once. These are mechanical mailbox transitions under the arena contract, not new ",
            "semantic decisions. Do not issue separate cross-read, bet, or verdict requests after ",
            "proposals are dispatched. The verdict must identify the winning participant by its ",
            "exact participant id, rank the alternatives, preserve dissent, state reopening ",
            "signals, and report whether closed decisions remained preserved."
        )
        .to_string(),
        MemythosArenaResumeExecutionMode::ReassessAffectedPositions => format!(
            concat!(
                "This is a bounded partial resume. App-server will dispatch exactly one ",
                "resume_reassessment assignment to each affected participant id after this intake ",
                "turn completes, with no proposal, cross-read, or separate bet assignment. Each ",
                "affected parent uses OOTB thread memory and the ",
                "cited novelty refs to return one changed position, remaining material objections, ",
                "revised bet, and reopening breakpoints. Affected participant ids: {}. ",
                "Source round: {}. Cited change refs: {}."
            ),
            execution_plan.affected_participant_ids.join(", "),
            execution_plan.source_round_id.as_deref().unwrap_or("none"),
            execution_plan.cited_change_refs.join(", "),
        ),
        MemythosArenaResumeExecutionMode::RetainDecision => {
            "The prior decision is retained; do not dispatch arena work.".to_string()
        }
    };
    format!(
        "Client request origin: {}\nCase: {}\nLayer objective: {}\nExpected deliverable: {}\nCompletion criteria:\n- {}\nClosed decisions:\n- {}\nUncertainties:\n- {}\nReality evidence:\n- {}\nCost goal: {}\n{}\n\nNative arena contract:\nDecision method: {:?}\nRound policy: {}\nParticipants:\n{}\n\nNative resume execution mode: {:?}\n{}\n\nThis request activates one autonomous native arena run. You are the Room Concierge and own the arena objective, initial framing, exceptions, dependencies, and communication; the client will only observe. You are not a proposer and you do not decide the business outcome. Frame the authorized work and end this turn; app-server dispatches the phase assignments after your turn completes. Do not call tools to activate proposals, reassessments, cross-reads, bets, or the Judge. Mechanical mailbox transitions are app-server responsibilities, not new semantic decisions. A material exception wakes you; an ordinary checkpoint does not. The Judge verdict is queued back into your native mailbox for continuity and closes the successful round without requiring you to restate or re-judge it. Never bet or judge as Room Concierge. Do not keep a concierge turn alive while peers work. Do not ask the client to activate phases, create parents, assemble contracts, or recover partial provisioning; those are app-server responsibilities.",
        params.request_origin,
        params.case_brief,
        params.layer_objective,
        params.expected_deliverable,
        params.completion_criteria.join("\n- "),
        params.closed_decisions.join("\n- "),
        params.uncertainties.join("\n- "),
        params.reality_evidence.join("\n- "),
        params.cost_goal,
        params.resume_context.as_ref().map(|resume| format!(
            "Resume semantic boundary:\nProtected decisions:\n- {}\nRevisable settlement:\n- {}\nOpen implementation scope:\n- {}",
            resume.protected_decisions.join("\n- "),
            resume.revisable_settlement.join("\n- "),
            resume.open_implementation_scope.join("\n- "),
        )).unwrap_or_else(|| "Resume semantic boundary: initial round; no prior settlement scope supplied.".to_string()),
        contract.coordination.decision_method,
        round_policy,
        participants,
        execution_plan.mode,
        execution_instruction,
    )
}

pub(super) fn validate_resume_execution_plan_message(
    plan: &MemythosArenaResumeExecutionPlan,
    round_id: &str,
    message_kind: &str,
    source_role: &str,
    target_thread_id: &str,
    target_is_affected: bool,
) -> Result<(), JSONRPCErrorError> {
    match plan.mode {
        MemythosArenaResumeExecutionMode::ReassessAffectedPositions => {
            if source_role == "room_concierge" {
                if message_kind != "resume_reassessment" {
                    return Err(invalid_params(format!(
                        "partial resume round {round_id} only authorizes resume_reassessment assignments; received {message_kind}"
                    )));
                }
                if !target_is_affected {
                    return Err(invalid_params(format!(
                        "partial resume target thread {target_thread_id} is not leased to the native affected participant set"
                    )));
                }
            }
        }
        MemythosArenaResumeExecutionMode::InitialRound
        | MemythosArenaResumeExecutionMode::FullRound => {
            if message_kind == "resume_reassessment" {
                return Err(invalid_params(format!(
                    "round {round_id} uses a full phase plan and cannot dispatch resume_reassessment"
                )));
            }
        }
        MemythosArenaResumeExecutionMode::RetainDecision => {
            return Err(invalid_params(format!(
                "retained round {round_id} cannot dispatch arena work"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_native_resume_assessment(
    assessment: &MemythosArenaResumeAssessment,
    previous: &MemythosArenaCompositionProvisionResponse,
) -> Result<(), JSONRPCErrorError> {
    if assessment.rationale.trim().is_empty() {
        return Err(invalid_params(
            "native novelty assessment requires a rationale",
        ));
    }
    let active_ids = previous
        .contract
        .participants
        .iter()
        .map(|participant| participant.participant_id.as_str())
        .collect::<HashSet<_>>();
    if let Some(unknown) = assessment
        .affected_participant_ids
        .iter()
        .find(|participant_id| !active_ids.contains(participant_id.as_str()))
    {
        return Err(invalid_params(format!(
            "native novelty assessment references inactive participant {unknown}"
        )));
    }
    let plan = &assessment.resume_execution_plan;
    if plan.affected_participant_ids != assessment.affected_participant_ids
        || plan.affected_decision_refs != assessment.affected_decision_refs
        || plan.cited_change_refs != assessment.cited_change_refs
    {
        return Err(invalid_params(
            "native resume execution plan must exactly mirror affected participants, decisions, and cited change refs from its assessment",
        ));
    }
    let expected_source_round_id = format!(
        "{}-round-{}",
        previous.contract.arena_id, previous.composition_version
    );
    if plan.source_round_id.as_deref() != Some(expected_source_round_id.as_str()) {
        return Err(invalid_params(format!(
            "native resume execution plan must cite source round {expected_source_round_id}"
        )));
    }
    match assessment.disposition {
        MemythosArenaResumeDisposition::InitialRound => Err(invalid_params(
            "active arena novelty assessment cannot return initial_round",
        )),
        MemythosArenaResumeDisposition::RetainDecision => {
            if !assessment.affected_participant_ids.is_empty()
                || assessment.comparability_invalidated
                || !assessment.avoided_full_round
                || plan.mode != MemythosArenaResumeExecutionMode::RetainDecision
            {
                return Err(invalid_params(
                    "retain_decision must preserve comparability, affect no participants, and record the avoided full round",
                ));
            }
            Ok(())
        }
        MemythosArenaResumeDisposition::PartialResume => {
            if assessment.affected_participant_ids.is_empty()
                || assessment.cited_change_refs.is_empty()
                || assessment.comparability_invalidated
                || !assessment.avoided_full_round
                || plan.mode != MemythosArenaResumeExecutionMode::ReassessAffectedPositions
            {
                return Err(invalid_params(
                    "partial_resume requires affected participants and cited change refs while preserving comparability and avoiding a full round",
                ));
            }
            Ok(())
        }
        MemythosArenaResumeDisposition::FullRound => {
            if assessment.cited_change_refs.is_empty()
                || assessment.affected_decision_refs.is_empty()
                || !assessment.comparability_invalidated
                || assessment.avoided_full_round
                || plan.mode != MemythosArenaResumeExecutionMode::FullRound
            {
                return Err(invalid_params(
                    "full_round requires cited change refs, affected decisions, and explicit comparability invalidation",
                ));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(mode: MemythosArenaResumeExecutionMode) -> MemythosArenaResumeExecutionPlan {
        MemythosArenaResumeExecutionPlan {
            mode,
            source_round_id: Some("arena-round-1".to_string()),
            affected_participant_ids: vec!["bettor-growth".to_string()],
            affected_decision_refs: Vec::new(),
            cited_change_refs: vec!["evidence://change".to_string()],
        }
    }

    #[test]
    fn partial_resume_only_dispatches_reassessment_to_affected_targets() {
        let partial = plan(MemythosArenaResumeExecutionMode::ReassessAffectedPositions);

        assert!(
            validate_resume_execution_plan_message(
                &partial,
                "round-2",
                "peer_proposal",
                "room_concierge",
                "thread-growth",
                true,
            )
            .is_err()
        );
        assert!(
            validate_resume_execution_plan_message(
                &partial,
                "round-2",
                "resume_reassessment",
                "room_concierge",
                "thread-risk",
                false,
            )
            .is_err()
        );
        assert!(
            validate_resume_execution_plan_message(
                &partial,
                "round-2",
                "resume_reassessment",
                "room_concierge",
                "thread-growth",
                true,
            )
            .is_ok()
        );
    }

    #[test]
    fn full_round_rejects_partial_reassessment_messages() {
        let full = plan(MemythosArenaResumeExecutionMode::FullRound);

        assert!(
            validate_resume_execution_plan_message(
                &full,
                "round-2",
                "resume_reassessment",
                "room_concierge",
                "thread-growth",
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn retained_decision_rejects_all_dispatch() {
        let retained = plan(MemythosArenaResumeExecutionMode::RetainDecision);

        assert!(
            validate_resume_execution_plan_message(
                &retained,
                "round-2",
                "peer_proposal",
                "room_concierge",
                "thread-growth",
                true,
            )
            .is_err()
        );
    }
}
