use crate::error_code::invalid_params;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosArenaCompositionContract;
use codex_app_server_protocol::MemythosArenaCompositionProvisionParams;
use codex_app_server_protocol::MemythosArenaCompositionProvisionResponse;
use codex_app_server_protocol::MemythosArenaCompositionRevision;
use codex_app_server_protocol::MemythosArenaCompositionRevisionAction;
use codex_app_server_protocol::MemythosArenaCompositionRevisionActionKind;
use codex_app_server_protocol::MemythosArenaCostContext;
use codex_app_server_protocol::MemythosArenaCostEnvelopeMode;
use codex_app_server_protocol::MemythosArenaDecisionMethod;
use codex_app_server_protocol::MemythosArenaRequestParams;
use codex_app_server_protocol::MemythosParentRole;
use codex_app_server_protocol::MemythosParentStance;
use codex_protocol::openai_models::ReasoningEffort;
use std::collections::HashMap;
use std::collections::HashSet;

pub(super) fn is_competitive_method(method: MemythosArenaDecisionMethod) -> bool {
    matches!(
        method,
        MemythosArenaDecisionMethod::CompetitiveDebate
            | MemythosArenaDecisionMethod::BettingRound
            | MemythosArenaDecisionMethod::RankedSelection
    )
}

pub(super) fn validate_arena_composition_contract(
    params: &MemythosArenaCompositionProvisionParams,
) -> Result<(), JSONRPCErrorError> {
    let contract = &params.contract;
    if contract.contract_version.trim().is_empty()
        || contract.arena_id.trim().is_empty()
        || contract.shared_objective.trim().is_empty()
        || contract.effort_rationale.trim().is_empty()
        || contract.completion_criteria.is_empty()
        || contract.participants.is_empty()
    {
        return Err(invalid_params(
            "arena composition requires version, arena id, objective, completion criteria, and participants",
        ));
    }
    if params.room_id.trim().is_empty() {
        return Err(invalid_params(
            "arena composition room id must not be empty",
        ));
    }
    if contract.unresolved_role_gap.is_some() {
        return Err(invalid_params(
            "arena composition cannot be provisioned with an unresolved role gap",
        ));
    }

    let mut participant_ids = HashSet::new();
    let mut role_stances = HashSet::new();
    for participant in &contract.participants {
        if !participant_ids.insert(participant.participant_id.as_str()) {
            return Err(invalid_params(format!(
                "duplicate arena participant id: {}",
                participant.participant_id
            )));
        }
        if !role_stances.insert((participant.agent_role.as_str(), participant.stance.as_str())) {
            return Err(invalid_params(format!(
                "duplicate role/stance composition: {}/{}",
                participant.agent_role, participant.stance
            )));
        }
        if MemythosParentRole::from_wire(&participant.agent_role).is_none() {
            return Err(invalid_params(format!(
                "unsupported arena parent role: {}",
                participant.agent_role
            )));
        }
        if MemythosParentStance::from_wire(&participant.stance).is_none() {
            return Err(invalid_params(format!(
                "unsupported arena parent stance: {}",
                participant.stance
            )));
        }
        if participant.role_objective.trim().is_empty()
            || participant.expected_contribution.trim().is_empty()
            || participant.exit_condition.trim().is_empty()
            || participant.effort_intent.trim().is_empty()
        {
            return Err(invalid_params(format!(
                "participant {} has an incomplete role contract",
                participant.participant_id
            )));
        }
        if participant.token_budget.is_some_and(|budget| budget <= 0) {
            return Err(invalid_params(format!(
                "participant {} token budget must be positive when specified",
                participant.participant_id
            )));
        }
        if matches!(participant.reasoning_effort, ReasoningEffort::Custom(_)) {
            return Err(invalid_params(format!(
                "participant {} reasoning effort must use a native app-server value",
                participant.participant_id
            )));
        }
        if matches!(
            participant.reasoning_effort,
            ReasoningEffort::None | ReasoningEffort::Minimal
        ) {
            return Err(invalid_params(format!(
                "participant {} reasoning effort {} is incompatible with the active arena parent toolset; use low or greater",
                participant.participant_id,
                participant.reasoning_effort.as_str()
            )));
        }
        if !params.upstream_authority_scope.is_empty()
            && participant.authority_scope.iter().any(|scope| {
                !is_native_method_authority(&participant.agent_role, scope)
                    && !params.upstream_authority_scope.contains(scope)
            })
        {
            return Err(invalid_params(format!(
                "participant {} exceeds upstream authority scope",
                participant.participant_id
            )));
        }
    }

    let find_participant = |participant_id: &str| {
        contract
            .participants
            .iter()
            .find(|participant| participant.participant_id == participant_id)
    };
    if let Some(concierge_id) = contract.coordination.concierge_participant_id.as_deref() {
        let concierge = find_participant(concierge_id).ok_or_else(|| {
            invalid_params(format!("unknown concierge participant: {concierge_id}"))
        })?;
        if concierge.agent_role != "room_concierge" {
            return Err(invalid_params(
                "concierge participant must use the room_concierge role",
            ));
        }
    }
    if let Some(coordinator_id) = contract.coordination.coordinator_participant_id.as_deref() {
        let coordinator = find_participant(coordinator_id).ok_or_else(|| {
            invalid_params(format!("unknown coordinator participant: {coordinator_id}"))
        })?;
        if !matches!(
            coordinator.agent_role.as_str(),
            "process_steward" | "coordinator"
        ) {
            return Err(invalid_params(
                "coordinator participant must use the process_steward or coordinator role",
            ));
        }
        if contract.coordination.concierge_participant_id.as_deref() == Some(coordinator_id) {
            return Err(invalid_params(
                "coordinator and concierge must be independent participants",
            ));
        }
        let rationale = contract.rationale.to_ascii_lowercase();
        if !["exception", "regulat", "governance", "method conflict"]
            .iter()
            .any(|marker| rationale.contains(marker))
        {
            return Err(invalid_params(
                "an additional coordinator/process steward requires explicit exceptional-governance rationale",
            ));
        }
    }
    if let Some(judge_id) = contract.coordination.judge_participant_id.as_deref() {
        let judge = find_participant(judge_id)
            .ok_or_else(|| invalid_params(format!("unknown judge participant: {judge_id}")))?;
        if judge.agent_role != "judge" {
            return Err(invalid_params("judge participant must use the judge role"));
        }
    }
    if is_competitive_method(contract.coordination.decision_method) {
        let Some(round_policy) = contract.coordination.round_policy.as_ref() else {
            return Err(invalid_params(
                "competitive arena composition requires a round policy",
            ));
        };
        if round_policy.minimum_competing_positions < 2 {
            return Err(invalid_params(
                "competitive arena requires at least two competing positions",
            ));
        }
        let proposal_bearing_positions = contract
            .participants
            .iter()
            .filter(|participant| participant.agent_role == "bettor")
            .map(|participant| participant.stance.as_str())
            .collect::<HashSet<_>>()
            .len();
        if proposal_bearing_positions < round_policy.minimum_competing_positions as usize {
            return Err(invalid_params(format!(
                "competitive arena requires at least {} proposal-bearing parents with independent threads and stances",
                round_policy.minimum_competing_positions
            )));
        }
        if contract.coordination.concierge_participant_id.is_none()
            || contract.coordination.judge_participant_id.is_none()
        {
            return Err(invalid_params(
                "competitive arena requires independent Room Concierge and judge participants",
            ));
        }
    }
    validate_arena_cost_envelope(contract)?;
    Ok(())
}

pub(super) fn validate_arena_cost_envelope(
    contract: &MemythosArenaCompositionContract,
) -> Result<(), JSONRPCErrorError> {
    let envelope = &contract.cost_envelope;
    if envelope.rationale.trim().is_empty() {
        return Err(invalid_params("arena cost envelope requires rationale"));
    }
    let participant_budgets = contract
        .participants
        .iter()
        .map(|participant| participant.token_budget)
        .collect::<Vec<_>>();
    match envelope.mode {
        MemythosArenaCostEnvelopeMode::Open => {
            if envelope.total_token_budget.is_some()
                || envelope.coordination_token_budget.is_some()
                || envelope.substantive_token_budget.is_some()
                || participant_budgets.iter().any(Option::is_some)
            {
                return Err(invalid_params(
                    "open arena cost envelope requires null native token budgets",
                ));
            }
        }
        MemythosArenaCostEnvelopeMode::Calibrated => {
            if envelope.baseline_refs.is_empty() {
                return Err(invalid_params(
                    "calibrated arena cost envelope requires comparable baseline refs",
                ));
            }
            validate_bounded_arena_cost_envelope(contract)?;
        }
        MemythosArenaCostEnvelopeMode::ExplicitCap => {
            validate_bounded_arena_cost_envelope(contract)?;
        }
    }
    if is_competitive_method(contract.coordination.decision_method)
        && !envelope.method_integrity_funded
    {
        return Err(invalid_params(
            "competitive arena cost envelope must fund method integrity or select a different method",
        ));
    }
    Ok(())
}

pub(super) fn validate_planned_arena_cost_context(
    params: &MemythosArenaRequestParams,
    contract: &MemythosArenaCompositionContract,
) -> Result<(), JSONRPCErrorError> {
    let context = params.cost_context.as_ref();
    match contract.cost_envelope.mode {
        MemythosArenaCostEnvelopeMode::Open => Ok(()),
        MemythosArenaCostEnvelopeMode::ExplicitCap => {
            let explicit_cap = context
                .and_then(|context| context.explicit_token_cap)
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    invalid_params(
                        "planner selected explicit_cap without a positive caller token cap",
                    )
                })?;
            if contract.cost_envelope.total_token_budget != Some(explicit_cap) {
                return Err(invalid_params(format!(
                    "explicit arena cost envelope must equal caller cap {explicit_cap}"
                )));
            }
            Ok(())
        }
        MemythosArenaCostEnvelopeMode::Calibrated => {
            let accepted_refs = context
                .into_iter()
                .flat_map(|context| context.comparable_evidence.iter())
                .filter(|evidence| evidence.accepted_result && evidence.tokens_used > 0)
                .map(|evidence| evidence.evidence_ref.as_str())
                .collect::<HashSet<_>>();
            if contract
                .cost_envelope
                .baseline_refs
                .iter()
                .any(|reference| !accepted_refs.contains(reference.as_str()))
                || contract.cost_envelope.baseline_refs.is_empty()
            {
                return Err(invalid_params(
                    "calibrated arena cost envelope must cite only accepted comparable evidence supplied by the caller",
                ));
            }
            Ok(())
        }
    }
}

pub(super) fn validate_arena_cost_context(
    context: Option<&MemythosArenaCostContext>,
) -> Result<(), JSONRPCErrorError> {
    let Some(context) = context else {
        return Ok(());
    };
    if context.explicit_token_cap.is_some_and(|cap| cap <= 0) {
        return Err(invalid_params(
            "explicit arena token cap must be positive when supplied",
        ));
    }
    for evidence in &context.comparable_evidence {
        if evidence.evidence_ref.trim().is_empty()
            || evidence.tokens_used <= 0
            || evidence.comparability_rationale.trim().is_empty()
        {
            return Err(invalid_params(
                "comparable cost evidence requires a ref, positive token usage, and comparability rationale",
            ));
        }
    }
    Ok(())
}

fn validate_bounded_arena_cost_envelope(
    contract: &MemythosArenaCompositionContract,
) -> Result<(), JSONRPCErrorError> {
    let envelope = &contract.cost_envelope;
    let total = envelope
        .total_token_budget
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_params("bounded arena cost envelope requires a positive total"))?;
    let coordination = envelope
        .coordination_token_budget
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            invalid_params("bounded arena cost envelope requires coordination budget")
        })?;
    let substantive = envelope
        .substantive_token_budget
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_params("bounded arena cost envelope requires substantive budget"))?;
    let participant_total = contract
        .participants
        .iter()
        .map(|participant| {
            participant.token_budget.ok_or_else(|| {
                invalid_params(format!(
                    "bounded arena cost envelope requires participant {} token budget",
                    participant.participant_id
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<i64>();
    if participant_total != total || coordination + substantive != total {
        return Err(invalid_params(format!(
            "arena cost envelope totals are inconsistent: participant={participant_total}, coordination+substantive={}, total={total}",
            coordination + substantive
        )));
    }
    let coordination_participant_total = contract
        .participants
        .iter()
        .filter(|participant| {
            matches!(
                participant.agent_role.as_str(),
                "room_concierge" | "process_steward" | "coordinator"
            )
        })
        .map(|participant| participant.token_budget.unwrap_or_default())
        .sum::<i64>();
    if coordination_participant_total != coordination {
        return Err(invalid_params(
            "coordination token budget must equal the native goal budgets of the Room Concierge and any justified process steward",
        ));
    }
    Ok(())
}

pub(super) fn is_native_method_authority(agent_role: &str, scope: &str) -> bool {
    matches!(
        (agent_role, scope),
        ("room_concierge", "coordinate")
            | ("room_concierge", "delegate")
            | ("process_steward", "coordinate")
            | ("coordinator", "coordinate")
            | ("judge", "judge")
    )
}

pub(super) fn validate_arena_composition_revision(
    params: &MemythosArenaCompositionProvisionParams,
    previous: Option<&MemythosArenaCompositionProvisionResponse>,
) -> Result<(), JSONRPCErrorError> {
    let Some(previous) = previous else {
        if params.revision.is_some() {
            return Err(invalid_params(
                "initial arena composition cannot declare a revision",
            ));
        }
        return Ok(());
    };
    let revision = params.revision.as_ref().ok_or_else(|| {
        invalid_params("an active arena composition requires an explicit add/keep/retire revision")
    })?;
    if revision.revision_id.trim().is_empty()
        || revision.trigger.trim().is_empty()
        || revision.rationale.trim().is_empty()
        || revision.previous_contract_ref.trim().is_empty()
    {
        return Err(invalid_params(
            "arena composition revision requires id, trigger, rationale, and previous contract ref",
        ));
    }
    if revision.previous_version != previous.composition_version
        || revision.next_version != previous.composition_version + 1
    {
        return Err(invalid_params(format!(
            "arena composition revision version must advance {} -> {}",
            previous.composition_version,
            previous.composition_version + 1
        )));
    }
    let expected_ref = previous.event_refs.first().cloned().unwrap_or_default();
    if revision.previous_contract_ref != expected_ref {
        return Err(invalid_params(format!(
            "arena composition revision must reference previous contract {expected_ref}"
        )));
    }

    let previous_participants = previous
        .contract
        .participants
        .iter()
        .map(|participant| (participant.participant_id.as_str(), participant))
        .collect::<HashMap<_, _>>();
    let next_participants = params
        .contract
        .participants
        .iter()
        .map(|participant| (participant.participant_id.as_str(), participant))
        .collect::<HashMap<_, _>>();
    let mut action_ids = HashSet::new();
    for action in &revision.actions {
        if !action_ids.insert(action.participant_id.as_str()) {
            return Err(invalid_params(format!(
                "duplicate revision action for participant {}",
                action.participant_id
            )));
        }
        if action.reason.trim().is_empty() {
            return Err(invalid_params(format!(
                "revision action for participant {} requires a reason",
                action.participant_id
            )));
        }
        match action.action {
            MemythosArenaCompositionRevisionActionKind::Keep => {
                let previous_participant = previous_participants
                    .get(action.participant_id.as_str())
                    .ok_or_else(|| invalid_params("keep action references a new participant"))?;
                let next_participant = next_participants
                    .get(action.participant_id.as_str())
                    .ok_or_else(|| {
                        invalid_params("kept participant is absent from next composition")
                    })?;
                if previous_participant.agent_role != next_participant.agent_role
                    || previous_participant.stance != next_participant.stance
                {
                    return Err(invalid_params(format!(
                        "live participant {} cannot change role or stance; retire and add a new participant",
                        action.participant_id
                    )));
                }
                let lease = previous
                    .leases
                    .iter()
                    .find(|lease| lease.participant_id == action.participant_id)
                    .ok_or_else(|| invalid_params("kept participant has no active lease"))?;
                if action.thread_id.as_deref() != Some(lease.thread_id.as_str()) {
                    return Err(invalid_params(format!(
                        "kept participant {} must preserve thread {}",
                        action.participant_id, lease.thread_id
                    )));
                }
            }
            MemythosArenaCompositionRevisionActionKind::Add => {
                if previous_participants.contains_key(action.participant_id.as_str())
                    || !next_participants.contains_key(action.participant_id.as_str())
                    || action.thread_id.is_some()
                {
                    return Err(invalid_params(format!(
                        "add action for {} must describe a new participant without a preselected thread",
                        action.participant_id
                    )));
                }
            }
            MemythosArenaCompositionRevisionActionKind::Retire => {
                let lease = previous
                    .leases
                    .iter()
                    .find(|lease| lease.participant_id == action.participant_id)
                    .ok_or_else(|| {
                        invalid_params("retire action references a non-active participant")
                    })?;
                if next_participants.contains_key(action.participant_id.as_str())
                    || action.thread_id.as_deref() != Some(lease.thread_id.as_str())
                {
                    return Err(invalid_params(format!(
                        "retire action for {} must remove its exact active thread",
                        action.participant_id
                    )));
                }
            }
        }
    }
    for participant_id in previous_participants.keys() {
        if !action_ids.contains(participant_id) {
            return Err(invalid_params(format!(
                "previous participant {participant_id} requires keep or retire action"
            )));
        }
    }
    for participant_id in next_participants.keys() {
        if !action_ids.contains(participant_id) {
            return Err(invalid_params(format!(
                "next participant {participant_id} requires keep or add action"
            )));
        }
    }
    Ok(())
}

pub(super) fn build_native_composition_revision(
    params: &MemythosArenaRequestParams,
    previous: &MemythosArenaCompositionProvisionResponse,
    next: &MemythosArenaCompositionContract,
) -> Result<MemythosArenaCompositionRevision, JSONRPCErrorError> {
    let trigger = params
        .composition_change_signal
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            invalid_params(
                "an active arena requires compositionChangeSignal for native agentic replanning",
            )
        })?;
    let previous_by_id = previous
        .contract
        .participants
        .iter()
        .map(|participant| (participant.participant_id.as_str(), participant))
        .collect::<HashMap<_, _>>();
    let next_by_id = next
        .participants
        .iter()
        .map(|participant| (participant.participant_id.as_str(), participant))
        .collect::<HashMap<_, _>>();
    let lease_by_id = previous
        .leases
        .iter()
        .map(|lease| (lease.participant_id.as_str(), lease))
        .collect::<HashMap<_, _>>();
    let mut actions = Vec::new();
    for participant in &previous.contract.participants {
        let lease = lease_by_id
            .get(participant.participant_id.as_str())
            .ok_or_else(|| invalid_params("active composition participant has no lease"))?;
        match next_by_id.get(participant.participant_id.as_str()) {
            Some(next_participant)
                if next_participant.agent_role == participant.agent_role
                    && next_participant.stance == participant.stance =>
            {
                actions.push(MemythosArenaCompositionRevisionAction {
                    action: MemythosArenaCompositionRevisionActionKind::Keep,
                    participant_id: participant.participant_id.clone(),
                    thread_id: Some(lease.thread_id.clone()),
                    reason: "native planner preserved role and stance identity".to_string(),
                });
            }
            Some(_) => {
                return Err(invalid_params(format!(
                    "native planner changed role or stance for live participant {}; replacements require retire plus a new participant id",
                    participant.participant_id
                )));
            }
            None => actions.push(MemythosArenaCompositionRevisionAction {
                action: MemythosArenaCompositionRevisionActionKind::Retire,
                participant_id: participant.participant_id.clone(),
                thread_id: Some(lease.thread_id.clone()),
                reason: format!("native replanning retired this contribution after: {trigger}"),
            }),
        }
    }
    for participant in &next.participants {
        if !previous_by_id.contains_key(participant.participant_id.as_str()) {
            actions.push(MemythosArenaCompositionRevisionAction {
                action: MemythosArenaCompositionRevisionActionKind::Add,
                participant_id: participant.participant_id.clone(),
                thread_id: None,
                reason: format!("native replanning added this contribution after: {trigger}"),
            });
        }
    }
    Ok(MemythosArenaCompositionRevision {
        revision_id: format!(
            "{}-revision-{}",
            params.arena_id,
            previous.composition_version + 1
        ),
        previous_version: previous.composition_version,
        next_version: previous.composition_version + 1,
        previous_contract_ref: previous.event_refs.first().cloned().unwrap_or_default(),
        trigger: trigger.to_string(),
        rationale: next.rationale.clone(),
        actions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn competitive_methods_are_explicit() {
        assert!(is_competitive_method(
            MemythosArenaDecisionMethod::CompetitiveDebate
        ));
        assert!(is_competitive_method(
            MemythosArenaDecisionMethod::RankedSelection
        ));
        assert!(!is_competitive_method(
            MemythosArenaDecisionMethod::SingleExpert
        ));
    }

    #[test]
    fn method_authority_does_not_grant_business_authority() {
        assert!(is_native_method_authority("room_concierge", "delegate"));
        assert!(is_native_method_authority("judge", "judge"));
        assert!(!is_native_method_authority(
            "room_concierge",
            "approve_budget"
        ));
    }
}
