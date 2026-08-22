use crate::error_code::invalid_params;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::MemythosThreadConsolidateParams;
use codex_app_server_protocol::MemythosThreadConsolidationSourceRef;
use codex_app_server_protocol::MemythosThreadContractAssembleParams;

pub(super) const MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS: usize = 240;

pub(super) fn compact_event_refs(mut event_refs: Vec<String>) -> Vec<String> {
    event_refs.retain(|event_ref| !event_ref.trim().is_empty());
    event_refs.sort();
    event_refs.dedup();
    event_refs
}

pub(super) fn compact_summary(summary: String) -> String {
    let normalized = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS {
        return normalized;
    }

    let mut compacted: String = normalized
        .chars()
        .take(MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS.saturating_sub(1))
        .collect();
    compacted.push('…');
    compacted
}

pub(super) fn normalize_consolidation_items_view(items_view: Option<&str>) -> &'static str {
    match items_view {
        Some("summary") | None => "summary",
        Some("full") => "full",
        Some("notLoaded") => "notLoaded",
        Some(_) => "summary",
    }
}

pub(super) fn empty_consolidation_source_ref(
    thread_id: &str,
    cursor: Option<String>,
    items_view: &str,
) -> MemythosThreadConsolidationSourceRef {
    MemythosThreadConsolidationSourceRef {
        thread_id: thread_id.to_string(),
        turn_refs: Vec::new(),
        items_view: items_view.to_string(),
        cursor: cursor.clone(),
        next_cursor: cursor,
        latest_agent_message_ref: None,
        latest_agent_message_text: None,
        technical_evidence_refs: Vec::new(),
    }
}

pub(super) fn build_thread_consolidation_prompt(
    params: &MemythosThreadConsolidateParams,
) -> String {
    format!(
        concat!(
            "Consolida la informacion de los hilos fuente como coordinador Memythos.\n",
            "No estas recibiendo una instruccion humana directa; estas coordinando una arena.\n",
            "Usa el contexto de aplicacion `memythos.thread_consolidation` como evidencia.\n",
            "No copies JSON ni logs tecnicos en la respuesta conversacional.\n",
            "Devuelve una sintesis natural con acuerdos, disensos, definiciones faltantes y siguiente accion.\n",
            "\n",
            "Proposito: {purpose:?}\n",
            "Modo de autoridad: {authority_mode:?}\n",
            "Instrucciones: {instructions}\n"
        ),
        purpose = params.purpose,
        authority_mode = params.authority_mode,
        instructions = params.instructions
    )
}

pub(super) fn contract_source_evidence_refs(
    source_refs: &[MemythosThreadConsolidationSourceRef],
    technical_evidence_refs: &[String],
    agent_message_ref: Option<&str>,
    structured_output_ref: Option<&str>,
) -> Vec<String> {
    let mut refs = source_refs
        .iter()
        .flat_map(|source| {
            source
                .turn_refs
                .iter()
                .chain(source.technical_evidence_refs.iter())
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    refs.extend(technical_evidence_refs.iter().cloned());
    if let Some(agent_message_ref) = agent_message_ref {
        refs.push(agent_message_ref.to_string());
    }
    if let Some(structured_output_ref) = structured_output_ref {
        refs.push(structured_output_ref.to_string());
    }
    compact_event_refs(refs)
}

pub(super) fn sanitize_contract_ref_segment(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        "contract".to_string()
    } else {
        normalized
    }
}

pub(super) fn validate_thread_consolidation_request(
    params: &MemythosThreadConsolidateParams,
) -> Result<(), JSONRPCErrorError> {
    if params.coordinator_thread_id.trim().is_empty() {
        return Err(invalid_params(
            "coordinatorThreadId is required".to_string(),
        ));
    }
    if params.source_thread_ids.is_empty() {
        return Err(invalid_params(
            "sourceThreadIds must contain at least one thread".to_string(),
        ));
    }
    if params
        .source_thread_ids
        .iter()
        .any(|thread_id| thread_id.trim().is_empty())
    {
        return Err(invalid_params(
            "sourceThreadIds cannot contain empty thread ids".to_string(),
        ));
    }
    if params.instructions.trim().is_empty() {
        return Err(invalid_params("instructions is required".to_string()));
    }
    if let Some(items_view) = params.items_view.as_deref() {
        match items_view {
            "summary" | "full" | "notLoaded" => {}
            other => {
                return Err(invalid_params(format!(
                    "itemsView must be summary, full or notLoaded; got {other}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_thread_contract_assemble_request(
    params: &MemythosThreadContractAssembleParams,
) -> Result<(), JSONRPCErrorError> {
    if params.coordinator_thread_id.trim().is_empty() {
        return Err(invalid_params(
            "coordinatorThreadId is required".to_string(),
        ));
    }
    if params.source_thread_ids.is_empty() {
        return Err(invalid_params(
            "sourceThreadIds must contain at least one thread".to_string(),
        ));
    }
    if params
        .source_thread_ids
        .iter()
        .any(|thread_id| thread_id.trim().is_empty())
    {
        return Err(invalid_params(
            "sourceThreadIds cannot contain empty thread ids".to_string(),
        ));
    }
    if params.contract_kind.trim().is_empty() {
        return Err(invalid_params("contractKind is required".to_string()));
    }
    if params.instructions.trim().is_empty() {
        return Err(invalid_params("instructions is required".to_string()));
    }
    if params.output_schema.is_none() {
        return Err(invalid_params("outputSchema is required".to_string()));
    }
    if let Some(items_view) = params.items_view.as_deref() {
        match items_view {
            "summary" | "full" | "notLoaded" => {}
            other => {
                return Err(invalid_params(format!(
                    "itemsView must be summary, full or notLoaded; got {other}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_responses_output_schema(
    schema: &serde_json::Value,
) -> Result<(), JSONRPCErrorError> {
    fn find_unsupported_keyword(
        value: &serde_json::Value,
        path: &mut Vec<String>,
    ) -> Option<String> {
        match value {
            serde_json::Value::Object(object) => {
                if object.contains_key("allOf") {
                    return Some(path.join("."));
                }
                for (key, nested) in object {
                    path.push(key.clone());
                    if let Some(found) = find_unsupported_keyword(nested, path) {
                        return Some(found);
                    }
                    path.pop();
                }
                None
            }
            serde_json::Value::Array(values) => {
                values.iter().enumerate().find_map(|(index, nested)| {
                    path.push(index.to_string());
                    let found = find_unsupported_keyword(nested, path);
                    path.pop();
                    found
                })
            }
            _ => None,
        }
    }

    if let Some(path) = find_unsupported_keyword(schema, &mut Vec::new()) {
        return Err(invalid_params(format!(
            "arena composition output schema contains unsupported allOf at {path}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_refs_are_sorted_deduplicated_and_empty_refs_are_removed() {
        assert_eq!(
            compact_event_refs(vec![
                "z".to_string(),
                "".to_string(),
                "a".to_string(),
                "z".to_string(),
                "  ".to_string(),
            ]),
            vec!["a".to_string(), "z".to_string()]
        );
    }

    #[test]
    fn telemetry_summary_is_normalized_and_bounded() {
        let summary = format!("  {}  ", "word ".repeat(100));
        let compacted = compact_summary(summary);

        assert_eq!(
            compacted.chars().count(),
            MEMYTHOS_TELEMETRY_SUMMARY_MAX_CHARS
        );
        assert!(compacted.ends_with('…'));
        assert!(!compacted.contains("  "));
    }
}
