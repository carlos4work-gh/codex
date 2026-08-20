use std::time::Duration;

use chrono::Utc;
use codex_app_server_protocol::MemythosTokenUsageBreakdown;
use codex_app_server_protocol::TokenUsageBreakdown;

pub(super) fn native_token_usage_key(thread_id: &str, turn_id: &str) -> String {
    format!("{thread_id}::{turn_id}")
}

pub(super) fn memythos_usage_breakdown(
    usage: &TokenUsageBreakdown,
) -> MemythosTokenUsageBreakdown {
    MemythosTokenUsageBreakdown {
        total_tokens: usage.total_tokens,
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        non_cached_input_tokens: (usage.input_tokens - usage.cached_input_tokens).max(0),
        output_tokens: usage.output_tokens,
        reasoning_output_tokens: usage.reasoning_output_tokens,
    }
}

pub(super) fn subtract_memythos_usage(
    current: &MemythosTokenUsageBreakdown,
    previous: &MemythosTokenUsageBreakdown,
) -> MemythosTokenUsageBreakdown {
    MemythosTokenUsageBreakdown {
        total_tokens: (current.total_tokens - previous.total_tokens).max(0),
        input_tokens: (current.input_tokens - previous.input_tokens).max(0),
        cached_input_tokens: (current.cached_input_tokens - previous.cached_input_tokens).max(0),
        non_cached_input_tokens: (current.non_cached_input_tokens
            - previous.non_cached_input_tokens)
            .max(0),
        output_tokens: (current.output_tokens - previous.output_tokens).max(0),
        reasoning_output_tokens: (current.reasoning_output_tokens
            - previous.reasoning_output_tokens)
            .max(0),
    }
}

pub(super) fn add_memythos_usage(
    total: &mut MemythosTokenUsageBreakdown,
    delta: &MemythosTokenUsageBreakdown,
) {
    total.total_tokens += delta.total_tokens;
    total.input_tokens += delta.input_tokens;
    total.cached_input_tokens += delta.cached_input_tokens;
    total.non_cached_input_tokens += delta.non_cached_input_tokens;
    total.output_tokens += delta.output_tokens;
    total.reasoning_output_tokens += delta.reasoning_output_tokens;
}

pub(super) fn sum_memythos_usage<'a>(
    usage: impl Iterator<Item = &'a MemythosTokenUsageBreakdown>,
) -> MemythosTokenUsageBreakdown {
    usage.fold(MemythosTokenUsageBreakdown::default(), |mut total, item| {
        add_memythos_usage(&mut total, item);
        total
    })
}

pub(super) fn record_mailbox_resolution_metrics(
    action: &str,
    outcome: &str,
    live_reenqueue_status: &str,
    duration: Duration,
) {
    let Some(metrics) = codex_otel::global() else {
        return;
    };
    let tags = [
        ("action", action),
        ("outcome", outcome),
        ("live_reenqueue_status", live_reenqueue_status),
    ];
    let _ = metrics.counter("codex.native_mailbox.resolution", 1, &tags);
    let _ = metrics.record_duration(
        "codex.native_mailbox.resolution.duration_ms",
        duration,
        &tags,
    );
}

pub(super) fn record_mailbox_health_metrics(snapshot: &codex_state::NativeMailboxHealthSnapshot) {
    let Some(metrics) = codex_otel::global() else {
        return;
    };
    for (status, value) in [
        ("pending", snapshot.pending_count),
        ("quarantined", snapshot.quarantined_count),
        ("consumed", snapshot.consumed_count),
        ("skipped", snapshot.skipped_count),
        ("aborted", snapshot.aborted_count),
    ] {
        let _ = metrics.gauge("codex.native_mailbox.records", value, &[("status", status)]);
    }
    let _ = metrics.gauge(
        "codex.native_mailbox.max_attempt_count",
        snapshot.max_attempt_count,
        &[],
    );
    let now_ms = Utc::now().timestamp_millis();
    for (status, updated_at_ms) in [
        ("pending", snapshot.oldest_pending_updated_at_ms),
        ("quarantined", snapshot.oldest_quarantined_updated_at_ms),
    ] {
        let age_ms = updated_at_ms.map_or(0, |timestamp| now_ms.saturating_sub(timestamp));
        let _ = metrics.gauge(
            "codex.native_mailbox.oldest_age_ms",
            age_ms,
            &[("status", status)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(total: i64, input: i64, cached: i64, output: i64) -> MemythosTokenUsageBreakdown {
        MemythosTokenUsageBreakdown {
            total_tokens: total,
            input_tokens: input,
            cached_input_tokens: cached,
            non_cached_input_tokens: input - cached,
            output_tokens: output,
            reasoning_output_tokens: 0,
        }
    }

    #[test]
    fn usage_delta_saturates_each_counter_independently() {
        let delta = subtract_memythos_usage(&usage(8, 5, 2, 3), &usage(10, 4, 3, 6));

        assert_eq!(delta.total_tokens, 0);
        assert_eq!(delta.input_tokens, 1);
        assert_eq!(delta.cached_input_tokens, 0);
        assert_eq!(delta.non_cached_input_tokens, 2);
        assert_eq!(delta.output_tokens, 0);
    }

    #[test]
    fn usage_sum_preserves_all_attribution_counters() {
        let items = [usage(10, 6, 2, 4), usage(7, 5, 1, 2)];

        assert_eq!(sum_memythos_usage(items.iter()), usage(17, 11, 3, 6));
    }
}
