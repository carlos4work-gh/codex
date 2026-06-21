use anyhow::Context;
use clap::Parser;
use serde_json::Value;
use serde_json::json;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;

#[derive(Debug, Parser)]
pub(crate) struct MemythosSniffCommand {
    /// Directory where Memythos trace artifacts will be written.
    #[arg(short = 'o', long = "output-dir", value_name = "DIR")]
    pub(crate) output_dir: Option<PathBuf>,

    /// Do not write the human-readable observation memo.
    #[arg(long = "no-memo", default_value_t = false)]
    pub(crate) no_memo: bool,

    /// Inherit stdin for piped prompts or interactive child input.
    #[arg(long = "inherit-stdin", default_value_t = false)]
    pub(crate) inherit_stdin: bool,

    /// Arguments forwarded to `codex exec --experimental-json`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub(crate) exec_args: Vec<String>,
}

#[derive(Default)]
struct SniffStats {
    event_count: usize,
    command_count: usize,
    mcp_tool_count: usize,
    web_search_count: usize,
    file_change_count: usize,
    reasoning_count: usize,
    error_count: usize,
    final_message: Option<String>,
    thread_id: Option<String>,
}

pub(crate) async fn run_memythos_sniff(cmd: MemythosSniffCommand) -> anyhow::Result<()> {
    let output_dir = cmd.output_dir.unwrap_or_else(default_output_dir);
    fs::create_dir_all(&output_dir)
        .await
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let raw_path = output_dir.join("codex-events.raw.jsonl");
    let stderr_path = output_dir.join("codex-stderr.log");
    let memo_path = output_dir.join("agent-observation-trace.md");
    let summary_path = output_dir.join("run-summary.json");

    let mut raw_file = fs::File::create(&raw_path)
        .await
        .with_context(|| format!("failed to create {}", raw_path.display()))?;
    let mut stderr_file = fs::File::create(&stderr_path)
        .await
        .with_context(|| format!("failed to create {}", stderr_path.display()))?;

    let mut args = vec!["exec".to_string(), "--experimental-json".to_string()];
    args.extend(strip_separator(cmd.exec_args));

    eprintln!("memythos-sniff output: {}", output_dir.display());
    eprintln!("memythos-sniff command: codex {}", args.join(" "));

    let current_exe = std::env::current_exe().context("failed to locate current codex binary")?;
    let stdin = if cmd.inherit_stdin {
        Stdio::inherit()
    } else {
        Stdio::null()
    };
    let mut child = Command::new(current_exe)
        .args(args)
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn codex exec")?;

    let stdout = child.stdout.take().context("child stdout was not piped")?;
    let stderr = child.stderr.take().context("child stderr was not piped")?;

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Some(line) = reader.next_line().await? {
            stderr_file.write_all(line.as_bytes()).await?;
            stderr_file.write_all(b"\n").await?;
            eprintln!("{line}");
        }
        anyhow::Ok(())
    });

    let mut reader = BufReader::new(stdout).lines();
    let mut stats = SniffStats::default();
    let mut observations = Vec::new();
    while let Some(line) = reader.next_line().await? {
        raw_file.write_all(line.as_bytes()).await?;
        raw_file.write_all(b"\n").await?;
        stats.event_count += 1;

        match serde_json::from_str::<Value>(&line) {
            Ok(event) => {
                if let Some(observation) = observe_event(&event, &mut stats) {
                    eprintln!("memythos-sniff: {observation}");
                    observations.push(observation);
                }
            }
            Err(err) => {
                let observation = format!("unparsed jsonl event: {err}");
                eprintln!("memythos-sniff: {observation}");
                observations.push(observation);
            }
        }
    }

    raw_file.flush().await?;
    let status = child
        .wait()
        .await
        .context("failed waiting for codex exec")?;
    stderr_task
        .await
        .context("stderr task failed to join")?
        .context("stderr task failed")?;

    if !cmd.no_memo {
        fs::write(&memo_path, render_memo(&stats, &observations))
            .await
            .with_context(|| format!("failed to write {}", memo_path.display()))?;
    }

    let summary = json!({
        "status": status.code(),
        "success": status.success(),
        "output_dir": output_dir,
        "raw_events": raw_path,
        "stderr_log": stderr_path,
        "observation_memo": if cmd.no_memo { Value::Null } else { json!(memo_path) },
        "thread_id": stats.thread_id,
        "event_count": stats.event_count,
        "command_count": stats.command_count,
        "mcp_tool_count": stats.mcp_tool_count,
        "web_search_count": stats.web_search_count,
        "file_change_count": stats.file_change_count,
        "reasoning_count": stats.reasoning_count,
        "error_count": stats.error_count,
        "final_message": stats.final_message,
    });
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .await
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    println!("{}", serde_json::to_string_pretty(&summary)?);

    if !status.success() {
        anyhow::bail!("codex exec exited with status {status}");
    }
    Ok(())
}

fn strip_separator(args: Vec<String>) -> Vec<String> {
    args.into_iter().filter(|arg| arg != "--").collect()
}

fn default_output_dir() -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    PathBuf::from(".memythos")
        .join("codex-sniff")
        .join(format!("run-{millis}"))
}

fn observe_event(event: &Value, stats: &mut SniffStats) -> Option<String> {
    let event_type = event.get("type")?.as_str().unwrap_or("unknown");
    match event_type {
        "thread.started" => {
            let thread_id = event.get("thread_id")?.as_str()?.to_string();
            stats.thread_id = Some(thread_id.clone());
            Some(format!("thread started: {thread_id}"))
        }
        "turn.started" => Some("turn started".to_string()),
        "turn.completed" => Some(format!(
            "turn completed: {}",
            compact_json(event.get("usage").unwrap_or(&Value::Null))
        )),
        "turn.failed" => {
            stats.error_count += 1;
            Some(format!(
                "turn failed: {}",
                event
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ))
        }
        "error" => {
            stats.error_count += 1;
            Some(format!(
                "stream error: {}",
                event
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ))
        }
        "item.started" | "item.updated" | "item.completed" => {
            observe_item_event(event_type, event, stats)
        }
        _ => Some(format!("{event_type}: {}", compact_json(event))),
    }
}

fn observe_item_event(event_type: &str, event: &Value, stats: &mut SniffStats) -> Option<String> {
    let item = event.get("item")?;
    let item_type = item.get("type")?.as_str().unwrap_or("unknown");
    let phase = event_type.strip_prefix("item.").unwrap_or(event_type);
    match item_type {
        "agent_message" => {
            let text = item.get("text").and_then(Value::as_str).unwrap_or("");
            if event_type == "item.completed" && !text.trim().is_empty() {
                stats.final_message = Some(text.to_string());
            }
            Some(format!("{phase} agent_message: {}", preview(text)))
        }
        "reasoning" => {
            if event_type == "item.completed" {
                stats.reasoning_count += 1;
            }
            Some(format!(
                "{phase} reasoning: {}",
                preview(item.get("text").and_then(Value::as_str).unwrap_or(""))
            ))
        }
        "command_execution" => {
            if event_type == "item.completed" {
                stats.command_count += 1;
            }
            let command = item.get("command").and_then(Value::as_str).unwrap_or("");
            let status = item.get("status").and_then(Value::as_str).unwrap_or("");
            let exit_code = item.get("exit_code").and_then(Value::as_i64);
            let exit_code_suffix = exit_code_suffix(exit_code);
            Some(format!(
                "{phase} command[{status}{exit_code_suffix}]: {}",
                preview(command)
            ))
        }
        "mcp_tool_call" => {
            if event_type == "item.completed" {
                stats.mcp_tool_count += 1;
            }
            let server = item.get("server").and_then(Value::as_str).unwrap_or("");
            let tool = item.get("tool").and_then(Value::as_str).unwrap_or("");
            let status = item.get("status").and_then(Value::as_str).unwrap_or("");
            Some(format!("{phase} mcp_tool[{status}]: {server}.{tool}"))
        }
        "collab_tool_call" => {
            let tool = item.get("tool").and_then(Value::as_str).unwrap_or("");
            let status = item.get("status").and_then(Value::as_str).unwrap_or("");
            Some(format!("{phase} collab_tool[{status}]: {tool}"))
        }
        "web_search" => {
            if event_type == "item.completed" {
                stats.web_search_count += 1;
            }
            let query = item.get("query").and_then(Value::as_str).unwrap_or("");
            Some(format!("{phase} web_search: {}", preview(query)))
        }
        "file_change" => {
            if event_type == "item.completed" {
                stats.file_change_count += 1;
            }
            let status = item.get("status").and_then(Value::as_str).unwrap_or("");
            let changes = item
                .get("changes")
                .and_then(Value::as_array)
                .map(|changes| changes.len())
                .unwrap_or_default();
            Some(format!("{phase} file_change[{status}]: {changes} changes"))
        }
        "todo_list" => {
            let done = item
                .get("items")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            item.get("completed")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                        })
                        .count()
                })
                .unwrap_or_default();
            let total = item
                .get("items")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            Some(format!("{phase} todo_list: {done}/{total} completed"))
        }
        "error" => {
            if event_type == "item.completed" {
                stats.error_count += 1;
            }
            Some(format!(
                "{phase} error: {}",
                item.get("message").and_then(Value::as_str).unwrap_or("")
            ))
        }
        _ => Some(format!("{phase} {item_type}: {}", compact_json(item))),
    }
}

fn render_memo(stats: &SniffStats, observations: &[String]) -> String {
    let mut memo = String::new();
    memo.push_str("# Memythos Codex Sniff\n\n");
    memo.push_str("## Summary\n\n");
    memo.push_str(&format!(
        "- Thread: {}\n",
        stats.thread_id.as_deref().unwrap_or("unknown")
    ));
    memo.push_str(&format!("- Events: {}\n", stats.event_count));
    memo.push_str(&format!("- Commands: {}\n", stats.command_count));
    memo.push_str(&format!("- MCP tools: {}\n", stats.mcp_tool_count));
    memo.push_str(&format!("- Web searches: {}\n", stats.web_search_count));
    memo.push_str(&format!("- File changes: {}\n", stats.file_change_count));
    memo.push_str(&format!(
        "- Reasoning summaries: {}\n",
        stats.reasoning_count
    ));
    memo.push_str(&format!("- Errors: {}\n", stats.error_count));
    memo.push_str("\n## Observation Timeline\n\n");
    for (index, observation) in observations.iter().enumerate() {
        memo.push_str(&format!("{}. {}\n", index + 1, observation));
    }
    if let Some(final_message) = stats.final_message.as_ref() {
        memo.push_str("\n## Final Message\n\n");
        memo.push_str(final_message);
        memo.push('\n');
    }
    memo
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_string())
}

fn preview(value: &str) -> String {
    const MAX_CHARS: usize = 180;
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_CHARS {
        return normalized;
    }
    let mut out = normalized.chars().take(MAX_CHARS).collect::<String>();
    out.push_str("...");
    out
}

fn exit_code_suffix(exit_code: Option<i64>) -> String {
    exit_code
        .map(|code| format!(", exit={code}"))
        .unwrap_or_default()
}
