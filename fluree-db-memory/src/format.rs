use crate::types::{Memory, MemoryStatus, RecallResult, ScoredMemory};

/// Format a memory for human-readable text output.
pub fn format_text(memory: &Memory) -> String {
    let mut out = String::new();
    out.push_str(&format!("ID:      {}\n", memory.id));
    out.push_str(&format!("Kind:    {}\n", memory.kind));
    out.push_str(&format!("Content: {}\n", memory.content));
    if !memory.tags.is_empty() {
        out.push_str(&format!("Tags:    {}\n", memory.tags.join(", ")));
    }
    if !memory.artifact_refs.is_empty() {
        out.push_str(&format!("Refs:    {}\n", memory.artifact_refs.join(", ")));
    }
    if let Some(branch) = &memory.branch {
        out.push_str(&format!("Branch:  {branch}\n"));
    }
    if let Some(rationale) = &memory.rationale {
        out.push_str(&format!("Rationale: {rationale}\n"));
    }
    if let Some(alternatives) = &memory.alternatives {
        out.push_str(&format!("Alternatives: {alternatives}\n"));
    }
    out.push_str(&format!("Created: {}\n", memory.created_at));
    out
}

/// Format a recall result for human-readable text output.
pub fn format_recall_text(result: &RecallResult) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Recall: \"{}\" ({} matches)\n\n",
        result.query,
        result.memories.len()
    ));

    for (i, scored) in result.memories.iter().enumerate() {
        out.push_str(&format!(
            "{}. [score: {:.1}] {}\n   {}\n",
            i + 1,
            scored.score,
            scored.memory.id,
            scored.memory.content,
        ));
        if !scored.memory.tags.is_empty() {
            out.push_str(&format!("   Tags: {}\n", scored.memory.tags.join(", ")));
        }
        out.push('\n');
    }

    out
}

/// Format a memory as JSON.
pub fn format_json(memory: &Memory) -> serde_json::Value {
    serde_json::to_value(memory).unwrap_or(serde_json::Value::Null)
}

/// Format a recall result as JSON.
pub fn format_recall_json(result: &RecallResult) -> serde_json::Value {
    serde_json::to_value(result).unwrap_or(serde_json::Value::Null)
}

/// Format memories as an XML context block for LLM injection.
///
/// This is the format that agents receive when they call the `memory_recall` MCP tool.
pub fn format_context(memories: &[ScoredMemory]) -> String {
    format_context_paged(memories, 0, None, memories.len(), false, None)
}

/// Format memories as an XML context block with pagination metadata.
///
/// `offset` and `limit` describe the current page.  When `limit` is `None`
/// (caller used the default), it is omitted from the pagination tag to avoid
/// confusing the LLM — the smart score-based filter may return fewer results
/// than the internal default, and showing a limit the caller never asked for
/// looks like a mismatch.
/// `total_store` is the total number of current memories in the store.
/// `has_more` indicates that additional results may be available at `offset + shown`.
/// `next_score` is the BM25 score of the first result not returned, giving the LLM a
/// signal for whether it's worth requesting the next page.
pub fn format_context_paged(
    memories: &[ScoredMemory],
    offset: usize,
    limit: Option<usize>,
    total_store: usize,
    has_more: bool,
    next_score: Option<f64>,
) -> String {
    let mut out = String::new();
    out.push_str("<memory-context>\n");

    for scored in memories {
        let mem = &scored.memory;
        out.push_str(&format!(
            "  <memory id=\"{}\" kind=\"{}\" score=\"{:.1}\">\n",
            mem.id, mem.kind, scored.score
        ));
        out.push_str(&format!(
            "    <content>{}</content>\n",
            xml_escape(&mem.content)
        ));
        if !mem.tags.is_empty() {
            out.push_str(&format!("    <tags>{}</tags>\n", mem.tags.join(", ")));
        }
        if !mem.artifact_refs.is_empty() {
            out.push_str(&format!(
                "    <refs>{}</refs>\n",
                mem.artifact_refs.join(", ")
            ));
        }
        if let Some(severity) = &mem.severity {
            out.push_str(&format!("    <severity>{severity:?}</severity>\n"));
        }
        if let Some(rationale) = &mem.rationale {
            out.push_str(&format!(
                "    <rationale>{}</rationale>\n",
                xml_escape(rationale)
            ));
        }
        if let Some(alternatives) = &mem.alternatives {
            out.push_str(&format!(
                "    <alternatives>{}</alternatives>\n",
                xml_escape(alternatives)
            ));
        }
        out.push_str("  </memory>\n");
    }

    let shown = memories.len();
    let limit_attr = limit.map(|l| format!(" limit=\"{l}\"")).unwrap_or_default();
    if has_more {
        let next_offset = offset + shown;
        let next_score_hint = next_score
            .map(|s| format!(", next score: {s:.1}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "  <pagination shown=\"{}\" offset=\"{}\"{} total_in_store=\"{}\">\
             Results {}\u{2013}{}{next_score_hint}. Use offset={} to retrieve more.</pagination>\n",
            shown,
            offset,
            limit_attr,
            total_store,
            offset + 1,
            offset + shown,
            next_offset,
        ));
    } else {
        out.push_str(&format!(
            "  <pagination shown=\"{shown}\" offset=\"{offset}\" total_in_store=\"{total_store}\" />\n",
        ));
    }

    out.push_str("</memory-context>");
    out
}

/// Format memory status for human-readable output.
///
/// Includes counts by kind and previews of recent memories so that
/// the LLM can see what topics are stored and use better keywords.
pub fn format_status_text(status: &MemoryStatus) -> String {
    let mut out = String::new();

    if !status.initialized {
        return "Memory store is empty. Use memory_add to store project knowledge.".to_string();
    }

    if status.total_memories == 0 {
        return "Memory store is empty. Use memory_add to store project knowledge.".to_string();
    }

    if let Some(dir) = &status.memory_dir {
        out.push_str(&format!("  Directory: {dir}\n"));
    }

    out.push_str(&format!(
        "Memory Store: {} memories, {} tags\n",
        status.total_memories, status.total_tags
    ));

    if !status.by_kind.is_empty() {
        let kinds: Vec<String> = status
            .by_kind
            .iter()
            .map(|(kind, count)| format!("{count} {kind}"))
            .collect();
        out.push_str(&format!("  Kinds: {}\n", kinds.join(", ")));
    }

    if !status.recent.is_empty() {
        out.push_str("\nRecent memories:\n");
        for preview in &status.recent {
            let tags_str = if preview.tags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", preview.tags.join(", "))
            };
            out.push_str(&format!(
                "  - [{}] {}{}\n    ID: {}\n",
                preview.kind, preview.summary, tags_str, preview.id
            ));
        }
        out.push_str("\nUse memory_recall with specific keywords from above to search.");
    }

    out
}

/// Format related memories as an XML block for post-add housekeeping.
///
/// Shown after a successful `memory_add` to surface existing memories that
/// may overlap with the one just stored. Gives the LLM concrete tool calls
/// to clean up duplicates or stale entries.
pub fn format_related_memories(related: &[ScoredMemory]) -> String {
    let mut out = String::new();
    out.push_str(
        "\n\nExisting memories listed below may overlap with what you just stored. \
         Help clean up stale memories!\n\
         - Superseded or redundant: memory_forget(id=\"<old-id>\")\n\
         - Could be improved: memory_update(id=\"<old-id>\") with only the fields to change (content, tags, or refs)\n\n",
    );
    out.push_str("<related-memories>\n");

    for scored in related {
        let mem = &scored.memory;
        let age = format_age(&mem.created_at);
        out.push_str(&format!(
            "  <memory id=\"{}\" kind=\"{}\" score=\"{:.1}\" age=\"{}\">\n",
            mem.id, mem.kind, scored.score, age
        ));
        let preview: String = mem.content.chars().take(120).collect();
        let ellipsis = if mem.content.len() > preview.len() {
            "..."
        } else {
            ""
        };
        out.push_str(&format!(
            "    <content>{}{}</content>\n",
            xml_escape(&preview),
            ellipsis
        ));
        if !mem.tags.is_empty() {
            out.push_str(&format!("    <tags>{}</tags>\n", mem.tags.join(", ")));
        }
        out.push_str("  </memory>\n");
    }

    out.push_str("</related-memories>");

    out
}

/// Format a created_at timestamp as a human-readable age string.
fn format_age(created_at: &str) -> String {
    use chrono::{DateTime, Utc};
    let Ok(created) = DateTime::parse_from_rfc3339(created_at) else {
        return "?".to_string();
    };
    let age = Utc::now() - created.to_utc();
    let days = age.num_days();
    if days == 0 {
        let hours = age.num_hours();
        if hours == 0 {
            format!("{}m", age.num_minutes().max(1))
        } else {
            format!("{hours}h")
        }
    } else if days < 30 {
        format!("{days}d")
    } else {
        format!("{}w", days / 7)
    }
}

/// Minimal XML escaping for content text.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryKind, Scope};

    fn sample_memory() -> Memory {
        Memory {
            id: "mem:fact-test123".to_string(),
            kind: MemoryKind::Fact,
            content: "Use nextest for running tests".to_string(),
            tags: vec!["testing".to_string(), "cargo".to_string()],
            scope: Scope::Repo,
            severity: None,
            artifact_refs: vec!["Cargo.toml".to_string()],
            branch: Some("main".to_string()),
            created_at: "2026-02-21T12:00:00Z".to_string(),
            rationale: None,
            alternatives: None,
        }
    }

    #[test]
    fn text_format_includes_all_fields() {
        let text = format_text(&sample_memory());
        assert!(text.contains("mem:fact-test123"));
        assert!(text.contains("nextest"));
        assert!(text.contains("testing, cargo"));
        assert!(text.contains("Cargo.toml"));
        assert!(text.contains("main"));
    }

    #[test]
    fn context_format_is_xml() {
        let scored = vec![ScoredMemory {
            memory: sample_memory(),
            score: 15.0,
        }];
        let ctx = format_context(&scored);
        assert!(ctx.starts_with("<memory-context>"));
        assert!(ctx.ends_with("</memory-context>"));
        assert!(ctx.contains("score=\"15.0\""));
    }

    #[test]
    fn xml_escape_works() {
        assert_eq!(xml_escape("a < b & c > d"), "a &lt; b &amp; c &gt; d");
    }

    #[test]
    fn format_age_minutes() {
        use chrono::Utc;
        let ts = (Utc::now() - chrono::Duration::minutes(15)).to_rfc3339();
        let age = format_age(&ts);
        assert!(age.ends_with('m'), "expected minutes, got: {age}");
    }

    #[test]
    fn format_age_hours() {
        use chrono::Utc;
        let ts = (Utc::now() - chrono::Duration::hours(5)).to_rfc3339();
        let age = format_age(&ts);
        assert_eq!(age, "5h");
    }

    #[test]
    fn format_age_days() {
        use chrono::Utc;
        let ts = (Utc::now() - chrono::Duration::days(12)).to_rfc3339();
        let age = format_age(&ts);
        assert_eq!(age, "12d");
    }

    #[test]
    fn format_age_weeks() {
        use chrono::Utc;
        let ts = (Utc::now() - chrono::Duration::days(45)).to_rfc3339();
        let age = format_age(&ts);
        assert_eq!(age, "6w");
    }

    #[test]
    fn format_age_invalid_timestamp() {
        assert_eq!(format_age("not-a-date"), "?");
    }

    #[test]
    fn related_memories_format_structure() {
        let scored = vec![ScoredMemory {
            memory: Memory {
                id: "mem:fact-old".to_string(),
                kind: MemoryKind::Fact,
                content: "PSOT queries return supersets".to_string(),
                tags: vec!["query".to_string(), "index".to_string()],
                scope: Scope::Repo,
                severity: None,
                artifact_refs: vec![],
                branch: None,
                created_at: "2026-03-01T10:00:00Z".to_string(),
                rationale: None,
                alternatives: None,
            },
            score: 18.2,
        }];

        let output = format_related_memories(&scored);

        assert!(output.contains("<related-memories>"));
        assert!(output.contains("</related-memories>"));
        assert!(output.contains("mem:fact-old"));
        assert!(output.contains("score=\"18.2\""));
        assert!(output.contains("PSOT queries return supersets"));
        assert!(output.contains("<tags>query, index</tags>"));
        assert!(output.contains("memory_forget"));
        assert!(output.contains("memory_update"));
    }

    #[test]
    fn related_memories_truncates_long_content() {
        let long_content = "x".repeat(200);
        let scored = vec![ScoredMemory {
            memory: Memory {
                id: "mem:fact-long".to_string(),
                kind: MemoryKind::Fact,
                content: long_content,
                tags: vec![],
                scope: Scope::Repo,
                severity: None,
                artifact_refs: vec![],
                branch: None,
                created_at: "2026-03-01T10:00:00Z".to_string(),
                rationale: None,
                alternatives: None,
            },
            score: 12.0,
        }];

        let output = format_related_memories(&scored);
        assert!(output.contains("..."));
        // Content in the XML should be truncated to ~120 chars + "..."
        assert!(!output.contains(&"x".repeat(200)));
    }

    #[test]
    fn related_memories_empty_returns_empty() {
        let output = format_related_memories(&[]);
        // With no memories, the preamble text and XML are still produced
        // but there are no <memory> elements inside
        assert!(output.contains("<related-memories>"));
        assert!(!output.contains("<memory "));
    }
}
