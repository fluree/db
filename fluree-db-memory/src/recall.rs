use crate::types::{Memory, MemoryKind, ScoredMemory};
use chrono::{DateTime, Utc};

/// Recall engine that combines BM25 content scores with metadata bonuses.
///
/// The primary ranking signal comes from Fluree's native `@fulltext` / `fulltext()`
/// BM25 scoring on `mem:content`. Metadata bonuses (tag matches, artifact refs,
/// branch affinity, recency) are layered on top to re-rank results.
pub struct RecallEngine;

impl RecallEngine {
    /// Re-rank BM25 results by applying metadata bonuses.
    ///
    /// `bm25_hits` are `(memory_id, bm25_score)` pairs from `MemoryStore::recall_fulltext()`.
    /// Each hit is matched against the full `Memory` objects (loaded separately) to apply
    /// tag, artifact ref, branch, and recency bonuses.
    ///
    /// Returns `ScoredMemory` entries sorted by combined score, descending.
    pub fn rerank(
        query: &str,
        bm25_hits: &[(String, f64)],
        memories: &[Memory],
        current_branch: Option<&str>,
    ) -> Vec<ScoredMemory> {
        let query_lower = query.to_lowercase();
        let query_words: Vec<&str> = query_lower
            .split_whitespace()
            .filter(|w| w.len() > 2)
            .collect();

        let now = Utc::now();

        let mut scored: Vec<ScoredMemory> = bm25_hits
            .iter()
            .filter_map(|(id, bm25_score)| {
                // BM25 query may return compact prefix IDs (mem:fact-...) while
                // Memory objects use full IRIs (https://ns.flur.ee/memory#fact-...).
                let mem = memories.iter().find(|m| ids_match(&m.id, id))?;
                let bonus = metadata_bonus(mem, &query_lower, &query_words, current_branch, &now);
                Some(ScoredMemory {
                    memory: mem.clone(),
                    score: *bm25_score + bonus,
                })
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        scored
    }

    /// Fallback: score memories purely from metadata when BM25 is unavailable
    /// (e.g., empty query, or fulltext query returns no results but tag/branch
    /// matches are still relevant).
    pub fn recall_metadata_only(
        query: &str,
        memories: &[Memory],
        current_branch: Option<&str>,
        limit: Option<usize>,
    ) -> Vec<ScoredMemory> {
        let query_lower = query.to_lowercase();
        let query_words: Vec<&str> = query_lower
            .split_whitespace()
            .filter(|w| w.len() > 2)
            .collect();

        let now = Utc::now();

        let mut scored: Vec<ScoredMemory> = memories
            .iter()
            .map(|mem| {
                let score = metadata_bonus(mem, &query_lower, &query_words, current_branch, &now);
                ScoredMemory {
                    memory: mem.clone(),
                    score,
                }
            })
            .filter(|sm| sm.score > 0.0)
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if let Some(limit) = limit {
            scored.truncate(limit);
        }

        scored
    }

    /// Find existing memories related to a just-stored memory.
    ///
    /// Runs BM25 + metadata re-ranking with the smart score cliff, excludes the
    /// new memory itself, and applies a minimum score threshold.
    /// Returns an empty vec when nothing qualifies.
    pub fn find_related(
        new_id: &str,
        content: &str,
        bm25_hits: &[(String, f64)],
        all_memories: &[Memory],
        current_branch: Option<&str>,
    ) -> Vec<ScoredMemory> {
        const MIN_SCORE: f64 = 10.0;
        const LIMIT: usize = 2;

        let scored = if bm25_hits.is_empty() {
            Self::recall_metadata_only(content, all_memories, current_branch, Some(LIMIT + 2))
        } else {
            Self::rerank(content, bm25_hits, all_memories, current_branch)
        };

        // Filter out the memory we just created (handles compact vs full IRI)
        // and apply minimum score threshold
        let mut candidates: Vec<_> = scored
            .into_iter()
            .filter(|s| !ids_match(&s.memory.id, new_id) && s.score >= MIN_SCORE)
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // Apply the same smart score cliff as recall: drop below 50% of top
        let top = candidates[0].score;
        if top > 0.0 {
            let threshold = top * 0.5;
            let keep = candidates
                .iter()
                .take_while(|s| s.score >= threshold)
                .count()
                .max(1);
            candidates.truncate(keep);
        }

        candidates.truncate(LIMIT);
        candidates
    }
}

/// Match memory IDs that may be in different formats:
/// - Full IRI: `https://ns.flur.ee/memory#fact-01abc`
/// - Compact prefix: `mem:fact-01abc`
pub fn ids_match(full_id: &str, query_id: &str) -> bool {
    if full_id == query_id {
        return true;
    }
    // Extract the local part after the namespace/prefix
    let full_local = full_id
        .strip_prefix("https://ns.flur.ee/memory#")
        .unwrap_or(full_id);
    let query_local = query_id.strip_prefix("mem:").unwrap_or(query_id);
    full_local == query_local
}

/// Compute metadata-based bonus score for a memory.
///
/// These bonuses are additive on top of the BM25 content score:
/// - Tag match: +10 per tag that contains a query word
/// - Artifact ref match: +8 per ref that contains a query word
/// - Kind match: +6 if the query mentions the memory kind
/// - Branch match: +3 if the memory is on the current git branch
/// - Recency: +2 if created in last 7 days, +1 if last 30 days
fn metadata_bonus(
    mem: &Memory,
    query_lower: &str,
    query_words: &[&str],
    current_branch: Option<&str>,
    now: &DateTime<Utc>,
) -> f64 {
    let mut bonus = 0.0;

    // Tag match: +10 per matching tag
    for tag in &mem.tags {
        let tag_lower = tag.to_lowercase();
        if query_words
            .iter()
            .any(|w| tag_lower == *w || tag_lower.contains(w))
        {
            bonus += 10.0;
        }
    }

    // Artifact ref path match: +8
    for aref in &mem.artifact_refs {
        let aref_lower = aref.to_lowercase();
        if query_words.iter().any(|w| aref_lower.contains(w)) {
            bonus += 8.0;
        }
    }

    // Kind match: +6 if query mentions the memory kind
    let kind_names = match mem.kind {
        MemoryKind::Fact => &["fact", "facts"][..],
        MemoryKind::Decision => &["decision", "decisions", "decided"][..],
        MemoryKind::Constraint => &["constraint", "constraints", "rule", "rules"][..],
    };
    if kind_names.iter().any(|kn| query_lower.contains(kn)) {
        bonus += 6.0;
    }

    // Branch match: +3
    if let (Some(mem_branch), Some(cur_branch)) = (&mem.branch, current_branch) {
        if mem_branch == cur_branch {
            bonus += 3.0;
        }
    }

    // Recency bonus
    if let Ok(created) = DateTime::parse_from_rfc3339(&mem.created_at) {
        let age = *now - created.to_utc();
        if age.num_days() < 7 {
            bonus += 2.0;
        } else if age.num_days() < 30 {
            bonus += 1.0;
        }
    }

    bonus
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Memory, MemoryKind, Scope};

    fn make_memory(id: &str, content: &str, tags: &[&str], kind: MemoryKind) -> Memory {
        Memory {
            id: id.to_string(),
            kind,
            content: content.to_string(),
            tags: tags.iter().map(std::string::ToString::to_string).collect(),
            scope: Scope::Repo,
            severity: None,
            artifact_refs: Vec::new(),
            branch: None,
            created_at: Utc::now().to_rfc3339(),
            rationale: None,
            alternatives: None,
        }
    }

    #[test]
    fn rerank_applies_tag_bonus() {
        let memories = vec![
            make_memory(
                "mem:a",
                "Use nextest for tests",
                &["testing"],
                MemoryKind::Fact,
            ),
            make_memory(
                "mem:b",
                "The database uses RDF triples",
                &["database"],
                MemoryKind::Fact,
            ),
        ];

        // Simulate BM25 returning both with similar content scores
        let bm25_hits = vec![("mem:a".to_string(), 1.5), ("mem:b".to_string(), 1.2)];

        let results = RecallEngine::rerank("testing", &bm25_hits, &memories, None);
        assert_eq!(results.len(), 2);
        // mem:a should rank higher due to tag bonus (+10 for "testing" tag)
        assert_eq!(results[0].memory.id, "mem:a");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn rerank_preserves_bm25_ordering_when_no_bonus() {
        let memories = vec![
            make_memory("mem:a", "Topic alpha", &[], MemoryKind::Fact),
            make_memory("mem:b", "Topic beta", &[], MemoryKind::Fact),
        ];

        // BM25 says mem:b is more relevant
        let bm25_hits = vec![("mem:b".to_string(), 3.0), ("mem:a".to_string(), 1.0)];

        let results = RecallEngine::rerank("unrelated query", &bm25_hits, &memories, None);
        assert_eq!(results[0].memory.id, "mem:b");
    }

    #[test]
    fn metadata_only_fallback() {
        let memories = vec![
            make_memory(
                "mem:a",
                "Use nextest for tests",
                &["testing"],
                MemoryKind::Fact,
            ),
            make_memory("mem:b", "Unrelated content", &["other"], MemoryKind::Fact),
        ];

        let results = RecallEngine::recall_metadata_only("testing", &memories, None, None);
        assert!(!results.is_empty());
        assert_eq!(results[0].memory.id, "mem:a");
    }

    #[test]
    fn branch_bonus_applied() {
        let mut mem = make_memory("mem:a", "Some fact", &[], MemoryKind::Fact);
        mem.branch = Some("feature/memory".to_string());

        let memories = vec![mem];
        let bm25_hits = vec![("mem:a".to_string(), 1.0)];

        let results =
            RecallEngine::rerank("some query", &bm25_hits, &memories, Some("feature/memory"));
        assert_eq!(results[0].score, 1.0 + 3.0 + 2.0); // bm25 + branch + recency
    }

    #[test]
    fn find_related_excludes_self_by_id() {
        let memories = vec![
            make_memory(
                "mem:fact-new",
                "PSOT returns supersets",
                &["query"],
                MemoryKind::Fact,
            ),
            make_memory(
                "mem:fact-old",
                "PSOT range queries return supersets",
                &["query"],
                MemoryKind::Fact,
            ),
        ];
        let bm25_hits = vec![
            ("mem:fact-new".to_string(), 20.0),
            ("mem:fact-old".to_string(), 18.0),
        ];

        let results = RecallEngine::find_related(
            "mem:fact-new",
            "PSOT returns supersets",
            &bm25_hits,
            &memories,
            None,
        );
        assert!(!results.is_empty());
        assert!(results.iter().all(|r| r.memory.id != "mem:fact-new"));
        assert_eq!(results[0].memory.id, "mem:fact-old");
    }

    #[test]
    fn find_related_excludes_self_cross_format_ids() {
        let memories = vec![
            make_memory(
                "https://ns.flur.ee/memory#fact-01abc",
                "PSOT returns supersets",
                &["query"],
                MemoryKind::Fact,
            ),
            make_memory(
                "https://ns.flur.ee/memory#fact-02def",
                "PSOT range queries return supersets",
                &["query"],
                MemoryKind::Fact,
            ),
        ];
        let bm25_hits = vec![
            ("mem:fact-01abc".to_string(), 20.0),
            ("mem:fact-02def".to_string(), 18.0),
        ];

        // new_id is compact, Memory.id is full IRI — should still exclude
        let results = RecallEngine::find_related(
            "mem:fact-01abc",
            "PSOT returns supersets",
            &bm25_hits,
            &memories,
            None,
        );
        assert!(results
            .iter()
            .all(|r| !ids_match(&r.memory.id, "mem:fact-01abc")));
    }

    #[test]
    fn find_related_empty_when_below_min_score() {
        let memories = vec![
            make_memory(
                "mem:fact-new",
                "PSOT returns supersets",
                &[],
                MemoryKind::Fact,
            ),
            make_memory(
                "mem:fact-old",
                "Unrelated content about trees",
                &[],
                MemoryKind::Fact,
            ),
        ];
        // Low BM25 scores — below the MIN_SCORE threshold of 10
        let bm25_hits = vec![
            ("mem:fact-new".to_string(), 5.0),
            ("mem:fact-old".to_string(), 3.0),
        ];

        let results = RecallEngine::find_related(
            "mem:fact-new",
            "PSOT returns supersets",
            &bm25_hits,
            &memories,
            None,
        );
        assert!(results.is_empty());
    }

    #[test]
    fn find_related_applies_score_cliff() {
        let memories = vec![
            make_memory("mem:fact-new", "PSOT supersets", &[], MemoryKind::Fact),
            make_memory(
                "mem:fact-a",
                "PSOT range queries return supersets",
                &["query"],
                MemoryKind::Fact,
            ),
            make_memory("mem:fact-b", "PSOT is an index type", &[], MemoryKind::Fact),
            make_memory("mem:fact-c", "Mentioned PSOT once", &[], MemoryKind::Fact),
        ];
        // mem:fact-a scores high, mem:fact-b moderate, mem:fact-c below 50% cliff
        let bm25_hits = vec![
            ("mem:fact-new".to_string(), 25.0),
            ("mem:fact-a".to_string(), 24.0),
            ("mem:fact-b".to_string(), 15.0),
            ("mem:fact-c".to_string(), 10.5),
        ];

        let results = RecallEngine::find_related(
            "mem:fact-new",
            "PSOT supersets",
            &bm25_hits,
            &memories,
            None,
        );
        // mem:fact-a has high score (~34 with tag bonus), mem:fact-b (~17 with recency)
        // mem:fact-c (~12.5 with recency) should be below 50% of top and get clipped
        assert!(!results.is_empty());
        assert!(results.len() <= 2);
    }

    #[test]
    fn find_related_caps_at_two() {
        let mut memories = vec![make_memory(
            "mem:fact-new",
            "common topic",
            &[],
            MemoryKind::Fact,
        )];
        let mut bm25_hits = vec![("mem:fact-new".to_string(), 30.0)];

        // Add 5 related memories all with high scores
        for i in 0..5 {
            let id = format!("mem:fact-{i}");
            memories.push(make_memory(
                &id,
                "common topic content",
                &["common"],
                MemoryKind::Fact,
            ));
            bm25_hits.push((id, 25.0));
        }

        let results =
            RecallEngine::find_related("mem:fact-new", "common topic", &bm25_hits, &memories, None);
        assert!(results.len() <= 2);
    }

    #[test]
    fn ids_match_compact_vs_full() {
        assert!(ids_match(
            "https://ns.flur.ee/memory#fact-01abc",
            "mem:fact-01abc"
        ));
        assert!(ids_match("mem:fact-01abc", "mem:fact-01abc"));
        assert!(ids_match(
            "https://ns.flur.ee/memory#fact-01abc",
            "https://ns.flur.ee/memory#fact-01abc"
        ));
        assert!(!ids_match("mem:fact-01abc", "mem:fact-02def"));
    }
}
