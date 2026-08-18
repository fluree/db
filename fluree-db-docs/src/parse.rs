//! Markdown → [`Section`]s using `pulldown-cmark` (the same CommonMark parser
//! mdBook uses), plus mdBook-compatible heading anchors.
//!
//! This is the only module that knows about the markdown parser; everything
//! downstream works on the parser-agnostic [`Section`] model.

use crate::model::{CodeBlock, Section, TreeNode};
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use std::collections::HashMap;

/// A heading discovered in a page, with the byte offset of its `#` marker so we
/// can slice the original markdown for each section's body.
struct Marker {
    level: u8,
    title: String,
    anchor: String,
    byte_start: usize,
}

/// A fenced code block with the byte offset of its opening fence, used to assign
/// it to the section whose byte range contains it.
struct RawCode {
    byte_start: usize,
    lang: String,
    code: String,
}

/// Parse one page's markdown into heading-scoped sections.
pub fn parse_sections(path: &str, md: &str) -> Vec<Section> {
    let mut markers: Vec<Marker> = Vec::new();
    let mut codes: Vec<RawCode> = Vec::new();
    let mut id_counter: HashMap<String, usize> = HashMap::new();

    // Heading accumulation state.
    let mut in_heading = false;
    let mut heading_level = 0u8;
    let mut heading_text = String::new();
    let mut heading_start = 0usize;

    // Code-block accumulation state.
    let mut in_code = false;
    let mut code_lang = String::new();
    let mut code_buf = String::new();
    let mut code_start = 0usize;

    for (event, range) in Parser::new_ext(md, Options::all()).into_offset_iter() {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                in_heading = true;
                heading_level = level as u8;
                heading_text.clear();
                heading_start = range.start;
            }
            Event::End(TagEnd::Heading(_)) => {
                in_heading = false;
                let anchor = unique_anchor(&heading_text, &mut id_counter);
                markers.push(Marker {
                    level: heading_level,
                    title: heading_text.trim().to_string(),
                    anchor,
                    byte_start: heading_start,
                });
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                in_code = true;
                code_start = range.start;
                code_buf.clear();
                code_lang = match kind {
                    CodeBlockKind::Fenced(info) => {
                        info.split_whitespace().next().unwrap_or("").to_string()
                    }
                    CodeBlockKind::Indented => String::new(),
                };
            }
            Event::End(TagEnd::CodeBlock) => {
                in_code = false;
                codes.push(RawCode {
                    byte_start: code_start,
                    lang: code_lang.clone(),
                    code: code_buf.clone(),
                });
            }
            // Heading titles accumulate plain + inline-code text. Code blocks
            // accumulate their literal text. Everything else is ignored here —
            // section bodies are sliced from the original markdown below.
            Event::Text(t) => {
                if in_heading {
                    heading_text.push_str(&t);
                } else if in_code {
                    code_buf.push_str(&t);
                }
            }
            Event::Code(t) if in_heading => {
                heading_text.push_str(&t);
            }
            _ => {}
        }
    }

    build_sections(path, md, &markers, &codes)
}

/// Slice the original markdown into one section per heading, plus a leading
/// preamble section if the page has content before its first heading.
fn build_sections(path: &str, md: &str, markers: &[Marker], codes: &[RawCode]) -> Vec<Section> {
    let mut sections = Vec::new();

    // Preamble: bytes before the first heading (or the whole page if none).
    let first_start = markers.first().map_or(md.len(), |m| m.byte_start);
    let preamble = &md[..first_start];
    if !preamble.trim().is_empty() {
        sections.push(Section {
            path: path.to_string(),
            anchor: String::new(),
            title: String::new(),
            heading_path: Vec::new(),
            level: 0,
            body: preamble.to_string(),
            code_blocks: codes_in(codes, 0, first_start),
        });
    }

    let mut stack: Vec<(u8, String)> = Vec::new();
    for (i, m) in markers.iter().enumerate() {
        let end = markers.get(i + 1).map_or(md.len(), |n| n.byte_start);

        // Maintain the ancestor-heading breadcrumb.
        while stack.last().is_some_and(|(lvl, _)| *lvl >= m.level) {
            stack.pop();
        }
        stack.push((m.level, m.title.clone()));
        let heading_path: Vec<String> = stack.iter().map(|(_, t)| t.clone()).collect();

        sections.push(Section {
            path: path.to_string(),
            anchor: m.anchor.clone(),
            title: m.title.clone(),
            heading_path,
            level: m.level,
            body: md[m.byte_start..end].to_string(),
            code_blocks: codes_in(codes, m.byte_start, end),
        });
    }

    sections
}

/// Code blocks whose opening fence falls within `[start, end)`.
fn codes_in(codes: &[RawCode], start: usize, end: usize) -> Vec<CodeBlock> {
    codes
        .iter()
        .filter(|c| c.byte_start >= start && c.byte_start < end)
        .map(|c| CodeBlock {
            lang: c.lang.clone(),
            code: c.code.clone(),
        })
        .collect()
}

/// mdBook's heading-id rule: lowercase; whitespace → `-`; drop anything that
/// isn't alphanumeric/`-`/`_`. Duplicate ids within a page get `-1`, `-2`, …
/// (matching mdBook's `unique_id_from_content`), so anchors line up with the
/// published site.
fn unique_anchor(content: &str, counter: &mut HashMap<String, usize>) -> String {
    let id = normalize_id(content);
    let count = counter.entry(id.clone()).or_insert(0);
    let unique = if *count == 0 {
        id.clone()
    } else {
        format!("{id}-{count}")
    };
    *count += 1;
    unique
}

/// Parse the mdBook `SUMMARY.md` table of contents into a nested tree. Each
/// `- [Title](path)` list item becomes a [`TreeNode`]; 2-space indentation
/// denotes nesting. Non-link lines (the `# Summary` header, blank lines,
/// separators, draft entries without a link) are ignored.
///
/// HTML comment spans are stripped BEFORE the line scan. mdBook parses
/// CommonMark, so an entry commented out with `<!-- … -->` — including the
/// multi-line block form used to temporarily unpublish a whole section — is
/// not a chapter. A line scan that missed that would disagree with mdBook in
/// exactly the direction the corpus policy forbids: a commented-out (i.e.
/// deliberately unpublished) page would still count as "published" here,
/// leaving it embedded and searchable with every guardrail green.
pub fn parse_summary(md: &str) -> Vec<TreeNode> {
    let md = strip_html_comments(md);
    let flat: Vec<(usize, String, String)> = md
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("- ") && !trimmed.starts_with("* ") {
                return None;
            }
            let depth = (line.len() - trimmed.len()) / 2;
            let (title, path) = parse_link(trimmed)?;
            Some((depth, title, path))
        })
        .collect();

    let mut iter = flat.into_iter().peekable();
    build_tree(&mut iter, 0)
}

/// Remove `<!-- … -->` spans (same-line or spanning lines). An unterminated
/// comment runs to end of input, matching how mdBook's CommonMark parse
/// treats it. Newlines inside a span are preserved so line-based depth
/// arithmetic on the surviving text is unaffected.
fn strip_html_comments(md: &str) -> String {
    let mut out = String::with_capacity(md.len());
    let mut rest = md;
    while let Some(start) = rest.find("<!--") {
        out.push_str(&rest[..start]);
        match rest[start..].find("-->") {
            Some(end) => {
                let span = &rest[start..start + end];
                out.extend(span.chars().filter(|&c| c == '\n'));
                rest = &rest[start + end + 3..];
            }
            None => {
                out.extend(rest[start..].chars().filter(|&c| c == '\n'));
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

type SummaryItems = std::iter::Peekable<std::vec::IntoIter<(usize, String, String)>>;

/// Recursively assemble nodes at `depth` from the flat (depth, title, path)
/// stream, descending into deeper runs as children.
fn build_tree(items: &mut SummaryItems, depth: usize) -> Vec<TreeNode> {
    let mut nodes: Vec<TreeNode> = Vec::new();
    while let Some(&(d, _, _)) = items.peek() {
        if d < depth {
            break;
        }
        if d > depth {
            // Malformed jump (skipped a level): attach to the previous node.
            let children = build_tree(items, d);
            if let Some(last) = nodes.last_mut() {
                last.children.extend(children);
            }
            continue;
        }
        let (_, title, path) = items.next().unwrap();
        let mut node = TreeNode {
            title,
            path,
            children: Vec::new(),
        };
        if items.peek().is_some_and(|&(nd, _, _)| nd > depth) {
            node.children = build_tree(items, depth + 1);
        }
        nodes.push(node);
    }
    nodes
}

/// Extract `(title, path)` from a `- [Title](path)` list item. A leading
/// `./` on the path is normalized away — mdBook accepts
/// `- [T](./a/b.md)` as the same chapter as `a/b.md`, and embedded asset
/// paths never carry the prefix, so keeping it would make a genuinely
/// published page fail the closed-set guardrail with a message claiming
/// the opposite of the truth.
fn parse_link(item: &str) -> Option<(String, String)> {
    let s = item
        .trim_start_matches("- ")
        .trim_start_matches("* ")
        .trim();
    let open = s.find('[')?;
    let mid = s[open..].find("](")? + open;
    let close = s[mid + 2..].find(')')? + mid + 2;
    let path = s[mid + 2..close].trim_start_matches("./").to_string();
    Some((s[open + 1..mid].to_string(), path))
}

fn normalize_id(content: &str) -> String {
    content
        .chars()
        .filter_map(|ch| {
            if ch.is_alphanumeric() || ch == '_' || ch == '-' {
                Some(ch.to_ascii_lowercase())
            } else if ch.is_whitespace() {
                Some('-')
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_matches_mdbook_slug() {
        let mut c = HashMap::new();
        assert_eq!(unique_anchor("Property Paths", &mut c), "property-paths");
    }

    #[test]
    fn duplicate_anchors_get_numeric_suffix() {
        let mut c = HashMap::new();
        assert_eq!(unique_anchor("Examples", &mut c), "examples");
        assert_eq!(unique_anchor("Examples", &mut c), "examples-1");
        assert_eq!(unique_anchor("Examples", &mut c), "examples-2");
    }

    #[test]
    fn hash_inside_fenced_block_is_not_a_heading() {
        let md = "# Title\n\n```bash\n# this is a shell comment\necho hi\n```\n\n## Real Heading\n";
        let secs = parse_sections("x.md", md);
        let titles: Vec<&str> = secs.iter().map(|s| s.title.as_str()).collect();
        assert_eq!(titles, vec!["Title", "Real Heading"]);
        // The code block belongs to the first section and kept its `#` line.
        assert_eq!(secs[0].code_blocks.len(), 1);
        assert!(secs[0].code_blocks[0]
            .code
            .contains("# this is a shell comment"));
        assert_eq!(secs[0].code_blocks[0].lang, "bash");
    }

    #[test]
    fn heading_path_tracks_nesting() {
        let md = "# A\n\n## B\n\ntext\n\n### C\n\n## D\n";
        let secs = parse_sections("x.md", md);
        let c = secs.iter().find(|s| s.title == "C").unwrap();
        assert_eq!(c.heading_path, vec!["A", "B", "C"]);
        let d = secs.iter().find(|s| s.title == "D").unwrap();
        assert_eq!(d.heading_path, vec!["A", "D"]);
    }

    #[test]
    fn body_preserves_original_markdown() {
        let md = "# Title\n\nSome **bold** text.\n";
        let secs = parse_sections("x.md", md);
        assert!(secs[0].body.contains("**bold**"));
    }

    #[test]
    fn summary_ignores_html_comments_in_both_forms() {
        // mdBook (CommonMark) treats commented-out entries as not-chapters;
        // the tree/guardrail scan must agree in both the same-line and the
        // multi-line block form, or a deliberately unpublished page still
        // counts as published.
        let md = concat!(
            "# Summary\n\n",
            "- [Kept](kept.md)\n",
            "<!-- - [SameLine](same-line.md) -->\n",
            "<!--\n",
            "- [Blocked](blocked.md)\n",
            "  - [BlockedChild](blocked-child.md)\n",
            "-->\n",
            "- [Also Kept](also-kept.md)\n",
        );
        let tree = parse_summary(md);
        let paths: Vec<&str> = tree.iter().map(|n| n.path.as_str()).collect();
        assert_eq!(paths, vec!["kept.md", "also-kept.md"]);
    }

    #[test]
    fn summary_unterminated_comment_runs_to_end() {
        let md = "- [Kept](kept.md)\n<!--\n- [Gone](gone.md)\n";
        let tree = parse_summary(md);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].path, "kept.md");
    }

    #[test]
    fn summary_normalizes_dot_slash_link_prefixes() {
        // `- [T](./a/b.md)` is the same chapter as `a/b.md` to mdBook, and
        // embedded asset paths never carry `./` — without normalization the
        // guardrail would call a published page unpublished.
        let md = "- [T](./contributing/benches.md)\n";
        let tree = parse_summary(md);
        assert_eq!(tree[0].path, "contributing/benches.md");
    }
}
