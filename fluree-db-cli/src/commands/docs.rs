//! `fluree docs` — search, read, and extract examples from the embedded,
//! version-pinned documentation, plus serve them over MCP (`fluree docs serve`).
//!
//! The corpus is baked into the binary by `fluree-db-docs`, so every result is
//! offline and version-exact for this build. The `serve` subcommand starts the
//! standalone `fluree-docs` MCP server (separate from `fluree mcp serve`, which
//! serves developer memory).

use crate::cli::DocsAction;
use crate::error::{CliError, CliResult};

pub async fn run(action: DocsAction) -> CliResult<()> {
    match action {
        DocsAction::Search { query, limit, json } => search(&query, limit, json),
        DocsAction::Get { path, anchor, json } => get(&path, anchor.as_deref(), json),
        DocsAction::Examples {
            query,
            lang,
            limit,
            json,
        } => examples(&query, lang.as_deref(), limit, json),
        DocsAction::Tree { json } => tree(json),
        DocsAction::Serve { transport } => serve(&transport).await,
    }
}

fn search(query: &str, limit: usize, json: bool) -> CliResult<()> {
    let hits = fluree_db_docs::index().search(query, limit);

    if json {
        println!("{}", serde_json::to_string_pretty(&hits)?);
        return Ok(());
    }

    if hits.is_empty() {
        println!("No matches for \"{query}\". Try different or more specific keywords.");
        print_footer();
        return Ok(());
    }

    println!(
        "docs v{} — {} result(s) for \"{query}\"\n",
        version(),
        hits.len()
    );
    for (i, h) in hits.iter().enumerate() {
        let n = i + 1;
        println!("{n}. {}#{}  (score {:.2})", h.path, h.anchor, h.score);
        if !h.heading_path.is_empty() {
            println!("   {}", h.heading_path.join(" › "));
        }
        if !h.snippet.is_empty() {
            println!("   {}", h.snippet);
        }
        println!();
    }
    print_hint();
    Ok(())
}

fn get(path: &str, anchor: Option<&str>, json: bool) -> CliResult<()> {
    let Some(page) = fluree_db_docs::index().get(path, anchor) else {
        return Err(CliError::Input(format!(
            "no docs page found for '{path}'. Use `fluree docs search` to find valid paths."
        )));
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&page)?);
        return Ok(());
    }

    println!("{}", page.content);
    print_footer();
    Ok(())
}

fn examples(query: &str, lang: Option<&str>, limit: usize, json: bool) -> CliResult<()> {
    let examples = fluree_db_docs::index().examples(query, lang, limit);

    if json {
        println!("{}", serde_json::to_string_pretty(&examples)?);
        return Ok(());
    }

    if examples.is_empty() {
        println!("No examples for \"{query}\". Try different keywords or drop the --lang filter.");
        print_footer();
        return Ok(());
    }

    println!(
        "docs v{} — {} example(s) for \"{query}\"\n",
        version(),
        examples.len()
    );
    for (i, e) in examples.iter().enumerate() {
        let n = i + 1;
        let lang = if e.lang.is_empty() { "text" } else { &e.lang };
        println!("{n}. {}#{}  [{lang}]", e.path, e.anchor);
        println!("```{lang}");
        println!("{}", e.code.trim_end());
        println!("```\n");
    }
    Ok(())
}

fn tree(json: bool) -> CliResult<()> {
    let tree = fluree_db_docs::index().tree();
    if json {
        println!("{}", serde_json::to_string_pretty(&tree)?);
        return Ok(());
    }
    println!("docs v{}\n", version());
    for node in &tree.nodes {
        print_node(node, 0);
    }
    Ok(())
}

fn print_node(node: &fluree_db_docs::TreeNode, depth: usize) {
    println!("{}{}  ({})", "  ".repeat(depth), node.title, node.path);
    for child in &node.children {
        print_node(child, depth + 1);
    }
}

/// Start the standalone `fluree-docs` MCP server. Reads JSON-RPC over stdio, so
/// it must not write to stdout/stderr.
async fn serve(transport: &str) -> CliResult<()> {
    if transport != "stdio" {
        return Err(CliError::Usage(format!(
            "unsupported MCP transport '{transport}'; valid: stdio"
        )));
    }

    use rmcp::ServiceExt;
    let service = fluree_db_docs::mcp::DocsToolService::new();
    let server = service
        .serve(rmcp::transport::io::stdio())
        .await
        .map_err(|e| CliError::Config(format!("failed to start docs MCP server: {e}")))?;
    server
        .waiting()
        .await
        .map_err(|e| CliError::Config(format!("docs MCP server error: {e}")))?;
    Ok(())
}

fn version() -> &'static str {
    fluree_db_docs::version()
}

fn print_footer() {
    println!("\ndocs v{}", version());
}

fn print_hint() {
    println!(
        "Read a hit: fluree docs get <path> [--anchor <anchor>]   (docs v{})",
        version()
    );
}
