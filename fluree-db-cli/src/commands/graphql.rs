//! `fluree graphql` — query a ledger through the schema derived from its data.
//!
//! There is nothing to configure: any ledger with typed subjects has a GraphQL
//! schema, so `fluree graphql --schema mydb` prints the SDL and
//! `fluree graphql mydb '{ persons { id } }'` runs against it.

use std::path::PathBuf;

use fluree_db_api::graphql::GraphQlRequest;
use fluree_db_api::server_defaults::FlureeDir;

use crate::context;
use crate::error::{CliError, CliResult};
use crate::input::{read_input, resolve_input};

/// `fluree graphql [ledger] [query]`
#[allow(clippy::too_many_arguments)]
pub async fn run(
    explicit_ledger: Option<&str>,
    positional_inline: Option<&str>,
    expr: Option<&str>,
    file: Option<&PathBuf>,
    variables: Option<&str>,
    operation: Option<&str>,
    schema_only: bool,
    bootstrap: bool,
    explain: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;
    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);
    // The default context decides the GraphQL names and the form `id` values take,
    // so it is not optional here the way it is for a raw JSON-LD query.
    let view = fluree.db_with_default_context(&ledger_id).await?;

    if schema_only {
        // Shows mutations when the ledger's `graphql:Schema` enables them, so
        // the SDL matches what this command will accept.
        println!(
            "{}",
            fluree_db_api::graphql::schema_sdl_with_mutations(&view).await?
        );
        return Ok(());
    }

    if bootstrap {
        // Printed, never transacted: shapes activate SHACL validation for their
        // class, so applying them is the author's call, after editing.
        let model = fluree_db_api::graphql::schema_model(&view).await;
        let shapes = fluree_db_graphql::schema::bootstrap::to_shacl(&model);
        println!(
            "{}",
            serde_json::to_string_pretty(&shapes).unwrap_or_else(|_| shapes.to_string())
        );
        return Ok(());
    }

    let source = resolve_input(expr, positional_inline, file.map(PathBuf::as_path), None)?;
    let query = read_input(&source)?;

    let variables = match variables {
        Some(raw) => Some(
            serde_json::from_str(raw)
                .map_err(|e| CliError::Usage(format!("--variables is not valid JSON: {e}")))?,
        ),
        None => None,
    };

    let request = GraphQlRequest {
        query,
        variables,
        operation_name: operation.map(str::to_string),
        explain,
        // The server's defaults, not `unlimited`: a document that works here
        // should be one the endpoint will also accept.
        limits: fluree_db_api::graphql::Limits::default(),
    };
    // A mutation needs the ledger itself, not a read view — decided from the
    // document, since there is no other signal.
    let response = if fluree_db_api::graphql::is_mutation(&request) {
        let ledger = fluree.ledger(&ledger_id).await?;
        let default_context = fluree.get_default_context(&ledger_id).await?;
        let (response, _committed) = fluree
            .graphql_transact(ledger, default_context, &request)
            .await?;
        response
    } else {
        fluree.graphql(&view, &request).await?
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&response).unwrap_or_else(|_| response.to_string())
    );

    // A GraphQL error lives in the response body, so the envelope is still
    // printed; the exit code is what a script can branch on.
    if response.get("errors").is_some() {
        return Err(CliError::Usage(
            "the GraphQL response carried errors (see `errors` above)".to_string(),
        ));
    }
    Ok(())
}
