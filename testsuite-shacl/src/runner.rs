//! Run one test case through the `fluree_db_api::validate` core.

use anyhow::{Context, Result};
use fluree_db_api::validate::{ShapesSource, ValidateOptions, ValidateReport};
use fluree_db_api::{Fluree, FlureeBuilder};

use crate::compare::compare_report;
use crate::file_iri;
use crate::manifest::{Expectation, TestCase};

/// Outcome of running one case.
#[derive(Debug)]
pub enum Outcome {
    Pass,
    /// The report was produced but did not match the expected one.
    Fail(String),
    /// The pipeline errored where a report was expected (load failure,
    /// unsupported construct, validation error).
    Error(String),
}

impl Outcome {
    pub fn passed(&self) -> bool {
        matches!(self, Outcome::Pass)
    }
}

/// Load the data graph into an ephemeral memory ledger, validate against the
/// shapes graph (inline Turtle), compare against the expectation.
pub async fn run_case(case: &TestCase) -> Outcome {
    match execute(case).await {
        Ok(outcome) => outcome,
        Err(e) => match case.expect {
            // A processing failure is the expected outcome of sht:Failure tests.
            Expectation::Failure => Outcome::Pass,
            _ => Outcome::Error(format!("{e:#}")),
        },
    }
}

async fn execute(case: &TestCase) -> Result<Outcome> {
    let data = read_with_base(&case.data_path)?;
    let shapes = read_with_base(&case.shapes_path)?;

    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "w3c/shacl:main";
    fluree.create_ledger(alias).await?;
    disable_staging_shacl(&fluree, alias).await?;

    fluree
        .graph(alias)
        .transact()
        .insert_turtle(&data)
        .commit()
        .await
        .context("loading data graph")?;

    let options = ValidateOptions {
        graph: None,
        shapes: ShapesSource::InlineTurtle(shapes),
        include_attached: false,
    };
    let report: ValidateReport = fluree
        .validate_ledger(alias, &options)
        .await
        .context("validating")?;

    match &case.expect {
        Expectation::Failure => Ok(Outcome::Fail(
            "expected a validation failure, got a report".to_string(),
        )),
        Expectation::Report { conforms, results } => {
            match compare_report(*conforms, results, &report) {
                None => Ok(Outcome::Pass),
                Some(mismatch) => Ok(Outcome::Fail(mismatch.to_string())),
            }
        }
    }
}

/// Read a Turtle file, prepending `@base <file://...>` so relative IRIs
/// (including `<>` self-references) resolve the same way they do in the
/// manifest graph the expected report was parsed from.
fn read_with_base(path: &std::path::Path) -> Result<String> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(format!("@base <{}> .\n{raw}", file_iri(path)))
}

/// Test files embed shapes and (often deliberately violating) data in one
/// document — staging-time SHACL enforcement must not reject the load.
/// Same mechanism as `fluree validate`'s file mode.
async fn disable_staging_shacl(fluree: &Fluree, alias: &str) -> Result<()> {
    let config_iri = fluree_db_core::graph_registry::config_graph_iri(alias);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:w3c> rdf:type f:LedgerConfig .
            <urn:config:w3c> f:shaclDefaults <urn:config:w3c-shacl> .
            <urn:config:w3c-shacl> f:shaclEnabled false .
        }}
    "
    );
    fluree
        .graph(alias)
        .transact()
        .upsert_turtle(&trig)
        .commit()
        .await?;
    Ok(())
}
