//! Subprocess worker for W3C SPARQL test isolation.
//!
//! Invoked by the test harness via `subprocess::run_in_subprocess()`.
//! Reads a `TestDescriptor` from the `W3C_TEST_DESCRIPTOR` env var,
//! executes the test, and writes a `SubprocessResult` as JSON to stdout.
//!
//! The parent process kills this process on timeout, cleanly terminating
//! all threads (including infinite-looping parsers).

use std::process::ExitCode;

use testsuite_sparql::subprocess::{SubprocessResult, TestDescriptor, ENV_TEST_DESCRIPTOR};

fn main() -> ExitCode {
    let descriptor_json = match std::env::var(ENV_TEST_DESCRIPTOR) {
        Ok(json) => json,
        Err(_) => {
            eprintln!("Error: {ENV_TEST_DESCRIPTOR} env var not set.");
            eprintln!("This binary is not meant to be run directly.");
            eprintln!("It is invoked by the W3C SPARQL test harness.");
            return ExitCode::from(2);
        }
    };

    let descriptor: TestDescriptor = match serde_json::from_str(&descriptor_json) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error deserializing test descriptor: {e}");
            return ExitCode::from(2);
        }
    };

    let result = match descriptor {
        TestDescriptor::Syntax {
            ref query_url,
            ref test_id,
        } => run_syntax_test(query_url, test_id),
        TestDescriptor::Eval {
            ref test_id,
            ref query_url,
            ref data_url,
            ref result_url,
            ref graph_data,
        } => run_eval_test(
            test_id,
            query_url,
            data_url.as_deref(),
            result_url,
            graph_data,
        ),
    };

    // Write result as JSON to stdout
    if let Ok(json) = serde_json::to_string(&result) {
        println!("{json}");
    } else {
        eprintln!("Failed to serialize result");
        return ExitCode::from(2);
    }

    if result.passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn run_syntax_test(query_url: &str, test_id: &str) -> SubprocessResult {
    use fluree_db_sparql::{parse_sparql, validate, Capabilities};
    use testsuite_sparql::files::read_file_to_string;

    let query_string = match read_file_to_string(query_url) {
        Ok(s) => s,
        Err(e) => {
            return SubprocessResult {
                passed: false,
                has_errors: None,
                error: Some(format!("Reading query file for test {test_id}: {e:#}")),
            };
        }
    };

    let output = parse_sparql(&query_string);
    let mut has_errors = output.has_errors();

    // Run validation if parsing produced an AST
    if !has_errors {
        if let Some(ast) = &output.ast {
            let val_diags = validate(ast, &Capabilities::default());
            if val_diags.iter().any(|d| d.is_error()) {
                has_errors = true;
            }
        }
    }

    SubprocessResult {
        passed: true, // "passed" means the subprocess ran successfully; caller checks has_errors
        has_errors: Some(has_errors),
        error: None,
    }
}

fn run_eval_test(
    test_id: &str,
    query_url: &str,
    data_url: Option<&str>,
    result_url: &str,
    graph_data: &[(String, String)],
) -> SubprocessResult {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            return SubprocessResult {
                passed: false,
                has_errors: None,
                error: Some(format!("Failed to create Tokio runtime: {e}")),
            };
        }
    };

    let outcome = rt.block_on(async {
        testsuite_sparql::query_handler::run_eval_test(
            test_id, query_url, data_url, result_url, graph_data,
        )
        .await
    });

    match outcome {
        Ok(()) => SubprocessResult {
            passed: true,
            has_errors: None,
            error: None,
        },
        Err(e) => SubprocessResult {
            passed: false,
            has_errors: None,
            error: Some(format!("{e:#}")),
        },
    }
}
