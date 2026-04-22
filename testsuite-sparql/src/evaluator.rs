use std::collections::HashMap;

use std::io::IsTerminal;

use anyhow::{bail, Result};

use crate::manifest::Test;

/// Result of running a single test.
pub struct TestResult {
    /// Test IRI.
    pub test: String,
    /// Ok(()) if passed, Err with details if failed.
    pub outcome: Result<()>,
}

/// Dispatches tests to type-specific handler functions.
type Handler = Box<dyn Fn(&Test) -> Result<()>>;

/// Dispatches tests to type-specific handler functions.
#[derive(Default)]
pub struct TestEvaluator {
    handlers: HashMap<String, Handler>,
}

impl TestEvaluator {
    /// Register a handler for a test type URI.
    pub fn register(
        &mut self,
        test_type: impl Into<String>,
        handler: impl Fn(&Test) -> Result<()> + 'static,
    ) {
        self.handlers.insert(test_type.into(), Box::new(handler));
    }

    /// Run all tests from the manifest iterator, dispatching to registered handlers.
    pub fn evaluate(
        &self,
        manifest: impl Iterator<Item = Result<Test>>,
    ) -> Result<Vec<TestResult>> {
        let mut count = 0;
        manifest
            .map(|test| {
                let test = test?;
                count += 1;
                if std::io::stderr().is_terminal() {
                    eprint!("\r  Running test {count}: {} ...", test.id);
                } else {
                    eprintln!("  Running test {count}: {} ...", test.id);
                }
                let outcome = test
                    .kinds
                    .iter()
                    .filter_map(|kind| self.handlers.get(kind.as_str()))
                    .map(|handler| handler(&test))
                    .reduce(Result::and)
                    .unwrap_or_else(|| {
                        bail!("No handler registered for test types: {:?}", test.kinds)
                    });
                Ok(TestResult {
                    test: test.id.clone(),
                    outcome,
                })
            })
            .collect()
    }
}
