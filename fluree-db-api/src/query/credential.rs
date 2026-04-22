use serde_json::Value as JsonValue;

use crate::query::helpers::parse_and_validate_sparql;
use crate::{ApiError, DatasetSpec, Fluree, QueryConnectionOptions, QueryResult, Result};

#[cfg(feature = "credential")]
use crate::credential;

#[cfg(feature = "credential")]
impl Fluree {
    /// Execute a credentialed connection query
    pub async fn credential_query_connection(
        &self,
        credential: credential::Input<'_>,
        values_map: Option<std::collections::HashMap<String, JsonValue>>,
    ) -> Result<QueryResult> {
        let verified = credential::verify_credential(credential)?;

        // Inject identity (and values_map) into query's opts field
        let mut query = verified.subject.clone();
        if let Some(obj) = query.as_object_mut() {
            let opts = obj.entry("opts").or_insert(serde_json::json!({}));
            if let Some(opts_obj) = opts.as_object_mut() {
                opts_obj.insert("identity".to_string(), serde_json::json!(verified.did));
                if let Some(vals) = values_map {
                    // Canonical field name: "policy-values" (with hyphen)
                    opts_obj.insert("policy-values".to_string(), serde_json::json!(vals));
                }
            }
        }

        self.query_connection(&query).await
    }

    /// Execute a credentialed SPARQL query
    pub async fn credential_query_sparql(
        &self,
        jws: &str,
        values_map: Option<std::collections::HashMap<String, JsonValue>>,
    ) -> Result<QueryResult> {
        // Use verify_jws directly to get raw payload without JSON parsing
        let jws_result = credential::verify_jws_sparql(jws)?;

        // jws_result.payload is the raw SPARQL string
        let sparql = &jws_result.payload;
        let identity = jws_result.did;

        // Parse SPARQL to extract dataset spec
        let ast = parse_and_validate_sparql(sparql)?;

        // Extract dataset clause
        let dataset_clause = match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Update(_) => None,
        };

        let spec = match dataset_clause {
            Some(clause) => DatasetSpec::from_sparql_clause(clause)
                .map_err(|e| ApiError::query(e.to_string()))?,
            None => DatasetSpec::default(),
        };

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL query (no FROM / FROM NAMED)",
            ));
        }

        let opts = QueryConnectionOptions {
            identity: Some(identity),
            policy_values: values_map,
            ..Default::default()
        };

        let dataset = self.build_dataset_view_with_policy(&spec, &opts).await?;
        self.query_dataset(&dataset, sparql).await
    }
}
