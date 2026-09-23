//! What a Unity Catalog holds: its catalogs, schemas and tables, and one
//! table's columns and declared keys. Nothing here reads a table's files.

use fluree_db_tabular::FieldType;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::config::UnityConfig;
use crate::error::Result;
use crate::unity::UnityClient;

/// Schemas listed at once when a whole catalog's tables are asked for.
const LISTING_CONCURRENCY: usize = 8;

const SYSTEM_SCHEMA: &str = "information_schema";

/// How far a listing of one catalog reaches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrowseDepth {
    Schemas,
    #[default]
    Tables,
}

/// A listing, as far down as the config's `catalog` and `schema` reach: the
/// metastore's catalogs, one catalog's schemas, or one schema's tables.
#[derive(Debug, Clone, Default, Serialize)]
pub struct UnityListing {
    pub catalogs: Vec<String>,
    /// `catalog.schema`
    pub schemas: Vec<String>,
    pub tables: Vec<ListedTable>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListedTable {
    /// `catalog.schema.table`, the name a mapping gives the table
    pub full_name: String,
    /// Unity's table type: `MANAGED`, `EXTERNAL`, `VIEW`, …
    pub kind: String,
    pub format: Option<String>,
    pub comment: Option<String>,
    /// A row filter, which Unity may decline to issue credentials past. A
    /// listing does not show column masks; a table's description does.
    pub access_rule: Option<String>,
    /// Why this reader cannot read the object at all.
    pub unreadable: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TableDescription {
    pub full_name: String,
    pub kind: String,
    pub format: Option<String>,
    pub location: Option<String>,
    pub comment: Option<String>,
    pub access_rule: Option<String>,
    pub unreadable: Option<String>,
    /// In table order.
    pub columns: Vec<DescribedColumn>,
    /// Declared, and by Unity's own account not enforced.
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<DeclaredForeignKey>,
}

#[derive(Debug, Clone)]
pub struct DescribedColumn {
    pub name: String,
    pub position: i32,
    /// The type as Unity writes it: `bigint`, `decimal(12,2)`, `array<int>`.
    pub type_text: String,
    /// The type a scan yields; `None` for one the reader cannot carry.
    pub field_type: Option<FieldType>,
    pub nullable: bool,
    pub comment: Option<String>,
    pub masked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeclaredForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    /// `catalog.schema.table`
    pub parent_table: String,
    pub parent_columns: Vec<String>,
}

/// List what `unity` holds. `unity` must be hydrated.
pub async fn browse_unity(unity: &UnityConfig, depth: BrowseDepth) -> Result<UnityListing> {
    unity.validate()?;
    let client = Arc::new(UnityClient::new(unity)?);
    browse(&client, unity, depth).await
}

pub(crate) async fn browse(
    client: &Arc<UnityClient>,
    unity: &UnityConfig,
    depth: BrowseDepth,
) -> Result<UnityListing> {
    let Some(catalog) = unity.catalog.as_deref() else {
        return Ok(UnityListing {
            catalogs: client.catalogs().await?,
            ..Default::default()
        });
    };
    if let Some(schema) = unity.schema.as_deref() {
        return Ok(UnityListing {
            tables: client.tables(catalog, schema).await?,
            ..Default::default()
        });
    }
    let schemas = client.schemas(catalog).await?;
    let mut tables = Vec::new();
    if depth == BrowseDepth::Tables {
        // Every catalog's system views are left out; they are still listed
        // when the schema is asked for by name. Each schema is asked for as
        // Unity spells it, which may not be as the config does.
        let wanted: Vec<(String, String)> = schemas
            .iter()
            .filter(|schema| !schema.name.eq_ignore_ascii_case(SYSTEM_SCHEMA))
            .map(|schema| (schema.catalog.clone(), schema.name.clone()))
            .collect();
        // Owned values keep the future `Send`; `buffered` keeps the order.
        let listed: Vec<Vec<ListedTable>> = futures::stream::iter(wanted)
            .map(|(catalog, schema)| {
                let client = client.clone();
                async move { client.tables(&catalog, &schema).await }
            })
            .buffered(LISTING_CONCURRENCY)
            .try_collect()
            .await?;
        tables = listed.into_iter().flatten().collect();
    }
    Ok(UnityListing {
        catalogs: Vec::new(),
        schemas: schemas.into_iter().map(|s| s.full_name).collect(),
        tables,
    })
}

/// Describe each of `table_names`, in order, over one client: one token
/// exchange for the lot rather than one per table. `unity` must be hydrated.
pub async fn describe_unity_tables(
    unity: &UnityConfig,
    table_names: &[String],
) -> Result<Vec<TableDescription>> {
    unity.validate()?;
    let client = Arc::new(UnityClient::new(unity)?);
    describe_all(&client, unity, table_names).await
}

pub(crate) async fn describe_all(
    client: &Arc<UnityClient>,
    unity: &UnityConfig,
    table_names: &[String],
) -> Result<Vec<TableDescription>> {
    let full_names = table_names
        .iter()
        .map(|name| unity.full_name(name))
        .collect::<Result<Vec<_>>>()?;
    // Owned values keep the future `Send`; `buffered` keeps the order.
    futures::stream::iter(full_names)
        .map(|full_name| {
            let client = client.clone();
            async move { client.describe(&full_name).await }
        })
        .buffered(LISTING_CONCURRENCY)
        .try_collect()
        .await
}

/// Describe `table_name`, completed from the config's defaults like a mapped
/// table's name. `unity` must be hydrated.
pub async fn describe_unity_table(
    unity: &UnityConfig,
    table_name: &str,
) -> Result<TableDescription> {
    unity.validate()?;
    UnityClient::new(unity)?
        .describe(&unity.full_name(table_name)?)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DeltaError;
    use fluree_db_iceberg::auth::AuthConfig;
    use fluree_db_iceberg::ConfigValue;
    use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const UNITY: &str = "/api/2.1/unity-catalog";

    fn config(server: &MockServer, catalog: Option<&str>, schema: Option<&str>) -> UnityConfig {
        UnityConfig {
            uri: server.uri(),
            auth: AuthConfig::Bearer {
                token: ConfigValue::literal("catalog-token"),
            },
            catalog: catalog.map(String::from),
            schema: schema.map(String::from),
        }
    }

    /// A plain HTTP client: the production one refuses the loopback mock.
    fn client(config: &UnityConfig) -> Arc<UnityClient> {
        Arc::new(UnityClient::with_http(config, reqwest::Client::new()).unwrap())
    }

    async fn serve(
        server: &MockServer,
        route: &str,
        scope: &[(&str, &str)],
        body: serde_json::Value,
    ) {
        let mut mock = Mock::given(method("GET"))
            .and(path(format!("{UNITY}/{route}")))
            .and(header("Authorization", "Bearer catalog-token"))
            .and(query_param_is_missing("page_token"));
        for (key, value) in scope {
            mock = mock.and(query_param(*key, *value));
        }
        mock.respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    fn delta(full_name: &str) -> serde_json::Value {
        serde_json::json!({
            "full_name": full_name, "table_type": "MANAGED", "data_source_format": "DELTA",
        })
    }

    fn names(tables: &[ListedTable]) -> Vec<&str> {
        tables.iter().map(|t| t.full_name.as_str()).collect()
    }

    async fn serve_main(server: &MockServer) {
        serve(
            server,
            "catalogs",
            &[],
            serde_json::json!({"catalogs": [{"name": "main"}, {"name": "samples"}]}),
        )
        .await;
        serve(
            server,
            "schemas",
            &[("catalog_name", "main")],
            serde_json::json!({"schemas": [
                {"full_name": "main.sales"},
                {"full_name": "main.information_schema"},
                {"full_name": "main.hr"},
            ]}),
        )
        .await;
        for (schema, table) in [
            ("sales", "main.sales.orders"),
            ("hr", "main.hr.people"),
            ("information_schema", "main.information_schema.tables"),
        ] {
            serve(
                server,
                "tables",
                &[
                    ("catalog_name", "main"),
                    ("schema_name", schema),
                    ("omit_columns", "true"),
                ],
                serde_json::json!({"tables": [delta(table)]}),
            )
            .await;
        }
    }

    /// Unity folds names to lowercase and matches them without regard to case.
    /// A catalog asked for with capitals is listed under Unity's spelling, and
    /// a table described that way is known by Unity's name.
    #[tokio::test]
    async fn names_are_unitys_spelling_not_the_requests() {
        let server = MockServer::start().await;
        serve(
            &server,
            "schemas",
            &[("catalog_name", "Main")],
            serde_json::json!({"schemas": [
                {"full_name": "main.sales", "catalog_name": "main", "name": "sales"},
                {"full_name": "main.information_schema", "catalog_name": "main",
                 "name": "information_schema"},
            ]}),
        )
        .await;
        serve(
            &server,
            "tables",
            &[
                ("catalog_name", "main"),
                ("schema_name", "sales"),
                ("omit_columns", "true"),
            ],
            serde_json::json!({"tables": [delta("main.sales.orders")]}),
        )
        .await;
        let catalog = config(&server, Some("Main"), None);
        let listing = browse(&client(&catalog), &catalog, BrowseDepth::Tables)
            .await
            .expect("each schema is asked for as Unity names it");
        assert_eq!(names(&listing.tables), ["main.sales.orders"]);

        serve(
            &server,
            "tables/MAIN.SALES.ORDERS",
            &[],
            serde_json::json!({
                "full_name": "main.sales.orders", "table_type": "MANAGED",
                "data_source_format": "DELTA", "storage_location": "s3://bucket/orders",
            }),
        )
        .await;
        let unity = config(&server, None, None);
        let described = describe_all(&client(&unity), &unity, &["MAIN.SALES.ORDERS".to_string()])
            .await
            .unwrap();
        assert_eq!(described[0].full_name, "main.sales.orders");
    }

    #[tokio::test]
    async fn a_listing_reaches_as_far_as_the_config_names() {
        let server = MockServer::start().await;
        serve_main(&server).await;

        let unscoped = config(&server, None, None);
        let listing = browse(&client(&unscoped), &unscoped, BrowseDepth::Tables)
            .await
            .unwrap();
        assert_eq!(listing.catalogs, ["main", "samples"]);
        assert!(listing.schemas.is_empty() && listing.tables.is_empty());

        let catalog = config(&server, Some("main"), None);
        let listing = browse(&client(&catalog), &catalog, BrowseDepth::Tables)
            .await
            .unwrap();
        assert!(listing.catalogs.is_empty());
        let schemas = ["main.sales", "main.information_schema", "main.hr"];
        assert_eq!(listing.schemas, schemas);
        assert_eq!(
            names(&listing.tables),
            ["main.sales.orders", "main.hr.people"]
        );

        let shallow = browse(&client(&catalog), &catalog, BrowseDepth::Schemas)
            .await
            .unwrap();
        assert_eq!(shallow.schemas, schemas);
        assert!(shallow.tables.is_empty());

        let schema = config(&server, Some("main"), Some("hr"));
        let listing = browse(&client(&schema), &schema, BrowseDepth::Schemas)
            .await
            .unwrap();
        assert!(listing.schemas.is_empty());
        assert_eq!(names(&listing.tables), ["main.hr.people"]);

        let system = config(&server, Some("main"), Some(SYSTEM_SCHEMA));
        let listing = browse(&client(&system), &system, BrowseDepth::Tables)
            .await
            .unwrap();
        assert_eq!(names(&listing.tables), ["main.information_schema.tables"]);
    }

    #[tokio::test]
    async fn a_listing_follows_every_page() {
        let server = MockServer::start().await;
        let scope = [("catalog_name", "main"), ("schema_name", "sales")];
        serve(
            &server,
            "tables",
            &scope,
            serde_json::json!({"tables": [delta("main.sales.a")], "next_page_token": "p&x=2"}),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(format!("{UNITY}/tables")))
            .and(query_param("schema_name", "sales"))
            .and(query_param("page_token", "p&x=2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"tables": [delta("main.sales.b")], "next_page_token": ""}),
            ))
            .expect(1)
            .mount(&server)
            .await;

        let unity = config(&server, Some("main"), Some("sales"));
        let listing = browse(&client(&unity), &unity, BrowseDepth::Tables)
            .await
            .unwrap();
        assert_eq!(names(&listing.tables), ["main.sales.a", "main.sales.b"]);
    }

    #[tokio::test]
    async fn a_page_token_that_repeats_ends_the_listing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("{UNITY}/tables")))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"tables": [delta("main.sales.a")], "next_page_token": "same"}),
            ))
            .mount(&server)
            .await;
        let unity = config(&server, Some("main"), Some("sales"));
        let client = client(&unity);
        let listing = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            browse(&client, &unity, BrowseDepth::Tables),
        )
        .await
        .expect("the listing ends")
        .unwrap();
        assert_eq!(listing.tables.len(), 2);
    }

    #[tokio::test]
    async fn a_listed_table_says_what_cannot_be_read_and_what_unity_governs() {
        let server = MockServer::start().await;
        serve(
            &server,
            "tables",
            &[("catalog_name", "main"), ("schema_name", "sales")],
            serde_json::json!({"tables": [
                delta("main.sales.orders"),
                {"full_name": "main.sales.recent", "table_type": "VIEW"},
                {"full_name": "main.sales.events", "table_type": "EXTERNAL",
                 "data_source_format": "PARQUET", "comment": "raw"},
                {"full_name": "main.sales.eu", "table_type": "MANAGED",
                 "data_source_format": "DELTA", "row_filter": {"function_name": "f"}},
            ]}),
        )
        .await;
        let unity = config(&server, Some("main"), Some("sales"));
        let tables = browse(&client(&unity), &unity, BrowseDepth::Tables)
            .await
            .unwrap()
            .tables;

        let orders = &tables[0];
        assert_eq!(
            (orders.kind.as_str(), orders.format.as_deref()),
            ("MANAGED", Some("DELTA"))
        );
        assert!(orders.unreadable.is_none() && orders.access_rule.is_none());
        assert_eq!(
            tables[1].unreadable.as_deref(),
            Some("is a VIEW, not a Delta table")
        );
        assert!(tables[2].unreadable.as_deref().unwrap().contains("PARQUET"));
        assert_eq!(tables[2].comment.as_deref(), Some("raw"));
        // Whether a governed table can be read is Unity's to say.
        assert_eq!(tables[3].access_rule.as_deref(), Some("row filter"));
        assert!(tables[3].unreadable.is_none());
    }

    #[tokio::test]
    async fn a_description_carries_columns_scan_types_and_declared_keys() {
        let server = MockServer::start().await;
        let column = |position: i32, name: &str, text: &str, json: serde_json::Value| {
            let field = serde_json::json!({
                "name": name, "type": json, "nullable": true, "metadata": {}
            });
            serde_json::json!({
                "name": name, "position": position, "type_text": text,
                "type_json": field.to_string(), "type_precision": 0, "type_scale": 0,
            })
        };
        let mut line = column(1, "line", "int", "integer".into());
        line["nullable"] = false.into();
        let mut note = column(3, "note", "string", "string".into());
        note["mask"] = serde_json::json!({"function_name": "hide"});
        note["comment"] = "free text".into();
        serve(
            &server,
            "tables/main.sales.orders",
            &[],
            serde_json::json!({
                "table_id": "t-1", "table_type": "MANAGED", "data_source_format": "DELTA",
                "storage_location": "s3://bucket/t-1", "comment": "every order line",
                "columns": [
                    note, line,
                    column(0, "order_id", "bigint", "long".into()),
                    column(2, "total", "decimal(12,2)", "decimal(12,2)".into()),
                    column(4, "tags", "array<string>", serde_json::json!({
                        "type": "array", "elementType": "string", "containsNull": true
                    })),
                ],
                "table_constraints": [
                    {"primary_key_constraint": {"name": "pk", "child_columns": ["order_id", "line"]}},
                    {"foreign_key_constraint": {
                        "name": "fk", "child_columns": ["order_id"],
                        "parent_table": "main.sales.headers", "parent_columns": ["id"],
                    }},
                ],
            }),
        )
        .await;
        let unity = config(&server, Some("main"), Some("sales"));
        let table = client(&unity).describe("main.sales.orders").await.unwrap();

        let seen: Vec<_> = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.field_type, c.nullable, c.masked))
            .collect();
        assert_eq!(
            seen,
            [
                ("order_id", Some(FieldType::Int64), true, false),
                ("line", Some(FieldType::Int32), false, false),
                // Unity's own precision and scale fields read zero.
                (
                    "total",
                    Some(FieldType::Decimal {
                        precision: 12,
                        scale: 2
                    }),
                    true,
                    false
                ),
                ("note", Some(FieldType::String), true, true),
                ("tags", None, true, false),
            ]
        );
        assert_eq!(table.columns[3].comment.as_deref(), Some("free text"));
        assert_eq!(table.columns[4].type_text, "array<string>");
        assert_eq!(table.primary_key, ["order_id", "line"]);
        assert_eq!(
            table.foreign_keys,
            [DeclaredForeignKey {
                name: "fk".into(),
                columns: vec!["order_id".into()],
                parent_table: "main.sales.headers".into(),
                parent_columns: vec!["id".into()],
            }]
        );
        assert_eq!(table.access_rule.as_deref(), Some("column mask"));
        assert_eq!(table.location.as_deref(), Some("s3://bucket/t-1"));
        assert_eq!(table.comment.as_deref(), Some("every order line"));
        assert!(table.unreadable.is_none());
    }

    #[tokio::test]
    async fn a_view_is_described_not_refused() {
        let server = MockServer::start().await;
        serve(
            &server,
            "tables/main.sales.recent",
            &[],
            serde_json::json!({"table_id": "v-1", "table_type": "VIEW", "columns": []}),
        )
        .await;
        let unity = config(&server, None, None);
        let view = client(&unity).describe("main.sales.recent").await.unwrap();
        assert_eq!(
            view.unreadable.as_deref(),
            Some("is a VIEW, not a Delta table")
        );
        assert!(view.location.is_none());
    }

    #[tokio::test]
    async fn a_refused_listing_names_its_scope_and_unitys_reason() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("{UNITY}/schemas")))
            .respond_with(
                ResponseTemplate::new(403).set_body_json(
                    serde_json::json!({"message": "User lacks USE CATALOG on 'main'"}),
                ),
            )
            .mount(&server)
            .await;
        let unity = config(&server, Some("main"), None);
        let error = browse(&client(&unity), &unity, BrowseDepth::Schemas)
            .await
            .unwrap_err();
        match &error {
            DeltaError::CatalogListing { scope, message } => {
                assert_eq!(scope, "catalog 'main'");
                assert!(
                    message.contains("USE CATALOG") && message.contains("403"),
                    "{message}"
                );
            }
            other => panic!("expected a listing error, got {other:?}"),
        }
        assert!(error
            .to_string()
            .starts_with("Unity Catalog, catalog 'main':"));
    }
}
