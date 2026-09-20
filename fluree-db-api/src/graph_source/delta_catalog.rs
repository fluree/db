//! Getting to a Delta source on Unity Catalog: what the catalog holds, what a
//! table looks like, whether it can be read, a mapping generated from the
//! tables, and a mapping checked against them. Nothing here registers a
//! source, and only the last two read anything but the catalog.

use std::collections::{BTreeSet, HashMap};

use fluree_db_delta::{
    BrowseDepth, DeclaredForeignKey, DeltaError, DeltaIoConfig, TableDescription, UnityConfig,
    UnityListing, VersionSelector,
};
use fluree_db_r2rml::emit::{
    self, emit_r2rml, naming::xsd_datatype, DiagCode, Diagnostic, EmitColumn, EmitColumnStats,
    EmitTableSchema, Severity, StructuredR2rmlMapping, TableKey, TableOverride,
};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

use super::delta::{open_placed, DeltaCreateConfig};
use super::iceberg_generate::{emit_options, validate_base_namespace, GenerateOptions};
use super::iceberg_validate::{
    compile_for_validate, cross_check_live, LiveColumn, ValidateR2rmlResponse, Wording,
};
use super::R2rmlMappingInput;
use crate::Result;

/// Tables described at once for one generated mapping.
const DESCRIBE_CONCURRENCY: usize = 8;

const DELTA_WORDING: Wording = Wording {
    casing: "The Delta reader resolves names ignoring case, so queries work; match the schema's \
             spelling to keep the mapping exact.",
    unread: "the table could not be read",
};

/// A table as Unity describes it. No file of the table is read for this.
#[derive(Debug, Clone, Serialize)]
pub struct DeltaTablePreview {
    /// `catalog.schema.table`, the name a mapping gives the table
    pub full_name: String,
    /// Unity's table type: `MANAGED`, `EXTERNAL`, `VIEW`, …
    pub kind: String,
    pub format: Option<String>,
    pub location: Option<String>,
    pub comment: Option<String>,
    /// `row filter` or `column mask`: Unity may decline credentials past one.
    pub access_rule: Option<String>,
    /// Why this reader cannot read the object at all.
    pub unreadable: Option<String>,
    pub columns: Vec<DeltaColumnInfo>,
    /// Declared in Unity, which does not enforce it.
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<DeclaredForeignKey>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaColumnInfo {
    pub name: String,
    pub position: i32,
    /// The type as Unity writes it: `bigint`, `decimal(12,2)`, `array<int>`.
    pub type_text: String,
    /// The datatype a generated mapping gives the column; `None` for a string
    /// or a column that is not `mappable`.
    pub xsd_type: Option<String>,
    /// False for a type a mapping cannot address (nested, variant, …).
    pub mappable: bool,
    pub nullable: bool,
    pub comment: Option<String>,
    pub masked: bool,
}

impl From<TableDescription> for DeltaTablePreview {
    fn from(table: TableDescription) -> Self {
        Self {
            columns: table
                .columns
                .into_iter()
                .map(|c| DeltaColumnInfo {
                    xsd_type: c
                        .field_type
                        .and_then(|t| xsd_datatype(t, true))
                        .map(str::to_string),
                    mappable: c.field_type.is_some(),
                    name: c.name,
                    position: c.position,
                    type_text: c.type_text,
                    nullable: c.nullable,
                    comment: c.comment,
                    masked: c.masked,
                })
                .collect(),
            full_name: table.full_name,
            kind: table.kind,
            format: table.format,
            location: table.location,
            comment: table.comment,
            access_rule: table.access_rule,
            unreadable: table.unreadable,
            primary_key: table.primary_key,
            foreign_keys: table.foreign_keys,
        }
    }
}

/// Whether a table can be read with the credentials Unity issues for it.
#[derive(Debug, Clone, Serialize)]
pub struct DeltaTableAccess {
    pub full_name: String,
    pub readable: bool,
    /// Where Unity places the table; absent if it would not say.
    pub location: Option<String>,
    /// The table's current version, read from its log.
    pub version: Option<u64>,
    pub data_file_count: Option<usize>,
    /// Why not, in the catalog's or the store's own words.
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GenerateDeltaR2rmlRequest {
    pub unity: UnityConfig,
    /// Table names, completed from the config's defaults; in output order.
    pub tables: Vec<String>,
    pub base_namespace: String,
    /// Keyed by table name as given in `tables`.
    pub per_table_overrides: HashMap<String, TableOverride>,
    pub options: GenerateOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct GenerateDeltaR2rmlResponse {
    pub turtle: String,
    pub structured: StructuredR2rmlMapping,
    pub diagnostics: Vec<Diagnostic>,
    /// The `rr:tableName` of each mapped table, in request order.
    pub tables: Vec<String>,
}

fn config_error(e: DeltaError) -> crate::ApiError {
    crate::ApiError::Config(e.to_string())
}

/// `catalog.schema.table` as the emitter's namespace and name.
fn table_key(full_name: &str) -> TableKey {
    match full_name.rsplit_once('.') {
        Some((namespace, name)) => TableKey::new(namespace, name),
        None => TableKey::new("", full_name),
    }
}

fn emit_schema(table: &TableDescription) -> EmitTableSchema {
    let key = table_key(&table.full_name);
    let columns: Vec<EmitColumn> = table
        .columns
        .iter()
        .map(|c| EmitColumn {
            // Delta has no durable column id in the catalog's record; a
            // column's place orders the mapping just as well.
            field_id: c.position,
            name: c.name.clone(),
            iceberg_type: c.type_text.clone(),
            // Never read for a column the emitter passes over.
            field_type: c.field_type.unwrap_or(fluree_db_tabular::FieldType::String),
            required: !c.nullable,
            nested: c.field_type.is_none(),
            doc: c.comment.clone(),
            stats: EmitColumnStats::default(),
        })
        .collect();
    let identifier_field_ids = table
        .primary_key
        .iter()
        .filter_map(|name| table.columns.iter().find(|c| &c.name == name))
        .map(|c| c.position)
        .collect();
    EmitTableSchema {
        namespace: key.namespace,
        name: key.name,
        columns,
        identifier_field_ids,
        foreign_keys: table
            .foreign_keys
            .iter()
            .map(|k| emit::DeclaredForeignKey {
                child_columns: k.columns.clone(),
                parent_table: k.parent_table.clone(),
                parent_columns: k.parent_columns.clone(),
            })
            .collect(),
    }
}

/// Overrides, given by table name as the request spells it, under the key the
/// emitter knows each table by.
fn keyed_overrides(
    unity: &UnityConfig,
    tables: &[String],
    overrides: &HashMap<String, TableOverride>,
) -> Result<HashMap<TableKey, TableOverride>> {
    overrides
        .iter()
        .map(|(name, table_override)| {
            if !tables.contains(name) {
                return Err(crate::ApiError::config(format!(
                    "an override names '{name}', which is not among the tables"
                )));
            }
            let full_name = unity.full_name(name).map_err(config_error)?;
            Ok((table_key(&full_name), table_override.clone()))
        })
        .collect()
}

/// The mapping for tables already described. Pure.
fn generate_from(
    tables: &[TableDescription],
    base_namespace: &str,
    options: &GenerateOptions,
    overrides: HashMap<TableKey, TableOverride>,
) -> Result<GenerateDeltaR2rmlResponse> {
    validate_base_namespace(base_namespace)?;
    if let Some(table) = tables.iter().find(|t| t.unreadable.is_some()) {
        return Err(crate::ApiError::config(format!(
            "'{}' {}",
            table.full_name,
            table.unreadable.as_deref().unwrap_or_default()
        )));
    }
    let schemas: Vec<EmitTableSchema> = tables.iter().map(emit_schema).collect();
    let emit_options = emit::EmitOptions {
        declared_key_source: "the primary key Unity Catalog declares".to_string(),
        ..emit_options(base_namespace, options, overrides)
    };
    let output = emit_r2rml(&schemas, &emit_options);
    Ok(GenerateDeltaR2rmlResponse {
        turtle: output.turtle,
        structured: output.structured,
        diagnostics: output.diagnostics,
        tables: tables.iter().map(|t| t.full_name.clone()).collect(),
    })
}

impl crate::Fluree {
    async fn hydrate_unity(&self, unity: &UnityConfig) -> Result<UnityConfig> {
        unity
            .hydrate(self.secret_resolver())
            .await
            .map_err(config_error)
    }

    /// List a Unity Catalog as far as `unity`'s `catalog` and `schema` reach.
    pub async fn browse_delta_unity(
        &self,
        unity: &UnityConfig,
        depth: BrowseDepth,
    ) -> Result<UnityListing> {
        let unity = self.hydrate_unity(unity).await?;
        fluree_db_delta::browse_unity(&unity, depth)
            .await
            .map_err(config_error)
    }

    /// One table's columns and declared keys, from Unity's record of it.
    pub async fn preview_delta_unity_table(
        &self,
        unity: &UnityConfig,
        table: &str,
    ) -> Result<DeltaTablePreview> {
        let unity = self.hydrate_unity(unity).await?;
        fluree_db_delta::describe_unity_table(&unity, table)
            .await
            .map(DeltaTablePreview::from)
            .map_err(config_error)
    }

    /// Read `table`'s log with the credentials Unity issues for it: the same
    /// steps a query's first touch of the table takes. A table that cannot be
    /// read is a report, not an error; a request that cannot be made is.
    pub async fn verify_delta_unity_table(
        &self,
        unity: &UnityConfig,
        io: &DeltaIoConfig,
        table: &str,
    ) -> Result<DeltaTableAccess> {
        let unity = self.hydrate_unity(unity).await?;
        unity.validate().map_err(config_error)?;
        let io = io
            .hydrate(self.secret_resolver())
            .await
            .map_err(config_error)?;
        let full_name = unity.full_name(table).map_err(config_error)?;
        let read = async {
            let table =
                fluree_db_delta::DeltaTable::open_in_unity(&full_name, &unity, &full_name, &io)
                    .await?;
            let snapshot = table.snapshot(VersionSelector::Latest).await?;
            let files = snapshot.file_count(&[]).await?;
            Ok::<_, DeltaError>((table.location().to_string(), snapshot.version(), files))
        }
        .await;
        Ok(match read {
            Ok((location, version, files)) => DeltaTableAccess {
                full_name,
                readable: true,
                location: Some(location),
                version: Some(version),
                data_file_count: Some(files),
                error: None,
            },
            Err(DeltaError::Config(e)) => return Err(crate::ApiError::Config(e)),
            Err(e) => DeltaTableAccess {
                full_name,
                readable: false,
                location: None,
                version: None,
                data_file_count: None,
                error: Some(e.to_string()),
            },
        })
    }

    /// Generate an R2RML mapping from Unity's record of each table: declared
    /// primary keys become subjects and declared foreign keys joins; the rest
    /// is decided as for any generated mapping, and said in the diagnostics.
    pub async fn generate_delta_r2rml(
        &self,
        req: GenerateDeltaR2rmlRequest,
    ) -> Result<GenerateDeltaR2rmlResponse> {
        if req.tables.is_empty() {
            return Err(crate::ApiError::config(
                "generating a mapping requires at least one table",
            ));
        }
        let unity = self.hydrate_unity(&req.unity).await?;
        let overrides = keyed_overrides(&unity, &req.tables, &req.per_table_overrides)?;
        // `buffered` keeps the request's order, and with it the mapping's.
        let tables: Vec<TableDescription> = futures::stream::iter(req.tables.clone())
            .map(|name| {
                let unity = unity.clone();
                async move { fluree_db_delta::describe_unity_table(&unity, &name).await }
            })
            .buffered(DESCRIBE_CONCURRENCY)
            .try_collect()
            .await
            .map_err(config_error)?;
        generate_from(&tables, &req.base_namespace, &req.options, overrides)
    }

    /// Check a mapping against the tables a Delta source would read, placed as
    /// `config` places them, without registering anything. Columns come from
    /// each table's own log, so this is what a query will find.
    pub async fn validate_delta_r2rml(
        &self,
        config: &DeltaCreateConfig,
    ) -> Result<ValidateR2rmlResponse> {
        config.validate()?;
        let R2rmlMappingInput::Content(turtle) = &config.mapping else {
            return Err(crate::ApiError::config(
                "a mapping is validated from its content",
            ));
        };
        let compiled = match compile_for_validate(turtle) {
            Ok(compiled) => compiled,
            Err(response) => return Ok(response),
        };
        let table_names: BTreeSet<String> = compiled
            .triples_maps
            .values()
            .filter_map(|tm| tm.table_name())
            .map(str::to_string)
            .collect();
        let mut diagnostics = Vec::new();
        if let Err(e) = super::r2rml::reject_sql_queries(&compiled) {
            diagnostics.push(Diagnostic {
                severity: Severity::Error,
                code: DiagCode::CompileError,
                table: None,
                column: None,
                message: e.to_string(),
            });
        }

        let gs_config = config.to_gs_config("");
        let io = gs_config
            .io
            .hydrate(self.secret_resolver())
            .await
            .map_err(config_error)?;
        let unity = match &gs_config.unity {
            Some(unity) => Some(self.hydrate_unity(unity).await?),
            None => None,
        };
        let mut schemas: HashMap<String, Vec<LiveColumn>> = HashMap::new();
        let mut unread: HashMap<String, String> = HashMap::new();
        for name in &table_names {
            let read = async {
                let table = open_placed(&gs_config, unity.as_ref(), &io, name).await?;
                let snapshot = table.snapshot(VersionSelector::Latest).await?;
                Ok::<_, DeltaError>(snapshot.columns())
            }
            .await;
            match read {
                Ok(columns) => {
                    let live = columns
                        .into_iter()
                        .map(|(name, field_type, nullable)| LiveColumn {
                            name,
                            field_type,
                            required: !nullable,
                            null_fraction: None,
                        })
                        .collect();
                    schemas.insert(name.clone(), live);
                }
                Err(DeltaError::Config(e)) => return Err(crate::ApiError::Config(e)),
                Err(e) => {
                    unread.insert(name.clone(), e.to_string());
                }
            }
        }
        diagnostics.extend(cross_check_live(
            &compiled,
            &schemas,
            &unread,
            DELTA_WORDING,
        ));
        diagnostics.extend(unmappable_columns(&compiled, &schemas));

        Ok(ValidateR2rmlResponse {
            compiled_ok: true,
            triples_map_count: compiled.len(),
            table_names: table_names.into_iter().collect(),
            diagnostics,
        })
    }
}

/// A mapped column of a type the reader cannot carry fails every query that
/// touches its table, so it is an error here and not a note.
fn unmappable_columns(
    compiled: &fluree_db_r2rml::mapping::CompiledR2rmlMapping,
    schemas: &HashMap<String, Vec<LiveColumn>>,
) -> Vec<Diagnostic> {
    let mut found = BTreeSet::new();
    for tm in compiled.triples_maps.values() {
        let Some(table) = tm.table_name() else {
            continue;
        };
        let Some(columns) = schemas.get(table) else {
            continue;
        };
        for wanted in tm.referenced_columns() {
            let unmappable = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(wanted))
                .is_some_and(|c| c.field_type.is_none());
            if unmappable {
                found.insert((table.to_string(), wanted.to_string()));
            }
        }
    }
    found
        .into_iter()
        .map(|(table, column)| {
            let message = format!(
                "Column '{column}' of '{table}' has a type a mapping cannot address (nested or \
                 semi-structured); leave it out of the mapping."
            );
            Diagnostic::new(
                Severity::Error,
                DiagCode::NestedColumnSkipped,
                table,
                Some(column),
                message,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_delta::DescribedColumn;
    use fluree_db_r2rml::loader::R2rmlLoader;
    use fluree_db_tabular::FieldType;

    const BASE: &str = "https://example.org/sales#";

    fn column(position: i32, name: &str, text: &str, ty: Option<FieldType>) -> DescribedColumn {
        DescribedColumn {
            name: name.to_string(),
            position,
            type_text: text.to_string(),
            field_type: ty,
            nullable: true,
            comment: None,
            masked: false,
        }
    }

    fn required(mut column: DescribedColumn) -> DescribedColumn {
        column.nullable = false;
        column
    }

    fn table(full_name: &str, columns: Vec<DescribedColumn>, pk: &[&str]) -> TableDescription {
        TableDescription {
            full_name: full_name.to_string(),
            kind: "MANAGED".to_string(),
            format: Some("DELTA".to_string()),
            location: Some("s3://bucket/t".to_string()),
            comment: None,
            access_rule: None,
            unreadable: None,
            columns,
            primary_key: pk.iter().map(|c| (*c).to_string()).collect(),
            foreign_keys: Vec::new(),
        }
    }

    fn sales() -> Vec<TableDescription> {
        let customers = table(
            "main.sales.customers",
            vec![
                required(column(0, "customer_id", "bigint", Some(FieldType::Int64))),
                column(1, "name", "string", Some(FieldType::String)),
            ],
            &["customer_id"],
        );
        let mut orders = table(
            "main.sales.orders",
            vec![
                required(column(0, "order_id", "bigint", Some(FieldType::Int64))),
                required(column(1, "line", "int", Some(FieldType::Int32))),
                // Not null, yet no part of the declared key.
                required(column(2, "buyer", "bigint", Some(FieldType::Int64))),
                column(
                    3,
                    "total",
                    "decimal(12,2)",
                    Some(FieldType::Decimal {
                        precision: 12,
                        scale: 2,
                    }),
                ),
                column(4, "extras", "variant", None),
            ],
            &["order_id", "line"],
        );
        orders.foreign_keys = vec![DeclaredForeignKey {
            name: "fk".to_string(),
            columns: vec!["buyer".to_string()],
            parent_table: "main.sales.customers".to_string(),
            parent_columns: vec!["customer_id".to_string()],
        }];
        vec![orders, customers]
    }

    fn generate(tables: &[TableDescription]) -> GenerateDeltaR2rmlResponse {
        generate_from(tables, BASE, &GenerateOptions::default(), HashMap::new()).unwrap()
    }

    #[test]
    fn a_generated_mapping_uses_the_names_and_keys_unity_declares() {
        let out = generate(&sales());
        assert_eq!(out.tables, ["main.sales.orders", "main.sales.customers"]);

        let orders = &out.structured.table_mappings[0];
        // The name `delta map` places through Unity, with no defaults needed.
        assert_eq!(orders.table_name, "main.sales.orders");
        assert!(
            orders
                .subject_template
                .ends_with("/orders/{order_id}/{line}"),
            "{}",
            orders.subject_template
        );
        // `buyer` and `customer_id` share no name: only the declaration joins them.
        let join = orders
            .columns
            .iter()
            .find_map(|c| c.foreign_key.as_ref())
            .expect("the declared key is a join");
        assert_eq!(
            (
                join.target_table.as_str(),
                join.child_column.as_str(),
                join.parent_column.as_str()
            ),
            ("main.sales.customers", "buyer", "customer_id")
        );
        let total = orders
            .columns
            .iter()
            .find(|c| c.column_name == "total")
            .unwrap();
        assert_eq!(total.datatype.as_deref(), Some("xsd:decimal"));
        // The notes speak of this catalog, and of the column's own type.
        let notes: Vec<_> = out.diagnostics.iter().map(|d| d.message.as_str()).collect();
        assert!(notes
            .iter()
            .any(|m| m.contains("from the primary key Unity Catalog declares")));
        assert!(notes
            .iter()
            .any(|m| m.contains("'extras' (variant) is not a flat scalar")));
        assert!(!notes.iter().any(|m| m.contains("Iceberg")), "{notes:?}");

        let compiled = R2rmlLoader::from_turtle(&out.turtle)
            .unwrap()
            .compile()
            .unwrap();
        assert_eq!(compiled.len(), 2);
        let mut names = compiled.table_names();
        names.sort_unstable();
        assert_eq!(names, ["main.sales.customers", "main.sales.orders"]);
    }

    #[test]
    fn without_a_declared_key_nullability_decides_the_subject() {
        let mut tables = sales();
        tables[1].primary_key.clear();
        let out = generate(&tables);
        let customers = &out.structured.table_mappings[1];
        // `name` may be null, and a null in a subject template drops the row.
        assert!(
            customers
                .subject_template
                .ends_with("/customers/{customer_id}"),
            "{}",
            customers.subject_template
        );
        assert!(out.diagnostics.iter().any(|d| {
            d.code == DiagCode::SubjectKeySynthesized
                && d.table.as_deref() == Some("main.sales.customers")
        }));
        // The table that kept its declared key needs no such note.
        assert!(!out.diagnostics.iter().any(|d| {
            d.code == DiagCode::SubjectKeySynthesized
                && d.table.as_deref() == Some("main.sales.orders")
        }));
    }

    #[test]
    fn a_column_no_mapping_can_address_is_left_out_and_said() {
        let out = generate(&sales());
        assert!(!out.turtle.contains("extras"));
        assert!(out.diagnostics.iter().any(|d| {
            d.code == DiagCode::NestedColumnSkipped && d.column.as_deref() == Some("extras")
        }));
    }

    #[test]
    fn what_is_not_a_readable_delta_table_is_not_mapped() {
        let mut tables = sales();
        tables[1].unreadable = Some("is a VIEW, not a Delta table".to_string());
        let error = generate_from(&tables, BASE, &GenerateOptions::default(), HashMap::new())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("'main.sales.customers' is a VIEW"),
            "{error}"
        );
    }

    #[test]
    fn an_override_is_given_by_the_tables_name_as_requested() {
        let unity = UnityConfig {
            uri: "https://workspace.example.com".to_string(),
            auth: fluree_db_iceberg::auth::AuthConfig::Bearer {
                token: fluree_db_iceberg::ConfigValue::literal("t"),
            },
            catalog: Some("main".to_string()),
            schema: Some("sales".to_string()),
        };
        let tables = vec!["orders".to_string(), "main.sales.customers".to_string()];
        let class = |name: &str| TableOverride {
            class_name: Some(name.to_string()),
            ..Default::default()
        };
        let given = HashMap::from([
            ("orders".to_string(), class("Purchase")),
            ("main.sales.customers".to_string(), class("Buyer")),
        ]);
        let keyed = keyed_overrides(&unity, &tables, &given).unwrap();
        let out = generate_from(&sales(), BASE, &GenerateOptions::default(), keyed).unwrap();
        let classes: Vec<_> = out
            .structured
            .table_mappings
            .iter()
            .map(|tm| tm.class_iri.as_str())
            .collect();
        assert_eq!(classes, [format!("{BASE}Purchase"), format!("{BASE}Buyer")]);

        let stray = HashMap::from([("returns".to_string(), class("Return"))]);
        let error = keyed_overrides(&unity, &tables, &stray)
            .unwrap_err()
            .to_string();
        assert!(error.contains("'returns'"), "{error}");
    }

    #[test]
    fn a_preview_says_what_a_mapping_would_make_of_each_column() {
        let preview = DeltaTablePreview::from(sales().remove(0));
        let seen: Vec<_> = preview
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    c.xsd_type.as_deref(),
                    c.mappable,
                    c.nullable,
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                ("order_id", Some("xsd:integer"), true, false),
                ("line", Some("xsd:integer"), true, false),
                ("buyer", Some("xsd:integer"), true, false),
                ("total", Some("xsd:decimal"), true, true),
                ("extras", None, false, true),
            ]
        );
        let wire = serde_json::to_value(&preview).unwrap();
        assert_eq!(wire["primary_key"], serde_json::json!(["order_id", "line"]));
        assert_eq!(
            wire["foreign_keys"][0]["parent_table"],
            "main.sales.customers"
        );
        assert_eq!(wire["columns"][4]["type_text"], "variant");
    }

    fn live(name: &str, field_type: Option<FieldType>, required: bool) -> LiveColumn {
        LiveColumn {
            name: name.to_string(),
            field_type,
            required,
            null_fraction: None,
        }
    }

    #[test]
    fn a_mapping_is_checked_against_the_columns_a_query_will_find() {
        let turtle = r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <https://example.org/> .
            ex:Orders a rr:TriplesMap ;
              rr:logicalTable [ rr:tableName "main.sales.orders" ] ;
              rr:subjectMap [ rr:template "https://example.org/o/{order_id}" ] ;
              rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "Total" ] ] ;
              rr:predicateObjectMap [ rr:predicate ex:extras ; rr:objectMap [ rr:column "extras" ] ] ;
              rr:predicateObjectMap [ rr:predicate ex:gone ; rr:objectMap [ rr:column "gone" ] ] .
            ex:Missing a rr:TriplesMap ;
              rr:logicalTable [ rr:tableName "main.sales.returns" ] ;
              rr:subjectMap [ rr:template "https://example.org/r/{id}" ] .
        "#;
        let compiled = compile_for_validate(turtle).unwrap();
        let schemas = HashMap::from([(
            "main.sales.orders".to_string(),
            vec![
                live("order_id", Some(FieldType::Int64), true),
                live("total", Some(FieldType::Float64), false),
                live("extras", None, false),
            ],
        )]);
        let unread = HashMap::from([(
            "main.sales.returns".to_string(),
            "Unity Catalog, table 'main.sales.returns': not found (404)".to_string(),
        )]);
        let mut found = cross_check_live(&compiled, &schemas, &unread, DELTA_WORDING);
        found.extend(unmappable_columns(&compiled, &schemas));
        let of = |code: DiagCode| -> Vec<&Diagnostic> {
            found.iter().filter(|d| d.code == code).collect()
        };

        let casing = of(DiagCode::CasingMismatch);
        assert_eq!(casing.len(), 1);
        assert!(
            casing[0].message.contains("resolves names ignoring case"),
            "{}",
            casing[0].message
        );
        assert!(!casing[0].message.contains("Iceberg"));

        assert_eq!(
            of(DiagCode::ColumnNotFound)[0].column.as_deref(),
            Some("gone")
        );

        let missing = of(DiagCode::TableNotFound);
        assert!(missing[0]
            .message
            .contains("the table could not be read: Unity Catalog"));

        let unmappable = of(DiagCode::NestedColumnSkipped);
        assert_eq!(unmappable.len(), 1);
        assert_eq!(unmappable[0].column.as_deref(), Some("extras"));
        assert_eq!(unmappable[0].severity, Severity::Error);
        // The subject key is `required`, so nothing is said of it.
        assert!(of(DiagCode::NoSafeSubjectKey).is_empty());
    }
}
