//! Test fixtures: the 16 `ENTERPRISE_DEMO.DW` tables encoded as emitter INPUT.
//!
//! CRITICAL: these fixtures encode only SCHEMA + STATS — never the join answers.
//! The `identifier_field_ids` are the surrogate `*_KEY` PKs; the min/max ranges
//! are plausible integer surrogate spaces chosen so range-containment RESOLVES
//! the resolvable FKs and the role-renamed employee FKs fail on NAME. The
//! emitter must INDEPENDENTLY infer every FK from name∧type∧range; the tests then
//! check the inferred graph matches `enterprise.ttl`'s hand-written joins.
//!
//! Column types are derived from `enterprise.ttl`'s `rr:datatype`s (integer →
//! `Int64`, double → `Float64`, decimal → `Decimal`, date → `Date`, timestamp →
//! `Timestamp`, boolean → `Boolean`, untyped → `String`). PKs are field id 1.

#![cfg(test)]

use fluree_db_tabular::FieldType;

use crate::emit::input::{EmitColumn, EmitColumnStats, EmitTableSchema, TypedBound};

/// Wide surrogate space shared by every dimension PK (child FK ranges ⊆ this).
const DIM_MAX: i64 = 100_000;
/// Wider surrogate space for fact PKs (fact rows outnumber dimension rows).
const FACT_MAX: i64 = 500_000;

/// A non-key scalar column with default (bounds-free) stats.
fn s(field_id: i32, name: &str, ft: FieldType) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: iceberg_type_str(ft),
        field_type: ft,
        required: false,
        nested: false,
        doc: None,
        stats: EmitColumnStats::default(),
    }
}

/// An integer key column (PK or FK) with an explicit `[min, max]` surrogate
/// range. `required` marks NOT-NULL (all surrogate PKs are required).
fn k(field_id: i32, name: &str, min: i64, max: i64, required: bool) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: "long".to_string(),
        field_type: FieldType::Int64,
        required,
        nested: false,
        doc: None,
        stats: EmitColumnStats {
            null_fraction: if required { Some(0.0) } else { None },
            min: Some(TypedBound::Int(min)),
            max: Some(TypedBound::Int(max)),
        },
    }
}

fn iceberg_type_str(ft: FieldType) -> String {
    match ft {
        FieldType::Boolean => "boolean".to_string(),
        FieldType::Int32 => "int".to_string(),
        FieldType::Int64 => "long".to_string(),
        FieldType::Float32 => "float".to_string(),
        FieldType::Float64 => "double".to_string(),
        FieldType::String => "string".to_string(),
        FieldType::Bytes => "binary".to_string(),
        FieldType::Date => "date".to_string(),
        FieldType::Timestamp => "timestamp".to_string(),
        FieldType::TimestampTz => "timestamptz".to_string(),
        FieldType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
    }
}

fn table(namespace: &str, name: &str, columns: Vec<EmitColumn>) -> EmitTableSchema {
    EmitTableSchema {
        namespace: namespace.to_string(),
        name: name.to_string(),
        columns,
        identifier_field_ids: vec![1], // PK is always field id 1 in these fixtures
    }
}

use FieldType::{Boolean as B, Date as Dt, Float64 as Db, String as St, Timestamp as Ts};
const DEC: FieldType = FieldType::Decimal {
    precision: 18,
    scale: 2,
};

/// All 16 `ENTERPRISE_DEMO.DW` tables, 8 dimensions then 8 facts.
pub fn enterprise_dw_tables() -> Vec<EmitTableSchema> {
    vec![
        // ===================== DIMENSIONS =====================
        table(
            "DW",
            "DIM_DATE",
            vec![
                k(1, "DATE_KEY", 1, DIM_MAX, true),
                s(2, "DATE", Dt),
                s(3, "DAY_OF_WEEK", FieldType::Int64),
                s(4, "DAY_NAME", St),
                s(5, "DAY_OF_MONTH", FieldType::Int64),
                s(6, "WEEK_OF_YEAR", FieldType::Int64),
                s(7, "MONTH_NUM", FieldType::Int64),
                s(8, "MONTH_NAME", St),
                s(9, "QUARTER_NUM", FieldType::Int64),
                s(10, "YEAR_NUM", FieldType::Int64),
                s(11, "IS_WEEKEND", B),
            ],
        ),
        table(
            "DW",
            "DIM_GEOGRAPHY",
            vec![
                k(1, "GEOGRAPHY_KEY", 1, DIM_MAX, true),
                s(2, "COUNTRY_CODE", St),
                s(3, "COUNTRY", St),
                s(4, "REGION", St),
                s(5, "STATE_PROVINCE", St),
                s(6, "CITY", St),
                s(7, "POSTAL_CODE", St),
                s(8, "LATITUDE", Db),
                s(9, "LONGITUDE", Db),
            ],
        ),
        table(
            "DW",
            "DIM_SUPPLIER",
            vec![
                k(1, "SUPPLIER_KEY", 1, DIM_MAX, true),
                s(2, "SUPPLIER_ID", St),
                s(3, "SUPPLIER_NAME", St),
                s(4, "CONTACT_EMAIL", St),
                s(5, "LEAD_TIME_DAYS", FieldType::Int64),
                s(6, "RATING", Db),
                k(7, "GEOGRAPHY_KEY", 1, DIM_MAX, false),
            ],
        ),
        table(
            "DW",
            "DIM_ACCOUNT",
            vec![
                k(1, "ACCOUNT_KEY", 1, DIM_MAX, true),
                s(2, "ACCOUNT_ID", St),
                s(3, "ACCOUNT_NAME", St),
                s(4, "INDUSTRY", St),
                s(5, "EMPLOYEE_COUNT", FieldType::Int64),
                s(6, "ANNUAL_REVENUE", Db),
                s(7, "TIER", St),
                s(8, "CREATED_DATE", Dt),
                k(9, "GEOGRAPHY_KEY", 1, DIM_MAX, false),
            ],
        ),
        table(
            "DW",
            "DIM_EMPLOYEE",
            vec![
                k(1, "EMPLOYEE_KEY", 1, DIM_MAX, true),
                s(2, "EMPLOYEE_ID", St),
                s(3, "FULL_NAME", St),
                s(4, "EMAIL", St),
                s(5, "ROLE", St),
                s(6, "DEPARTMENT", St),
                s(7, "HIRE_DATE", Dt),
                s(8, "IS_ACTIVE", B),
                k(9, "STORE_KEY", 1, DIM_MAX, false),
                k(10, "MANAGER_KEY", 1, DIM_MAX, false), // role-renamed → EMPLOYEE_KEY (unresolvable)
            ],
        ),
        table(
            "DW",
            "DIM_STORE",
            vec![
                k(1, "STORE_KEY", 1, DIM_MAX, true),
                s(2, "STORE_ID", St),
                s(3, "STORE_NAME", St),
                s(4, "CHANNEL", St),
                s(5, "STORE_TYPE", St),
                s(6, "OPEN_DATE", Dt),
                k(7, "GEOGRAPHY_KEY", 1, DIM_MAX, false),
                k(8, "REGION_MANAGER_KEY", 1, DIM_MAX, false), // role-renamed → EMPLOYEE_KEY
            ],
        ),
        table(
            "DW",
            "DIM_CUSTOMER",
            vec![
                k(1, "CUSTOMER_KEY", 1, DIM_MAX, true),
                s(2, "CUSTOMER_ID", St),
                s(3, "FULL_NAME", St),
                s(4, "EMAIL", St),
                s(5, "PHONE", St),
                s(6, "SEGMENT", St),
                s(7, "GENDER", St),
                s(8, "BIRTH_YEAR", FieldType::Int64),
                s(9, "SIGNUP_DATE", Dt),
                s(10, "SCD_VALID_FROM", Dt),
                s(11, "SCD_VALID_TO", Dt),
                s(12, "IS_CURRENT", B),
                k(13, "GEOGRAPHY_KEY", 1, DIM_MAX, false),
                k(14, "ACCOUNT_KEY", 1, DIM_MAX, false),
            ],
        ),
        table(
            "DW",
            "DIM_PRODUCT",
            vec![
                k(1, "PRODUCT_KEY", 1, DIM_MAX, true),
                s(2, "PRODUCT_ID", St),
                s(3, "PRODUCT_NAME", St),
                s(4, "BRAND", St),
                s(5, "CATEGORY", St),
                s(6, "SUBCATEGORY", St),
                s(7, "DEPARTMENT", St),
                s(8, "UNIT_COST", Db),
                s(9, "LIST_PRICE", Db),
                s(10, "IS_CURRENT", B),
                k(11, "SUPPLIER_KEY", 1, DIM_MAX, false),
            ],
        ),
        // ======================= FACTS ========================
        table(
            "DW",
            "FACT_ORDER",
            vec![
                k(1, "ORDER_KEY", 1, FACT_MAX, true),
                s(2, "ORDER_ID", St),
                s(3, "ORDER_STATUS", St),
                s(4, "ORDER_CHANNEL", St),
                s(5, "ORDER_TOTAL", Db),
                s(6, "CURRENCY", St),
                s(7, "ORDER_DATE", Dt),
                k(8, "CUSTOMER_KEY", 1, DIM_MAX, false),
                k(9, "ACCOUNT_KEY", 1, DIM_MAX, false),
                k(10, "STORE_KEY", 1, DIM_MAX, false),
                k(11, "SALES_REP_KEY", 1, DIM_MAX, false), // role-renamed → EMPLOYEE_KEY
                k(12, "ORDER_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_ORDER_LINE",
            vec![
                k(1, "ORDER_LINE_KEY", 1, FACT_MAX, true),
                s(2, "LINE_NUMBER", FieldType::Int64),
                s(3, "QUANTITY", FieldType::Int64),
                s(4, "UNIT_PRICE", Db),
                s(5, "DISCOUNT_PCT", Db),
                s(6, "EXTENDED_AMOUNT", Db),
                s(7, "ORDER_DATE", Dt),
                k(8, "ORDER_KEY", 1, FACT_MAX, false), // hub → FACT_ORDER.ORDER_KEY
                k(9, "PRODUCT_KEY", 1, DIM_MAX, false),
            ],
        ),
        table(
            "DW",
            "FACT_INVENTORY_SNAPSHOT",
            vec![
                k(1, "INVENTORY_KEY", 1, FACT_MAX, true),
                s(2, "SNAPSHOT_DATE", Dt),
                s(3, "ON_HAND_QTY", FieldType::Int64),
                s(4, "RESERVED_QTY", FieldType::Int64),
                s(5, "REORDER_POINT", FieldType::Int64),
                s(6, "UNITS_ON_ORDER", FieldType::Int64),
                k(7, "PRODUCT_KEY", 1, DIM_MAX, false),
                k(8, "STORE_KEY", 1, DIM_MAX, false),
                k(9, "SNAPSHOT_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_SHIPMENT",
            vec![
                k(1, "SHIPMENT_KEY", 1, FACT_MAX, true),
                s(2, "SHIPMENT_ID", St),
                s(3, "SHIP_DATE", Dt),
                s(4, "CARRIER", St),
                s(5, "SHIP_METHOD", St),
                s(6, "SHIP_STATUS", St),
                s(7, "TRACKING_NUMBER", St),
                s(8, "SHIP_COST", Db),
                k(9, "ORDER_KEY", 1, FACT_MAX, false), // hub → FACT_ORDER.ORDER_KEY
                k(10, "DEST_GEOGRAPHY_KEY", 1, DIM_MAX, false), // suffix → GEOGRAPHY_KEY
                k(11, "SHIP_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_PAYMENT",
            vec![
                k(1, "PAYMENT_KEY", 1, FACT_MAX, true),
                s(2, "PAYMENT_ID", St),
                s(3, "PAYMENT_DATE", Dt),
                s(4, "TENDER_TYPE", St),
                s(5, "AMOUNT", Db),
                s(6, "CURRENCY", St),
                s(7, "PAYMENT_STATUS", St),
                k(8, "ORDER_KEY", 1, FACT_MAX, false), // hub → FACT_ORDER.ORDER_KEY
                k(9, "CUSTOMER_KEY", 1, DIM_MAX, false),
                k(10, "PAYMENT_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_GL_JOURNAL",
            vec![
                k(1, "JOURNAL_KEY", 1, FACT_MAX, true),
                s(2, "JOURNAL_ID", St),
                s(3, "POSTING_DATE", Dt),
                s(4, "GL_ACCOUNT_CODE", FieldType::Int64),
                s(5, "GL_ACCOUNT_NAME", St),
                s(6, "COST_CENTER", St),
                s(7, "DEBIT_AMOUNT", DEC),
                s(8, "CREDIT_AMOUNT", DEC),
                s(9, "CURRENCY", St),
                s(10, "SOURCE_MODULE", St),
                k(11, "POSTING_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_WEB_EVENT",
            vec![
                k(1, "EVENT_KEY", 1, FACT_MAX, true),
                s(2, "EVENT_ID", St),
                s(3, "SESSION_ID", FieldType::Int64), // integer *_ID, no matching PK → UnresolvedFkCandidate
                s(4, "EVENT_DATE", Dt),
                s(5, "EVENT_TS", Ts),
                s(6, "EVENT_TYPE", St),
                s(7, "PAGE_URL", St),
                s(8, "DEVICE_TYPE", St),
                s(9, "BROWSER", St),
                s(10, "REFERRER", St),
                k(11, "CUSTOMER_KEY", 1, DIM_MAX, false),
                k(12, "PRODUCT_KEY", 1, DIM_MAX, false),
                k(13, "EVENT_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
        table(
            "DW",
            "FACT_SUPPORT_TICKET",
            vec![
                k(1, "TICKET_KEY", 1, FACT_MAX, true),
                s(2, "TICKET_ID", St),
                s(3, "OPEN_DATE", Dt),
                s(4, "CLOSE_DATE", Dt),
                s(5, "STATUS", St),
                s(6, "PRIORITY", St),
                s(7, "CATEGORY", St),
                s(8, "CSAT_SCORE", FieldType::Int64),
                s(9, "RESOLUTION_HOURS", Db),
                k(10, "CUSTOMER_KEY", 1, DIM_MAX, false),
                k(11, "PRODUCT_KEY", 1, DIM_MAX, false),
                k(12, "AGENT_KEY", 1, DIM_MAX, false), // role-renamed → EMPLOYEE_KEY
                k(13, "OPEN_DATE_KEY", 1, DIM_MAX, false), // suffix → DATE_KEY
            ],
        ),
    ]
}
