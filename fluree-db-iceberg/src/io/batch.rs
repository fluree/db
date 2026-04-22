//! Columnar batch format for Iceberg data reading.
//!
//! This module re-exports the core tabular types from `fluree_db_tabular`
//! and adds Iceberg-specific extensions (e.g., parsing Iceberg type strings).

pub use fluree_db_tabular::batch::*;

/// Extension trait for parsing `FieldType` from Iceberg type strings.
pub trait IcebergFieldTypeExt {
    /// Parse from Iceberg type string (e.g., "boolean", "int", "long", "decimal(10, 2)").
    fn from_iceberg_type(type_str: &str) -> Option<FieldType>;
}

impl IcebergFieldTypeExt for FieldType {
    fn from_iceberg_type(type_str: &str) -> Option<Self> {
        match type_str {
            "boolean" => Some(Self::Boolean),
            "int" => Some(Self::Int32),
            "long" => Some(Self::Int64),
            "float" => Some(Self::Float32),
            "double" => Some(Self::Float64),
            "string" => Some(Self::String),
            "binary" | "fixed" => Some(Self::Bytes),
            "date" => Some(Self::Date),
            "timestamp" => Some(Self::Timestamp),
            "timestamptz" => Some(Self::TimestampTz),
            s if s.starts_with("decimal") => {
                // Parse decimal(precision, scale)
                let inner = s.trim_start_matches("decimal(").trim_end_matches(')');
                let parts: Vec<&str> = inner.split(',').collect();
                if parts.len() == 2 {
                    let precision = parts[0].trim().parse().ok()?;
                    let scale = parts[1].trim().parse().ok()?;
                    Some(Self::Decimal { precision, scale })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_parsing() {
        assert_eq!(
            FieldType::from_iceberg_type("boolean"),
            Some(FieldType::Boolean)
        );
        assert_eq!(FieldType::from_iceberg_type("int"), Some(FieldType::Int32));
        assert_eq!(FieldType::from_iceberg_type("long"), Some(FieldType::Int64));
        assert_eq!(
            FieldType::from_iceberg_type("string"),
            Some(FieldType::String)
        );
        assert_eq!(FieldType::from_iceberg_type("date"), Some(FieldType::Date));
        assert_eq!(
            FieldType::from_iceberg_type("timestamp"),
            Some(FieldType::Timestamp)
        );
        assert_eq!(
            FieldType::from_iceberg_type("decimal(10, 2)"),
            Some(FieldType::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert_eq!(FieldType::from_iceberg_type("unknown"), None);
    }
}
