//! Geospatial function implementations
//!
//! Implements OGC GeoSPARQL functions: geof:distance

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_core::{geo, FlakeValue};

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_geof_distance<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "geof:distance")?;
    let v1 = args[0].eval_to_comparable(row, ctx)?;
    let v2 = args[1].eval_to_comparable(row, ctx)?;

    match (&v1, &v2) {
        (None, _) | (_, None) => Ok(None), // unbound variable
        _ => {
            let coords1 = extract_geo_coords(&v1);
            let coords2 = extract_geo_coords(&v2);

            match (coords1, coords2) {
                (Some((lat1, lng1)), Some((lat2, lng2))) => {
                    let distance = geo::haversine_distance(lat1, lng1, lat2, lng2);
                    Ok(Some(ComparableValue::Double(distance)))
                }
                _ => Err(QueryError::InvalidFilter(
                    "geof:distance requires point arguments".to_string(),
                )),
            }
        }
    }
}

/// Extract (lat, lng) from a ComparableValue
///
/// Supports:
/// - GeoPoint: direct extraction from packed representation
/// - String/TypedLiteral: parse WKT POINT format
fn extract_geo_coords(val: &Option<ComparableValue>) -> Option<(f64, f64)> {
    match val {
        Some(ComparableValue::GeoPoint(bits)) => Some((bits.lat(), bits.lng())),
        Some(ComparableValue::String(s)) => geo::try_extract_point(s),
        Some(ComparableValue::TypedLiteral { val, .. }) => match val {
            FlakeValue::GeoPoint(bits) => Some((bits.lat(), bits.lng())),
            FlakeValue::String(s) => geo::try_extract_point(s),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::{Batch, Binding};
    use crate::var_registry::VarId;
    use fluree_db_core::{GeoPointBits, Sid};
    use fluree_vocab::namespaces::OGC_GEO;
    use std::sync::Arc;

    #[test]
    fn test_geof_distance_with_geopoints() {
        // Paris: 48.8566°N, 2.3522°E
        // London: 51.5074°N, 0.1278°W
        let paris = GeoPointBits::new(48.8566, 2.3522).unwrap();
        let london = GeoPointBits::new(51.5074, -0.1278).unwrap();

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let col0 = vec![Binding::lit(
            FlakeValue::GeoPoint(paris),
            Sid::new(OGC_GEO, "wktLiteral"),
        )];
        let col1 = vec![Binding::lit(
            FlakeValue::GeoPoint(london),
            Sid::new(OGC_GEO, "wktLiteral"),
        )];
        let batch = Batch::new(schema, vec![col0, col1]).unwrap();
        let row = batch.row_view(0).unwrap();

        let result = eval_geof_distance::<_>(
            &[Expression::Var(VarId(0)), Expression::Var(VarId(1))],
            &row,
            None,
        )
        .unwrap();

        // Distance should be approximately 343 km
        if let Some(ComparableValue::Double(d)) = result {
            assert!(
                (d - 343_500.0).abs() < 5_000.0,
                "Expected ~343 km, got {d} m"
            );
        } else {
            panic!("Expected Double result, got {result:?}");
        }
    }

    #[test]
    fn test_geof_distance_with_wkt_strings() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let col0 = vec![Binding::lit(
            FlakeValue::String("POINT(2.3522 48.8566)".to_string()), // Paris (lng, lat)
            Sid::new(2, "string"),
        )];
        let col1 = vec![Binding::lit(
            FlakeValue::String("POINT(-0.1278 51.5074)".to_string()), // London (lng, lat)
            Sid::new(2, "string"),
        )];
        let batch = Batch::new(schema, vec![col0, col1]).unwrap();
        let row = batch.row_view(0).unwrap();

        let result = eval_geof_distance::<_>(
            &[Expression::Var(VarId(0)), Expression::Var(VarId(1))],
            &row,
            None,
        )
        .unwrap();

        // Distance should be approximately 343 km
        if let Some(ComparableValue::Double(d)) = result {
            assert!(
                (d - 343_500.0).abs() < 5_000.0,
                "Expected ~343 km, got {d} m"
            );
        } else {
            panic!("Expected Double result, got {result:?}");
        }
    }

    #[test]
    fn test_geof_distance_with_non_point_returns_error() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let col0 = vec![Binding::lit(
            FlakeValue::String("LINESTRING(0 0, 1 1, 2 2)".to_string()),
            Sid::new(2, "string"),
        )];
        let col1 = vec![Binding::lit(
            FlakeValue::String("POINT(2.3522 48.8566)".to_string()),
            Sid::new(2, "string"),
        )];
        let batch = Batch::new(schema, vec![col0, col1]).unwrap();
        let row = batch.row_view(0).unwrap();

        let result = eval_geof_distance::<_>(
            &[Expression::Var(VarId(0)), Expression::Var(VarId(1))],
            &row,
            None,
        );

        assert!(result.is_err());
    }
}
