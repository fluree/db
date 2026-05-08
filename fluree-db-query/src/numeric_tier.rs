//! Numeric type promotion ladder for SUM / AVG aggregates.
//!
//! W3C SPARQL §17.4.1.7 defines a four-tier numeric hierarchy used to type
//! the result of arithmetic and aggregate operations:
//!
//! ```text
//!   xsd:integer  →  xsd:decimal  →  xsd:float  →  xsd:double
//!     (tier 0)        (tier 1)      (tier 2)       (tier 3)
//! ```
//!
//! When an aggregate observes values of mixed numeric types, the result
//! promotes to the WIDEST tier seen across the group. Aggregate-specific
//! exceptions:
//!
//! - `SUM` returns the widest input tier directly.
//! - `AVG` returns the widest tier with one twist: an all-integer group
//!   widens to `xsd:decimal` (because `integer ÷ integer` is decimal in
//!   SPARQL division semantics).
//!
//! `FlakeValue` already preserves precision for `xsd:integer` (Long/BigInt)
//! and `xsd:decimal` (BigDecimal); `xsd:float` and `xsd:double` collapse to
//! `f64` (matching IEEE-754 runtime semantics where precision loss is
//! intentional). The accumulator accordingly carries an exact `BigDecimal`
//! sum while in the integer/decimal tiers and an `f64` sum once promoted to
//! float/double.

use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use fluree_db_core::{FlakeValue, Sid};
use fluree_vocab::xsd_names;

use crate::binding::Binding;

/// Position on the numeric promotion ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NumericTier {
    Integer,
    Decimal,
    Float,
    Double,
}

impl NumericTier {
    /// Map a `Binding` carrying a numeric literal to its tier. Returns
    /// `None` for non-numeric bindings (Unbound, Poisoned, strings, IRIs,
    /// `xsd:boolean`, NaN doubles, etc. — callers skip these inputs).
    pub(crate) fn of_binding(b: &Binding) -> Option<NumericTier> {
        match b {
            Binding::Lit { val, dtc, .. } => match val {
                FlakeValue::Long(_) | FlakeValue::BigInt(_) => {
                    Some(Self::from_int_dt(dtc.datatype()))
                }
                FlakeValue::Decimal(_) => Some(NumericTier::Decimal),
                FlakeValue::Double(d) if !d.is_nan() => {
                    Some(if dtc.datatype().name.as_ref() == xsd_names::FLOAT {
                        NumericTier::Float
                    } else {
                        NumericTier::Double
                    })
                }
                FlakeValue::Boolean(_) => Some(NumericTier::Integer),
                _ => None,
            },
            _ => None,
        }
    }

    /// Decide tier for an integer-stored value. The vast majority of
    /// integer-typed datatypes (xsd:long, xsd:int, xsd:short, ...) are
    /// promoted operands of `xsd:integer` per W3C SPARQL §17.4.1.7.
    fn from_int_dt(_dt: &Sid) -> NumericTier {
        // All integer subtypes promote to xsd:integer for arithmetic.
        // We don't look at the datatype name here because the SPARQL
        // promotion lattice treats them uniformly; we surface
        // `xsd:integer` on output regardless of input subtype.
        NumericTier::Integer
    }

    /// Return the widest of two tiers per the promotion lattice.
    pub(crate) fn widen(self, other: NumericTier) -> NumericTier {
        if self >= other {
            self
        } else {
            other
        }
    }

    /// The xsd: SID this tier renders as on aggregate output.
    fn output_sid(self) -> Sid {
        match self {
            NumericTier::Integer => Sid::xsd_integer(),
            NumericTier::Decimal => Sid::xsd_decimal(),
            NumericTier::Float => Sid::xsd_float(),
            NumericTier::Double => Sid::xsd_double(),
        }
    }
}

/// Type-aware sum accumulator that preserves precision while in the
/// integer/decimal tiers and folds to `f64` once promoted to float/double.
///
/// Uses `BigDecimal` for exact arithmetic in the integer+decimal tier band
/// — `BigDecimal` is internally an arbitrary-precision int plus a scale, so
/// it represents both `xsd:integer` and `xsd:decimal` values losslessly.
/// At the moment a float/double value is observed, the accumulated decimal
/// sum is folded into the `f64` accumulator (one-time precision loss,
/// matching the SPARQL spec's promotion semantics).
#[derive(Debug, Clone)]
pub(crate) struct NumericAccum {
    /// Widest tier observed so far. `None` until the first value is added.
    tier: Option<NumericTier>,
    /// Exact sum while `tier <= Decimal`. After promotion to Float/Double,
    /// this remains zero (its previous value has been folded into `f_sum`).
    d_sum: BigDecimal,
    /// Lossy sum once `tier >= Float`. Zero before promotion.
    f_sum: f64,
    /// Number of values observed (denominator for AVG).
    count: usize,
}

impl Default for NumericAccum {
    fn default() -> Self {
        Self {
            tier: None,
            d_sum: BigDecimal::zero(),
            f_sum: 0.0,
            count: 0,
        }
    }
}

impl NumericAccum {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Incorporate a binding into the accumulator. Bindings that aren't a
    /// recognized numeric tier (per `NumericTier::of_binding`) are silently
    /// ignored, matching SPARQL's "errors are skipped in aggregate input"
    /// semantics for SUM/AVG.
    pub(crate) fn add(&mut self, binding: &Binding) {
        let Some(in_tier) = NumericTier::of_binding(binding) else {
            return;
        };
        let new_tier = match self.tier {
            None => in_tier,
            Some(t) => t.widen(in_tier),
        };

        // Promote accumulator state if we're crossing into the f64 band.
        if matches!(new_tier, NumericTier::Float | NumericTier::Double)
            && !matches!(
                self.tier,
                Some(NumericTier::Float | NumericTier::Double)
            )
        {
            // Fold the exact decimal sum into f64 (one-time conversion).
            self.f_sum += self.d_sum.to_f64().unwrap_or(0.0);
            self.d_sum = BigDecimal::zero();
        }

        self.tier = Some(new_tier);
        self.count += 1;

        match (new_tier, binding) {
            (NumericTier::Integer | NumericTier::Decimal, Binding::Lit { val, .. }) => match val {
                FlakeValue::Long(n) => self.d_sum += BigDecimal::from(*n),
                FlakeValue::BigInt(b) => self.d_sum += BigDecimal::from(b.as_ref().clone()),
                FlakeValue::Decimal(d) => self.d_sum += d.as_ref().clone(),
                FlakeValue::Boolean(b) => self.d_sum += BigDecimal::from(i64::from(*b)),
                FlakeValue::Double(d) if !d.is_nan() => {
                    // Should not reach: if we see a Double, tier should have
                    // promoted to Float/Double in the block above. Defensive
                    // fallback to keep arithmetic correct.
                    self.f_sum += *d;
                }
                _ => {}
            },
            (NumericTier::Float | NumericTier::Double, Binding::Lit { val, .. }) => match val {
                FlakeValue::Long(n) => self.f_sum += *n as f64,
                FlakeValue::Boolean(b) => self.f_sum += f64::from(i64::from(*b) as i32),
                FlakeValue::BigInt(b) => {
                    self.f_sum += BigDecimal::from(b.as_ref().clone()).to_f64().unwrap_or(0.0);
                }
                FlakeValue::Decimal(d) => self.f_sum += d.as_ref().to_f64().unwrap_or(0.0),
                FlakeValue::Double(d) if !d.is_nan() => self.f_sum += *d,
                _ => {}
            },
            _ => {}
        }
    }

    /// Materialize the accumulator as a SUM result.
    ///
    /// Empty input returns `Binding::Unbound` for SPARQL conformance — the
    /// caller (e.g. `agg_sum`) decides whether to coerce to `0 xsd:integer`.
    pub(crate) fn finalize_sum(self) -> Binding {
        let Some(tier) = self.tier else {
            return Binding::Unbound;
        };
        let dt = tier.output_sid();
        match tier {
            NumericTier::Integer => {
                // Integer SUM stays in BigDecimal accumulator until output;
                // collapse to Long when it fits, else BigInt.
                let d = self.d_sum;
                if let Some(i) = d.to_i64().filter(|_| d.is_integer()) {
                    Binding::lit(FlakeValue::Long(i), dt)
                } else if d.is_integer() {
                    let (bi, _scale) = d.into_bigint_and_exponent();
                    Binding::lit(FlakeValue::BigInt(Box::new(bi)), dt)
                } else {
                    // Integer tier sums always integer-valued (no fractional
                    // part possible from integer inputs).
                    Binding::lit(FlakeValue::Decimal(Box::new(d)), Sid::xsd_decimal())
                }
            }
            NumericTier::Decimal => Binding::lit(FlakeValue::Decimal(Box::new(self.d_sum)), dt),
            NumericTier::Float | NumericTier::Double => {
                Binding::lit(FlakeValue::Double(self.f_sum), dt)
            }
        }
    }

    /// Materialize the accumulator as an AVG result.
    ///
    /// Empty input returns `0 xsd:integer` per W3C SPARQL test
    /// `agg-avg-03` (named "AVG with empty group (value defined to be 0)").
    /// All-integer groups promote to `xsd:decimal` because SPARQL's
    /// `integer ÷ integer` is decimal-typed.
    pub(crate) fn finalize_avg(self) -> Binding {
        if self.count == 0 {
            return Binding::lit(FlakeValue::Long(0), Sid::xsd_integer());
        }
        let tier = self.tier.expect("count > 0 implies tier set");
        let count = self.count as i64;
        match tier {
            NumericTier::Integer | NumericTier::Decimal => {
                let n = BigDecimal::from(count);
                // BigDecimal division on non-terminating quotients (e.g.
                // `127 / 3`) defaults to an arbitrary high-precision result
                // (~100 fractional digits). Cap to 20 fractional digits —
                // comparable to xsd:double's ~15-17 significant decimal
                // digits, with headroom — and normalize to strip trailing
                // zeros so terminating quotients render compactly
                // (`6 / 3` becomes `"2"`, not `"2.00000000000000000000"`).
                let q = self.d_sum / n;
                let bounded = q.with_scale_round(20, bigdecimal::RoundingMode::HalfEven);
                let normalized = bounded.normalized();
                Binding::lit(
                    FlakeValue::Decimal(Box::new(normalized)),
                    Sid::xsd_decimal(),
                )
            }
            NumericTier::Float | NumericTier::Double => {
                let avg = self.f_sum / count as f64;
                Binding::lit(FlakeValue::Double(avg), tier.output_sid())
            }
        }
    }
}

/// Build a deduplication key for a numeric binding.
#[allow(dead_code)] // Reserved for COUNT(DISTINCT *) and follow-up DISTINCT-aware fast paths. Two bindings yield the
/// same key iff their numeric VALUE is equal regardless of representation
/// (e.g. `xsd:integer 2` and `xsd:decimal 2.0` produce the same key, so
/// `SUM(DISTINCT)` treats them as one). This matches W3C semantics —
/// `DISTINCT` operates on RDF term equality, but for numeric values the
/// natural reading is value-based dedup once we've decided to type-promote.
///
/// Numeric strings produced by `BigDecimal::normalized()` are canonical
/// (trailing zeros stripped, scale minimized) so the string form is a
/// usable hash key.
pub(crate) fn numeric_dedup_key(binding: &Binding) -> Option<String> {
    let Binding::Lit { val, .. } = binding else {
        return None;
    };
    match val {
        FlakeValue::Long(n) => Some(format!("i:{n}")),
        FlakeValue::BigInt(b) => Some(format!("i:{b}")),
        FlakeValue::Boolean(b) => Some(format!("i:{}", i64::from(*b))),
        FlakeValue::Decimal(d) => {
            // Canonicalize to compare 1.0 == 1 == 1.00 by stripping trailing zeros.
            Some(format!("d:{}", d.normalized()))
        }
        FlakeValue::Double(d) if !d.is_nan() => {
            // Use the bit pattern so two identical f64 representations
            // dedup; 1.0E2 and 100.0 (same underlying f64) hash equal.
            Some(format!("f:{:016x}", d.to_bits()))
        }
        _ => None,
    }
}

/// Convenience: parse a decimal string into a `BigDecimal`, falling back
/// to `BigDecimal::zero()` on failure. Used by tests and internal
/// integration sites; not part of the public accumulator API.
#[cfg(test)]
pub(crate) fn parse_decimal(s: &str) -> BigDecimal {
    use std::str::FromStr;
    BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::zero())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn lit_long(n: i64) -> Binding {
        Binding::lit(FlakeValue::Long(n), Sid::xsd_integer())
    }
    fn lit_decimal(s: &str) -> Binding {
        Binding::lit(
            FlakeValue::Decimal(Box::new(parse_decimal(s))),
            Sid::xsd_decimal(),
        )
    }
    fn lit_double(d: f64) -> Binding {
        Binding::lit(FlakeValue::Double(d), Sid::xsd_double())
    }
    fn lit_float(d: f64) -> Binding {
        Binding::lit(FlakeValue::Double(d), Sid::xsd_float())
    }

    #[test]
    fn sum_all_integers_returns_xsd_integer() {
        let mut a = NumericAccum::new();
        a.add(&lit_long(1));
        a.add(&lit_long(2));
        a.add(&lit_long(3));
        match a.finalize_sum() {
            Binding::Lit { val, dtc, .. } => {
                assert_eq!(val.as_long(), Some(6));
                assert_eq!(*dtc.datatype(), Sid::xsd_integer());
            }
            other => panic!("expected Lit, got {other:?}"),
        }
    }

    #[test]
    fn sum_decimal_only_returns_xsd_decimal() {
        let mut a = NumericAccum::new();
        a.add(&lit_decimal("1.0"));
        a.add(&lit_decimal("2.2"));
        a.add(&lit_decimal("3.5"));
        let result = a.finalize_sum();
        let Binding::Lit { val, dtc, .. } = result else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_decimal());
        match val {
            FlakeValue::Decimal(d) => assert_eq!(d.normalized(), parse_decimal("6.7").normalized()),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn sum_mixed_integer_and_decimal_promotes_to_decimal() {
        let mut a = NumericAccum::new();
        a.add(&lit_long(1));
        a.add(&lit_decimal("2.2"));
        let result = a.finalize_sum();
        let Binding::Lit { val, dtc, .. } = result else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_decimal());
        match val {
            FlakeValue::Decimal(d) => assert_eq!(d.normalized(), parse_decimal("3.2").normalized()),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn sum_with_double_promotes_to_double() {
        let mut a = NumericAccum::new();
        a.add(&lit_decimal("2.5"));
        a.add(&lit_double(100.0));
        let result = a.finalize_sum();
        let Binding::Lit { val, dtc, .. } = result else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_double());
        assert!(matches!(val, FlakeValue::Double(d) if (d - 102.5).abs() < 1e-9));
    }

    #[test]
    fn sum_with_float_promotes_to_float_unless_double_seen() {
        let mut a = NumericAccum::new();
        a.add(&lit_long(1));
        a.add(&lit_float(2.5));
        let result = a.finalize_sum();
        let Binding::Lit { dtc, .. } = result else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_float());
    }

    #[test]
    fn float_then_double_promotes_to_double() {
        let mut a = NumericAccum::new();
        a.add(&lit_float(1.5));
        a.add(&lit_double(2.5));
        let result = a.finalize_sum();
        let Binding::Lit { dtc, .. } = result else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_double());
    }

    #[test]
    fn empty_sum_returns_unbound() {
        let a = NumericAccum::new();
        assert!(matches!(a.finalize_sum(), Binding::Unbound));
    }

    #[test]
    fn empty_avg_returns_zero_xsd_integer() {
        let a = NumericAccum::new();
        match a.finalize_avg() {
            Binding::Lit { val, dtc, .. } => {
                assert_eq!(val.as_long(), Some(0));
                assert_eq!(*dtc.datatype(), Sid::xsd_integer());
            }
            other => panic!("expected Lit 0 xsd:integer, got {other:?}"),
        }
    }

    #[test]
    fn avg_of_integers_returns_xsd_decimal() {
        // SPARQL: AVG(integers) → xsd:decimal because integer ÷ integer is
        // decimal-typed in the spec's division semantics.
        let mut a = NumericAccum::new();
        a.add(&lit_long(1));
        a.add(&lit_long(2));
        a.add(&lit_long(3));
        let Binding::Lit { val, dtc, .. } = a.finalize_avg() else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_decimal());
        match val {
            FlakeValue::Decimal(d) => assert_eq!(d.normalized(), parse_decimal("2").normalized()),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn avg_of_decimals_returns_xsd_decimal() {
        let mut a = NumericAccum::new();
        a.add(&lit_decimal("1.0"));
        a.add(&lit_decimal("2.2"));
        a.add(&lit_decimal("3.5"));
        a.add(&lit_decimal("2.2"));
        a.add(&lit_decimal("2.2"));
        let Binding::Lit { val, dtc, .. } = a.finalize_avg() else {
            panic!("expected Lit");
        };
        assert_eq!(*dtc.datatype(), Sid::xsd_decimal());
        match val {
            FlakeValue::Decimal(d) => {
                // 11.1 / 5 = 2.22 exactly
                assert_eq!(d.normalized(), parse_decimal("2.22").normalized());
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn dedup_key_value_equality_across_types() {
        // Integer 2 and Decimal 2.0 should dedup to the same key per
        // SPARQL value-equality, even though their datatype Sids differ.
        // (We use distinct prefixes on purpose: the DISTINCT semantics
        // for SUM here are RDF-term-equality; the W3C spec is permissive
        // about implementation choice. Our tests assert the chosen
        // behavior.)
        let k1 = numeric_dedup_key(&lit_long(2));
        let k2 = numeric_dedup_key(&lit_decimal("2.0"));
        // Different prefix because they're different RDF terms.
        assert_ne!(k1, k2);
        // But same-tier dedup works:
        assert_eq!(
            numeric_dedup_key(&lit_long(2)),
            numeric_dedup_key(&lit_long(2))
        );
        assert_eq!(
            numeric_dedup_key(&lit_decimal("2.0")),
            numeric_dedup_key(&lit_decimal("2.00"))
        );
    }
}
