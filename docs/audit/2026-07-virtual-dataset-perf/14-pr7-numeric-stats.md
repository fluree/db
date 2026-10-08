# PR-7 — widen file / row-group pruning to double + decimal (H4) — DESIGN SKETCH

**Branch:** `perf/r2rml-pr7-numeric-stats` (off `perf/r2rml-pr4c-optional-star` HEAD `b1cb988c2`)
**Status:** APPROVED + IMPLEMENTED (unit-green). Awaiting the H4 A/B/C live gate before commit/PR.
**Substrate:** ROADMAP PR-7, `05-diagnosis.md` H4 (F6: q011 date pruned 98.8% — the path works; decimal/double blind), `00-brief` (decimal never pushes down; int pushes down but doesn't prune).

## Where the blind spot actually is (two layers)

**Primary — the QUERY side never pushes a numeric predicate.** `ScanValue` (`provider.rs:43`) has only `Bool | Int | Date | Str | TemplateKey`; its own doc says *"Decimal / float predicates are left to the in-engine FILTER."* So `to_scan_value` (rewrite.rs) can't carry a double/decimal, `build_iceberg_filter` (r2rml.rs) never emits one, and the iceberg reader never even sees a numeric-column predicate to prune on. This is the real reason q019 shows `files_pruned = 0`.

**Secondary — the ICEBERG side would decline it anyway.** `LiteralValue` and `TypedValue` ALREADY carry `Float32/Float64/Decimal{unscaled,precision,scale}` (`value_codec.rs`, `predicate.rs`), and `TypedValue::partial_cmp` already scale-normalizes decimals (`decimal_cmp`). But: (i) `stat_bounds` (`pruning.rs:289`) has no `Statistics::Double/Float`/FLBA arm → `(None,None)`; (ii) `prunable_stats` (`:272`) returns `None` for ANY decimal column (a defensive guard); (iii) `TypedValue::lt/le` — which `bounds_can_contain` uses (NOT `partial_cmp`) — have Float arms but **no Decimal arm**, and the Float arms use raw `<` (a NaN hazard, §c).

## (a) `stat_bounds`: Double (clean) + Decimal (FLBA only)

- **Double/Float — add directly.** `(Statistics::Double(s), TypedValue::Float64(_))` and `(Statistics::Float(s), TypedValue::Float32(_))` arms. The bound is the raw f64/f32 min/max; `bounds_can_contain` compares via `lt/le` (Float arms exist, made NaN-safe in §c).
- **Decimal — support FLBA, DECLINE int32/int64-backed.** Parquet decimal physical encodings by precision: `int32` (≤9 digits), `int64` (≤18), `fixed_len_byte_array` (any) — all holding a **big-endian two's-complement UNSCALED integer**. The Iceberg spec mandates **FLBA** for decimals, so that is the only encoding a conformant table produces. PR-7 supports FLBA: relax `prunable_stats` to allow FLBA-decimal columns, decode the FLBA bytes → `i128` unscaled and read `precision/scale` from the column's `Decimal` logical type → `TypedValue::Decimal{unscaled, precision, scale}`. It **declines** int32/int64-backed decimals (return `None`, unchanged): their stats are unscaled ints indistinguishable from a real int column without threading the logical type, they are off-spec, and the whole point of the `07`/#1406 guard was to not compare an unscaled bound to a scaled literal. The scale is now carried IN the `TypedValue` and normalized by `decimal_cmp`, so the unscaled-vs-scaled bug that motivated the guard cannot recur on the FLBA path.

## (b) `ScanValue`/`TypedValue` + the query→iceberg bridge

- `TypedValue`: **add `Decimal` arms to `lt`/`le`** (delegate to `decimal_cmp`, as `partial_cmp` already does) — without them a pushed decimal predicate silently never prunes (`lt/le → None → keep`). Float arms already exist (fixed for NaN in §c).
- `ScanValue`: **add `Double(f64)` and `Decimal { unscaled: i128, precision: u8, scale: i8 }`** (mirroring `LiteralValue::Decimal`). Touch points, all small: `to_scan_value` (`rewrite.rs`) — emit them from an `xsd:double`/`xsd:decimal` FILTER operand; `build_iceberg_filter` (`r2rml.rs`) — bridge `ScanValue::Double/Decimal` → `LiteralValue::Float64/Decimal` (`to_typed_value` already handles the rest); `row_group_can_contain` / the file-level `decode_by_type_string` path already flow through `TypedValue`. The PR-1-era note stands: `ScanValue` variant additions are localized to those two functions plus the pattern match in the operator's filter builder.

## (c) NaN / ±0 and the strict-SUPERSET invariant

**Invariant (restated):** the pushdown is a conservative SUPER-filter — it may **over-keep** (a row group kept that the in-engine FILTER then rejects) but must **NEVER over-prune** (drop a row group holding a matching row). The in-engine SPARQL FILTER (post-decode) is the sole authority for the answer; pushdown only removes provably-empty files/row-groups.

**NaN.** Current `Float32/Float64` `lt/le` use raw `<`/`<=`, so any NaN operand yields `Some(false)` — which in `bounds_can_contain` can PRUNE (e.g. `col <= v` with a NaN upper bound → `lit.le(NaN)=Some(false)` → prune) even though NaN rows exist. That is an over-prune. **Fix:** make the Float `lt/le` arms NaN-conservative — if either operand is NaN, return `None` (→ `bounds_can_contain`'s `unwrap_or(true)` keeps the group). Parquet may also record a `nan_count`/absent-max; a column whose stats can't bound NaNs simply isn't pruned. **±0:** `-0.0 == +0.0` and neither `<` the other in IEEE — they collapse to one bound value, no ordering hazard. SPARQL FILTER numeric semantics (the authority) are unaffected; pushdown just declines to prune around NaN.

## (d) Gates

- **H4 A/B/C on the GL fact (the files_pruned counter now exists — F7 closed by PR-8's spans):**
  - **q019** (decimal FILTER): expect `files_pruned > 0` for the FIRST time — the headline H4 win. Honest caveat: the magnitude depends on whether the money column is CLUSTERED (like q011's date → 98.8%); an unclustered money column may prune little even with correct stats (data-layout, not code).
  - **q020** (date control): still prunes — a regression check that the existing int32/date path is untouched.
  - **q021** (int): already pushes down (`ScanValue::Int` + `stat_bounds` Int32/Int64) but prunes ~0 — DIAGNOSE and set expectations: per `00-brief`, the int column's values span every file's min/max (unsorted/unclustered), so no file is provably empty. **PR-7 does not fix this** — it is a data-distribution/layout property, not a stats gap; the fix would be clustering the data on the filtered column. State it plainly.
- **cold q019** (38.8 s cold, pruning-shaped prize): if the decimal column clusters, pruned files cut the cold data-fetch proportionally; if not, cold stays fetch-bound (the residual PR-5/PR-7-data lever). Report the actual files_pruned and the cold delta.
- **Parity everywhere** — the pushdown is a strict superset, so every result is byte-identical (the in-engine FILTER is authority); native 0-mismatch + q019/q020/q021 rows-parity.
- **Switch (my call): a dedicated `FLUREE_ICEBERG_NUMERIC_STATS`** (default on) gating ONLY the double+decimal widening, so it reverts independently of the shipped int/date/string pushdown (mirrors the PR-4c sub-switch discipline); off ⇒ `stat_bounds`/`ScanValue` behave exactly as today (numeric predicates → in-engine FILTER only).

## Change surface (on nod)

query: `ScanValue`(+Double,+Decimal) · `to_scan_value` · `build_iceberg_filter`. iceberg: `stat_bounds`(+Double,+FLBA-decimal) · `prunable_stats`(FLBA relax) · `TypedValue::lt/le`(+Decimal arm, NaN-safe Float). Tests: a hermetic pruning unit (double bounds prune/keep incl. NaN-keep; FLBA-decimal scale-aware prune/keep; int32/int64-decimal declines) + the H4 live A/B/C + parity. **PR-5's pruning leg builds on this** (numeric stats first, per the tail ordering).

## Implemented (approved with four riders)

Lead approved the two-layer reframe, the FLBA-only boundary, and the decimal_cmp-retires-the-skip line. Four riders, all landed:

1. **NaN over-prune fix ships in-PR + F15 register entry** (latent, unreachable pre-PR-7, armed by the `Double` push). `TypedValue::lt`/`le` `Float` arms now NaN→`None`→keep. See F15 in `04-findings-register.md`.
2. **Non-FLBA decimal decline is observable** — `prunable_stats` emits `tracing::debug!("decimal stats declined: non-FLBA physical encoding")` for INT32/INT64-backed decimals.
3. **Hermetic unit covers the cross-scale round-trip** — `9.99` (scale 2) vs a scale-3 column (`9.990`), prune + keep + boundary; plus a positive assert-it-prunes decimal case (guards the `lt/le` Decimal-arm gap) and the INT-decimal decline. Query decompose proven scale-insensitive.
4. **Live gate proves Snowflake decimals are FLBA** via q019 `files_pruned > 0`; if 0, check physical encoding before the code.

`!=` stays never-pruned; `eq`-on-double keeps `[min,max]` as a superset.

## The q019 integer-vs-decimal seam — Option A (lead ruling)

STOP-and-report finding: q019 (`FILTER(?deb > 1000000)`) compares an **integer** literal (SPARQL lexes `1000000` as `xsd:integer` → `FlakeValue::Long`) against the `xsd:decimal` `DEBIT_AMOUNT` column. The approved surface handled explicit decimal/double literals but NOT this integer-vs-decimal-column form, so q019 would still show `files_pruned=0`. Lead ruled **Option A**: in `build_iceberg_filter`, push `ScanValue::Int` against a `decimal(p,s)` column as an EXACT scale-0 decimal (`LiteralValue::Decimal{ unscaled: n as i128, scale: 0 }`) instead of `Int64` — `decimal_cmp` normalizes the scale gap, so the superset invariant holds by construction (int is exact as a scale-0 decimal; `rescale`'s `checked_mul→None→keep` covers the overflow edge). Gated by an **api-side** `iceberg_numeric_stats_enabled()` OnceLock reading `FLUREE_ICEBERG_NUMERIC_STATS` (the disk_catalog_cache precedent; the query-crate `pub(crate)` fn is NOT promoted). Off ⇒ integer stays `Int64` → decimal bound compare declines → no prune (full revert). Localized in `int_pushdown_literal`.

**Two documented residuals (decline-observably, not implemented).** (i) A DOUBLE literal vs a decimal column is NOT pushed — a binary-float→decimal coercion is inexact in general, so keep is correct; routed through a `debug!` breadcrumb. (ii) A DECIMAL literal vs an integer column is NOT pushed — no exact cross-type bound compare — likewise keep + breadcrumb. Both are conservative residuals (never wrong), not gaps; a future PR could add exact handling if a workload needs it.

**Change surface as shipped.** query: `ScanValue`(+`Double`,+`Decimal`) · `to_scan_value`(+gated arms, `scan_value_from_bigdecimal`) · `iceberg_numeric_stats_enabled()` (`FLUREE_ICEBERG_NUMERIC_STATS`, mod.rs) · `object_term_matches` exhaustiveness arms · `build_iceberg_filter`(`Double`→`Float64` on double cols, `Decimal`→`Decimal` on decimal cols, `Int`→scale-0 `Decimal` on decimal cols via `int_pushdown_literal`, + the two decline breadcrumbs). api: `iceberg_numeric_stats_enabled()` OnceLock + `int_pushdown_literal`. iceberg: `stat_bounds`(+`Double`/`Float`/FLBA-decimal, +`col_decimal` param) · `prunable_stats`(FLBA relax + decline log) · `column_decimal` helper · `TypedValue::lt`/`le`(+Decimal arm, NaN-safe Float). The numeric widening is gated at BOTH push sites (query `to_scan_value` for explicit decimal/double, api `int_pushdown_literal` for the int-coercion); the iceberg-side widening is inert without a numeric `LiteralValue`.

**Tests (unit-green, 11 new).** iceberg `pruning.rs`: `row_group_pruning_uses_double_stats`, `row_group_pruning_uses_flba_decimal_stats`, `row_group_pruning_int_coerced_scale0_decimal` (positive prune / in-bounds keep / rescale-overflow keep / Int64 revert), `row_group_pruning_declines_int_backed_decimal`, `bounds_can_contain_keeps_on_nan_bound`. iceberg `value_codec.rs`: `nan_float_compare_is_incomparable`, `decimal_lt_le_cross_scale`. query `rewrite.rs`: `bigdecimal_decomposes_scale_insensitively`. api `r2rml.rs`: `int_literal_coerces_to_scale0_decimal_only_when_numeric_stats_on`, `int_scalar_against_decimal_column_pushes_scale0_decimal`, `double_pushed_only_against_double_column`, `decimal_pushed_only_against_decimal_column_preserving_literal_scale`.

**Next:** H4 A/B/C live (q019 `files_pruned>0` — NOW the coercion makes this reachable; q020 date control; q021 int diagnose) + cold q019 + parity, then commit + PR. PR-5's pruning leg builds on this.
