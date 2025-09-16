### OWL-Datalog: Data-property hasValue with typed literals – backward inference limitation

This note documents a current limitation in the OWL-Datalog reasoner regarding data-property `owl:hasValue` with typed literals, shows how it manifests, and sketches a path to enable full support.

### Summary
- **Works**: Forward entailment from class membership to data-property assertion when the restriction uses `owl:hasValue` (typed or untyped).
- **Limitation**: **Backward inference** from a data-property assertion with a typed literal to class membership does not match today.
- **Workaround**: Use untyped literals (no `"@type"`) for the literal value, or remodel as an object property when appropriate.

### Affected pattern
Given a class defined by a hasValue restriction on a datatype property:

```turtle
# HighQuality ≡ ∃ qualityScore.{"95"^^xsd:int}
HighQuality ≡ ∃ qualityScore.{"95"^^xsd:int}

# Data
hasQualityScore(product1, "95"^^xsd:int)

# Expected OWL 2 RL-style backward inference (not working today):
hasQualityScore(?x, "95"^^xsd:int) → HighQuality(?x)
```

Forward entailment still works:

```turtle
KilogramMeasurement(?x) → hasUnit(?x, kg)  # object property hasValue
```

### JSON-LD examples
- Ontology (restriction with typed literal):

```json
{
  "@context": {"ex": "http://example.org/", "owl": "http://www.w3.org/2002/07/owl#", "xsd": "http://www.w3.org/2001/XMLSchema#"},
  "insert": [
    {
      "@id": "ex:HighQuality",
      "@type": "owl:Class",
      "owl:equivalentClass": {
        "@type": "owl:Restriction",
        "owl:onProperty": {"@id": "ex:qualityScore"},
        "owl:hasValue": {"@value": 95, "@type": "xsd:int"}
      }
    },
    {"@id": "ex:qualityScore", "@type": "owl:DatatypeProperty"}
  ]
}
```

- Data (typed-literal assertion):

```json
{
  "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
  "insert": [
    {"@id": "ex:product1", "ex:qualityScore": {"@value": 95, "@type": "xsd:int"}}
  ]
}
```

Observed behavior: class membership `ex:product1 rdf:type ex:HighQuality` is not inferred via the backward rule when the value is a typed JSON-LD literal node.

### Root cause (implementation details)
At rule compilation time, `owl:hasValue` value nodes are preserved “as-is” when building the rule `where` clause for backward inference.

```436:468:src/fluree/db/reasoner/owl_datalog.cljc
(defn equiv-has-value
  "Handles rules cls-hv1, cls-hv2"
  [rule-class restrictions]
  (reduce
   (fn [acc restriction]
     (let [{:keys [property is-inverse?]} (extract-property-with-inverse restriction)
           has-val  (util/get-first restriction const/iri-owl:hasValue)
           has-val* (cond
                      (util/get-id has-val) {"@id" (util/get-id has-val)}
                      (and (map? has-val) (contains? has-val "@value")) has-val
                      :else (util/get-value has-val))
           ...))
```

For typed literals, the rule’s `where` ends up matching on the raw JSON-LD value node (e.g., `{ "@value": 95, "@type": "xsd:int" }`). However, the general query pipeline canonicalizes literal matching by expanding datatype IRIs and coercing values to internal representations before comparison:

```69:87:src/fluree/db/query/fql/parse.cljc
(defn parse-value-datatype
  [v attrs context]
  (if-let [dt (get-type attrs)]
    (if (v/variable? dt)
      (-> v where/untyped-value (where/link-dt-var dt))
      (let [dt-iri (json-ld/expand-iri dt context)
            dt-sid (iri/iri->sid dt-iri)
            v*     (datatype/coerce-value v dt-sid)]
        (if (= const/iri-id dt-iri)
          (let [expanded (json-ld/expand-iri v* context)]
            (where/match-iri where/unmatched expanded)
          (where/anonymous-value v* dt-iri))))
    ...))
```

Because the reasoner’s generated rule bypasses this normalization step, typed literal unification in the `where` clause may not align with the stored (coerced, normalized) representation that the matcher expects.

### Why untyped literals work today
Untyped values are inserted and matched as simple scalars. The reasoner builds a `where` clause using the raw value (e.g., `95`), which aligns with how the matcher handles inferred JSON-LD-native types without needing a datatype node.

### Potential solution
Make the reasoner normalize typed value nodes when compiling `owl:hasValue` rules so that matching uses the same canonical form as regular queries.

Option A (local fix in rule generation):
- When `owl:hasValue` is a value node with `@value` and `@type`:
  - Expand the datatype IRI (compact `xsd:int` → full IRI).
  - Use `fluree.db.datatype/coerce-value` to coerce the literal to the internal representation for that datatype.
  - Emit the `where` clause using the normalized `(value, datatype-iri)` pair rather than the raw JSON-LD node map.
- Apply the same normalization to both backward (value → class) and forward (class → value) rule paths to keep symmetry.

Option B (shared canonicalization path):
- Introduce a small utility used by both query parsing and reasoner rule compilation that:
  - Expands datatype IRIs using the applicable JSON-LD context.
  - Coerces values via `datatype/coerce-value`.
  - Produces a canonical representation consumed by the matcher.

Considerations:
- Compact vs expanded datatype IRIs (`xsd:int` vs full IRI) must be normalized.
- Numeric families (`xsd:int`, `xsd:integer`, etc.) should rely on existing coercion semantics in `fluree.db.datatype`.
- Language-tagged literals are not valid with `@type` and should remain rejected when both are present.
- Keep CLJ/CLJS parity.

### Tests to add
- Positive: Backward inference with typed literal hasValue.
  - Ontology: `HighQuality ≡ ∃qualityScore.{95^^xsd:int}`.
  - Data: `qualityScore(product1, 95^^xsd:int)`.
  - Expect: `product1 rdf:type HighQuality`.
- Negative: Mismatch in literal value or datatype does not classify.
- Regression: Forward entailment (class → hasValue) still works for typed and untyped.

### Current workaround (unchanged)
- Use untyped literals (omit `"@type"`) if classification via data-property value is needed today.
- Where suitable, remodel as an object property with an individual (object) value.

### Conclusion
There is no conceptual reason in OWL 2 RL that forbids typed-literal backward inference for `owl:hasValue`. The current behavior is an implementation gap caused by missing literal normalization in rule-generated `where` patterns. Normalizing typed value nodes during rule compilation—or unifying rule compilation with the query pipeline’s canonicalization—should enable this feature with low risk.


