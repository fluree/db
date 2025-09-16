# OWL-Datalog Reasoner

A rule-materialization, Horn fragment approximating parts of SROIQ(D): supports intersections, unions compiled as alternate rules, property hierarchies and chains (with inverses), some/all/hasValue restrictions, and selected datatype equality. More expressive than OWL 2 RL; incomplete for OWL DL.

## Overview

The OWL-Datalog reasoner extends Fluree's existing OWL 2 RL reasoner with additional constructs while maintaining Datalog compatibility. It provides forward-chaining inference through rule materialization, generating inferred triples that are stored alongside the original data.

## Extensions Beyond OWL 2 RL

### Complex Intersections with Restrictions

OWL 2 RL has limited support for complex class expressions in intersections. OWL-Datalog supports:

```turtle
# Complex equivalentClass with multiple restrictions
ElectricVehicle ≡ Vehicle ∩ ∀hasPowerSource.{electricity} ∩ ∃hasRange.LongRange
```

This generates rules for:
- Forward entailment: If x is ElectricVehicle → x has the required properties
- Backward inference: If x meets all intersection criteria → x is ElectricVehicle

### Union Classes as Multiple Rules

While OWL 2 RL supports basic unions, OWL-Datalog compiles unions into multiple rule alternatives:

```turtle
# Union in equivalentClass
PaymentMethod ≡ CreditCard ∪ DebitCard ∪ DigitalWallet

# Generates separate rules:
# Rule 1: CreditCard(?x) → PaymentMethod(?x)
# Rule 2: DebitCard(?x) → PaymentMethod(?x) 
# Rule 3: DigitalWallet(?x) → PaymentMethod(?x)
```

### Property Chains with Inverse Properties

Extended support for complex property chains including inverse properties:

```turtle
# Property chain with inverse in the middle
hasGrandparent ≡ hasParent ∘ hasChild⁻ ∘ hasSibling

# Inline property chains in restrictions
ChainedClass ≡ ∃(hasParent ∘ hasChild⁻ ∘ hasSibling).Person
```

### Advanced Restriction Handling

#### hasValue Forward Entailment
```turtle
# If x is KilogramMeasurement, infer hasUnit kg
KilogramMeasurement ≡ Measurement ∩ ∃hasUnit.{kg}
```

#### allValuesFrom Forward Entailment
```turtle
# If x is SecureFolder and x contains y, then y is SecureDocument
SecureFolder ≡ Folder ∩ ∀contains.SecureDocument
```

#### Multiple Restrictions on Same Property
```turtle
# Multiple constraints on the same property
LuxuryCar ≡ Car ∩ ∃hasFeature.LeatherSeats ∩ ∃hasFeature.NavigationSystem ∩ ∃hasFeature.{sunroof}
```

## Supported Constructs

### Class Expressions
- **Intersections** (`owl:intersectionOf`): Full support including nested restrictions
- **Unions** (`owl:unionOf`): Compiled as multiple rule alternatives
- **Equivalences** (`owl:equivalentClass`): Bidirectional rules with superclass materialization

### Property Expressions
- **Property hierarchies** (`rdfs:subPropertyOf`): Transitive inference
- **Property chains** (`owl:propertyChainAxiom`): Including inverse properties
- **Inverse properties** (`owl:inverseOf`): With double inverse normalization

### Restrictions
- **Existential restrictions** (`owl:someValuesFrom`): Including unions and nested restrictions
- **Universal restrictions** (`owl:allValuesFrom`): Forward and backward inference
- **Value restrictions** (`owl:hasValue`): Object and data property values with forward entailment
- **Qualified cardinalities**: Basic parsing (implementation planned)

### Data Types
- **Typed literals**: Basic support for datatype-aware comparisons
- **Datatype properties**: Forward entailment for hasValue restrictions

## Architecture

### Rule Generation Process

1. **Graph Extraction**: Queries database with depth-6 traversal to capture nested structures
2. **Statement Classification**: Categorizes OWL constructs by type (class, property, restriction)
3. **Rule Compilation**: Generates Datalog rules for each construct
4. **Rule Materialization**: Executes rules to derive new triples

### Rule Types

- **Classification rules**: Infer class membership based on restrictions
- **Property rules**: Handle property hierarchies and chains  
- **Equivalence rules**: Bidirectional inference for equivalent classes
- **Forward entailment rules**: Derive property values from class membership
- **Backward inference rules**: Classify instances based on property patterns

### Integration with OWL 2 RL

OWL-Datalog extends the base OWL 2 RL ruleset with:
- Additional rule patterns for complex intersections
- Union handling as multiple rule alternatives
- Enhanced restriction processing
- Property chain inference

## Current Limitations

### Known Issues

#### Data Property hasValue with Typed Literals
**Status**: Limited support  
**Issue**: Backward inference from typed literal values to class membership doesn't work yet.

```turtle
# Forward entailment works:
KilogramMeasurement(?x) → hasUnit(?x, kg)

# Backward inference doesn't work:
hasQualityScore(?x, "95"^^xsd:int) ↛ HighQuality(?x)
```

**Workaround**: Use untyped literals (plain JSON-LD values with no "@type", e.g., `95` or `"95"` rather than `{"@value":95,"@type":"xsd:int"}`) or model the value as an object property where possible.

#### Property Chains with Separately Defined Axioms
**Status**: Partially supported  
**Issue**: allValuesFrom restrictions on properties that have chain axioms defined separately don't work because the restriction processor doesn't follow property references.

```turtle
# Works (inline chain):
ChainedClass ≡ ∃(hasParent ∘ hasChild).Person

# Doesn't work (separate definition):
hasGrandchild owl:propertyChainAxiom (hasParent hasChild) .
ChainedClass ≡ ∀hasGrandchild.Person
```

**Workaround**: Use inline property chain definitions in restrictions.

### Planned Enhancements

#### Full Qualified Cardinality Support
Currently only parsed, not reasoned over:
```turtle
# Planned support:
BoardOfDirectors ≡ Group ∩ ≥5 hasMember.Director ∩ ≤15 hasMember.Director
```

#### Disjointness Reasoning
```turtle
# Planned support:
Student owl:disjointWith Teacher
```

#### Complement Classes
```turtle  
# Planned support:
Inactive ≡ ¬Active
```

#### Enhanced Datatype Support
- More comprehensive typed literal matching
- Datatype reasoning and conversions
- Numeric comparisons and ranges

### OWL DL Incompleteness

OWL-Datalog is intentionally incomplete for full OWL DL to maintain decidability and performance:

- **Open World Assumption**: Limited support; primarily closed-world reasoning
- **Complex Boolean Combinations**: Some nested boolean expressions not supported
- **Unbounded Property Chains**: Chain length restrictions for performance
- **Modal Operators**: No support for modal logic constructs

## Usage Examples

### Basic Setup

```clojure
;; Create database with OWL ontology
(def db @(fluree/update db ontology))

;; Apply OWL-Datalog reasoning
(def reasoned-db @(fluree/reason db :owl-datalog))

;; Query inferred triples
@(fluree/query reasoned-db query)
```

### Complex Intersection Example

```json
{
  "@context": {
    "ex": "http://example.org/",
    "owl": "http://www.w3.org/2002/07/owl#"
  },
  "insert": [
    {
      "@id": "ex:PremiumProduct",
      "@type": "owl:Class",
      "owl:equivalentClass": {
        "@type": "owl:Class",
        "owl:intersectionOf": {
          "@list": [
            {"@id": "ex:Product"},
            {
              "@type": "owl:Restriction",
              "owl:onProperty": {"@id": "ex:hasQuality"},
              "owl:allValuesFrom": {"@id": "ex:HighQuality"}
            },
            {
              "@type": "owl:Restriction", 
              "owl:onProperty": {"@id": "ex:hasWarranty"},
              "owl:someValuesFrom": {"@id": "ex:ExtendedWarranty"}
            }
          ]
        }
      }
    }
  ]
}
```

### Property Chain Example

```json
{
  "@context": {
    "ex": "http://example.org/",
    "owl": "http://www.w3.org/2002/07/owl#"
  },
  "insert": [
    {
      "@id": "ex:ChainedRelation",
      "@type": "owl:Class",
      "owl:equivalentClass": {
        "@type": "owl:Restriction",
        "owl:onProperty": {
          "@type": "owl:ObjectProperty",
          "owl:propertyChainAxiom": {
            "@list": [
              {"@id": "ex:hasParent"},
              {
                "@type": "owl:ObjectProperty",
                "owl:inverseOf": {"@id": "ex:hasChild"}
              },
              {"@id": "ex:hasSibling"}
            ]
          }
        },
        "owl:someValuesFrom": {"@id": "ex:Person"}
      }
    }
  ]
}
```

## Performance Characteristics

### Rule Materialization
- **Forward-chaining**: All inferences computed upfront
- **Query Time**: Fast queries over materialized triples
- **Update Cost**: Re-reasoning required after ontology changes
- **Memory Usage**: Stores both asserted and inferred triples

### Scalability Considerations
- **Ontology Complexity**: Performance scales with rule complexity
- **Data Volume**: Materialized triples increase with instance data
- **Chain Length**: Property chain depth affects reasoning time
- **Union Cardinality**: Large unions generate many rules

## Testing and Validation

The reasoner includes comprehensive test suites:

- **Core functionality tests**: Basic OWL constructs (`owl_datalog_test.clj`)
- **Edge case tests**: Complex scenarios with unions, inverses, chains (`owl_datalog_edge_cases_test.clj`)  
- **Restriction tests**: Advanced restriction features (`owl_datalog_restrictions_test.clj`)

**Current Status**: 19 tests, 50 assertions, 0 failures

## Future Roadmap

### Short Term
1. **Enhanced typed literal support**: Improve datalog engine for better type matching
2. **Qualified cardinality reasoning**: Full implementation beyond parsing
3. **Performance optimizations**: Rule indexing and incremental reasoning

### Medium Term  
1. **Disjointness reasoning**: Support for `owl:disjointWith`
2. **Complement classes**: Basic negation support
3. **Enhanced datatype operations**: Numeric comparisons, string operations

### Long Term
1. **Incremental reasoning**: Update only affected inferences
2. **Explanation support**: Trace inference chains for debugging
3. **Custom rule extensions**: User-defined reasoning patterns

## Comparison with Other Reasoners

| Feature | OWL-Datalog | OWL 2 RL | Full OWL DL |
|---------|-------------|----------|-------------|
| Complex Intersections | ✅ Full | ⚠️ Limited | ✅ Full |
| Union Handling | ✅ Multiple Rules | ✅ Basic | ✅ Full |
| Property Chains | ✅ With Inverses | ✅ Basic | ✅ Full |
| hasValue Forward | ✅ Yes | ❌ No | ✅ Yes |
| Qualified Cardinalities | ⚠️ Planned | ❌ No | ✅ Full |
| Performance | ✅ Fast | ✅ Fast | ⚠️ Variable |
| Completeness | ⚠️ Horn Fragment | ⚠️ Limited | ✅ Complete |

## Conclusion

OWL-Datalog provides a powerful middle ground between the limited expressivity of OWL 2 RL and the computational complexity of full OWL DL reasoning. It's particularly well-suited for applications requiring:

- **Fast query performance** over materialized inferences
- **Complex class expressions** with intersections and unions
- **Property relationship reasoning** including chains and inverses
- **Rich domain modeling** with complex restriction patterns

The reasoner's Horn clause foundation ensures decidability while supporting significantly more expressive constructs than standard OWL 2 RL profiles.