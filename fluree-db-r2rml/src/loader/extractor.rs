//! R2RML mapping extractor
//!
//! Extracts TriplesMap definitions from a Graph IR.

use std::collections::HashMap;

use fluree_graph_ir::{Graph, Term, Triple};

use crate::error::{R2rmlError, R2rmlResult};
use crate::mapping::{
    ConstantValue, JoinCondition, LogicalTable, ObjectMap, PredicateMap, PredicateObjectMap,
    RefObjectMap, SubjectMap, TermType, TriplesMap,
};
use crate::vocab::R2RML;

/// Extracts R2RML mappings from a Graph IR
pub struct MappingExtractor<'a> {
    /// The source graph
    graph: &'a Graph,
    /// Index: subject → triples with that subject
    by_subject: HashMap<&'a str, Vec<&'a Triple>>,
}

impl<'a> MappingExtractor<'a> {
    /// Create a new extractor for the given graph
    pub fn new(graph: &'a Graph) -> Self {
        // Build subject index
        let mut by_subject: HashMap<&str, Vec<&Triple>> = HashMap::new();
        for triple in graph.iter() {
            if let Some(subj) = triple.s.as_iri() {
                by_subject.entry(subj).or_default().push(triple);
            } else if let Some(blank) = triple.s.as_blank() {
                // Use blank node's ntriples form as key
                by_subject.entry(blank.as_str()).or_default().push(triple);
            }
        }

        Self { graph, by_subject }
    }

    /// Extract all TriplesMap definitions from the graph
    pub fn extract_all(&self) -> R2rmlResult<Vec<TriplesMap>> {
        let mut triples_maps = Vec::new();

        // Find all subjects that are rdf:type rr:TriplesMap
        for triple in self.graph.iter() {
            if triple.p.as_iri() == Some(R2RML::RDF_TYPE)
                && triple.o.as_iri() == Some(R2RML::TRIPLES_MAP)
            {
                if let Some(subj_iri) = triple.s.as_iri() {
                    let tm = self.extract_triples_map(subj_iri)?;
                    triples_maps.push(tm);
                }
            }
        }

        Ok(triples_maps)
    }

    /// Extract a single TriplesMap by its IRI
    fn extract_triples_map(&self, tm_iri: &str) -> R2rmlResult<TriplesMap> {
        let triples = self.get_triples_for_subject(tm_iri);

        // Extract logical table
        let logical_table = self.extract_logical_table(&triples)?;

        // Extract subject map
        let subject_map = self.extract_subject_map(&triples)?;

        // Extract predicate-object maps
        let poms = self.extract_predicate_object_maps(&triples)?;

        Ok(TriplesMap {
            iri: tm_iri.to_string(),
            logical_table,
            subject_map,
            predicate_object_maps: poms,
        })
    }

    /// Extract the logical table from a TriplesMap
    fn extract_logical_table(&self, triples: &[&Triple]) -> R2rmlResult<LogicalTable> {
        // Find rr:logicalTable property
        let logical_table_obj = self.find_object(triples, R2RML::LOGICAL_TABLE)?;

        // The object should be a blank node or IRI that has rr:tableName
        let table_triples = self.get_triples_for_term(&logical_table_obj);

        // Find rr:tableName
        if let Some(table_name) = self.find_object_optional(&table_triples, R2RML::TABLE_NAME) {
            if let Some(name) = self.term_to_string(&table_name) {
                let normalized = LogicalTable::normalize_table_name(&name);
                return Ok(LogicalTable::TableName(normalized));
            }
        }

        // Check for rr:sqlQuery (not supported)
        if self
            .find_object_optional(&table_triples, R2RML::SQL_QUERY)
            .is_some()
        {
            return Err(R2rmlError::Unsupported(
                "rr:sqlQuery is not supported for Iceberg graph sources".to_string(),
            ));
        }

        Err(R2rmlError::MissingProperty(
            "rr:tableName in logical table".to_string(),
        ))
    }

    /// Extract the subject map from a TriplesMap
    fn extract_subject_map(&self, triples: &[&Triple]) -> R2rmlResult<SubjectMap> {
        // Check for shorthand rr:subject first
        if let Some(subject_obj) = self.find_object_optional(triples, R2RML::SUBJECT) {
            let iri = self
                .term_to_iri(&subject_obj)
                .ok_or_else(|| R2rmlError::InvalidValue {
                    property: "rr:subject".to_string(),
                    message: "expected IRI".to_string(),
                })?;
            return Ok(SubjectMap::constant(iri));
        }

        // Find rr:subjectMap property
        let subject_map_obj = self.find_object(triples, R2RML::SUBJECT_MAP)?;
        let sm_triples = self.get_triples_for_term(&subject_map_obj);

        let mut subject_map = SubjectMap::default();

        // Extract rr:template
        if let Some(template_obj) = self.find_object_optional(&sm_triples, R2RML::TEMPLATE) {
            if let Some(template) = self.term_to_string(&template_obj) {
                subject_map.template_columns = crate::mapping::extract_template_columns(&template);
                subject_map.template = Some(template);
            }
        }

        // Extract rr:column
        if let Some(column_obj) = self.find_object_optional(&sm_triples, R2RML::COLUMN) {
            if let Some(col) = self.term_to_string(&column_obj) {
                subject_map.column = Some(col);
            }
        }

        // Extract rr:constant
        if let Some(constant_obj) = self.find_object_optional(&sm_triples, R2RML::CONSTANT) {
            if let Some(iri) = self.term_to_iri(&constant_obj) {
                subject_map.constant = Some(iri);
            }
        }

        // Extract rr:class(es)
        for class_obj in self.find_objects(&sm_triples, R2RML::CLASS) {
            if let Some(class_iri) = self.term_to_iri(&class_obj) {
                subject_map.classes.push(class_iri);
            }
        }

        // Extract rr:termType
        if let Some(term_type_obj) = self.find_object_optional(&sm_triples, R2RML::TERM_TYPE) {
            if let Some(term_type_iri) = self.term_to_iri(&term_type_obj) {
                if let Some(tt) = TermType::from_iri(&term_type_iri) {
                    subject_map.term_type = tt;
                }
            }
        }

        Ok(subject_map)
    }

    /// Extract all predicate-object maps from a TriplesMap
    fn extract_predicate_object_maps(
        &self,
        triples: &[&Triple],
    ) -> R2rmlResult<Vec<PredicateObjectMap>> {
        let mut poms = Vec::new();

        for pom_obj in self.find_objects(triples, R2RML::PREDICATE_OBJECT_MAP) {
            let pom = self.extract_predicate_object_map(&pom_obj)?;
            poms.push(pom);
        }

        Ok(poms)
    }

    /// Extract a single predicate-object map
    fn extract_predicate_object_map(&self, pom_term: &Term) -> R2rmlResult<PredicateObjectMap> {
        let pom_triples = self.get_triples_for_term(pom_term);

        // Extract predicate map
        let predicate_map = self.extract_predicate_map(&pom_triples)?;

        // Extract object map
        let object_map = self.extract_object_map(&pom_triples)?;

        Ok(PredicateObjectMap {
            predicate_map,
            object_map,
        })
    }

    /// Extract predicate map from a predicate-object map
    fn extract_predicate_map(&self, triples: &[&Triple]) -> R2rmlResult<PredicateMap> {
        // Check for shorthand rr:predicate first
        if let Some(pred_obj) = self.find_object_optional(triples, R2RML::PREDICATE) {
            let iri = self
                .term_to_iri(&pred_obj)
                .ok_or_else(|| R2rmlError::InvalidValue {
                    property: "rr:predicate".to_string(),
                    message: "expected IRI".to_string(),
                })?;
            return Ok(PredicateMap::Constant(iri));
        }

        // Find rr:predicateMap
        if let Some(pm_obj) = self.find_object_optional(triples, R2RML::PREDICATE_MAP) {
            let pm_triples = self.get_triples_for_term(&pm_obj);

            // Check for rr:constant
            if let Some(const_obj) = self.find_object_optional(&pm_triples, R2RML::CONSTANT) {
                if let Some(iri) = self.term_to_iri(&const_obj) {
                    return Ok(PredicateMap::Constant(iri));
                }
            }

            // Check for rr:template
            if let Some(template_obj) = self.find_object_optional(&pm_triples, R2RML::TEMPLATE) {
                if let Some(template) = self.term_to_string(&template_obj) {
                    let columns = crate::mapping::extract_template_columns(&template);
                    return Ok(PredicateMap::Template { template, columns });
                }
            }

            // Check for rr:column
            if let Some(col_obj) = self.find_object_optional(&pm_triples, R2RML::COLUMN) {
                if let Some(col) = self.term_to_string(&col_obj) {
                    return Ok(PredicateMap::Column(col));
                }
            }
        }

        Err(R2rmlError::MissingProperty(
            "rr:predicate or rr:predicateMap".to_string(),
        ))
    }

    /// Extract object map from a predicate-object map
    fn extract_object_map(&self, triples: &[&Triple]) -> R2rmlResult<ObjectMap> {
        // Check for shorthand rr:object first
        if let Some(obj) = self.find_object_optional(triples, R2RML::OBJECT) {
            return Ok(self.constant_from_term(&obj));
        }

        // Find rr:objectMap
        let om_obj = self.find_object(triples, R2RML::OBJECT_MAP)?;
        let om_triples = self.get_triples_for_term(&om_obj);

        // Check for rr:parentTriplesMap (RefObjectMap)
        if let Some(parent_obj) = self.find_object_optional(&om_triples, R2RML::PARENT_TRIPLES_MAP)
        {
            let parent_iri =
                self.term_to_iri(&parent_obj)
                    .ok_or_else(|| R2rmlError::InvalidValue {
                        property: "rr:parentTriplesMap".to_string(),
                        message: "expected IRI".to_string(),
                    })?;

            let mut join_conditions = self.extract_join_conditions(&om_triples)?;

            // Iceberg subset: RefObjectMap without join conditions is invalid
            // (would cause cross-join explosion at runtime)
            if join_conditions.is_empty() {
                return Err(R2rmlError::InvalidValue {
                    property: "rr:parentTriplesMap".to_string(),
                    message: "RefObjectMap requires at least one rr:joinCondition for Iceberg graph sources".to_string(),
                });
            }

            // Sort for stable ordering (graph iteration order is not guaranteed)
            join_conditions.sort_by(|a, b| {
                (&a.child_column, &a.parent_column).cmp(&(&b.child_column, &b.parent_column))
            });

            return Ok(ObjectMap::RefObjectMap(RefObjectMap::with_conditions(
                parent_iri,
                join_conditions,
            )));
        }

        // Extract common properties
        let datatype = self
            .find_object_optional(&om_triples, R2RML::DATATYPE)
            .and_then(|t| self.term_to_iri(&t));
        let language = self
            .find_object_optional(&om_triples, R2RML::LANGUAGE)
            .and_then(|t| self.term_to_string(&t));
        let term_type = self
            .find_object_optional(&om_triples, R2RML::TERM_TYPE)
            .and_then(|t| self.term_to_iri(&t))
            .and_then(|iri| TermType::from_iri(&iri))
            .unwrap_or(TermType::Literal);

        // Check for rr:column
        if let Some(col_obj) = self.find_object_optional(&om_triples, R2RML::COLUMN) {
            if let Some(col) = self.term_to_string(&col_obj) {
                return Ok(ObjectMap::Column {
                    column: col,
                    datatype,
                    language,
                    term_type,
                });
            }
        }

        // Check for rr:constant
        if let Some(const_obj) = self.find_object_optional(&om_triples, R2RML::CONSTANT) {
            return Ok(self.constant_from_term(&const_obj));
        }

        // Check for rr:template
        if let Some(template_obj) = self.find_object_optional(&om_triples, R2RML::TEMPLATE) {
            if let Some(template) = self.term_to_string(&template_obj) {
                let columns = crate::mapping::extract_template_columns(&template);
                return Ok(ObjectMap::Template {
                    template,
                    columns,
                    term_type,
                    datatype,
                    language,
                });
            }
        }

        Err(R2rmlError::MissingProperty(
            "rr:column, rr:constant, rr:template, or rr:parentTriplesMap".to_string(),
        ))
    }

    /// Extract join conditions from a RefObjectMap
    fn extract_join_conditions(&self, triples: &[&Triple]) -> R2rmlResult<Vec<JoinCondition>> {
        let mut conditions = Vec::new();

        for jc_obj in self.find_objects(triples, R2RML::JOIN_CONDITION) {
            let jc_triples = self.get_triples_for_term(&jc_obj);

            let child = self
                .find_object_optional(&jc_triples, R2RML::CHILD)
                .and_then(|t| self.term_to_string(&t))
                .ok_or_else(|| {
                    R2rmlError::MissingProperty("rr:child in join condition".to_string())
                })?;

            let parent = self
                .find_object_optional(&jc_triples, R2RML::PARENT)
                .and_then(|t| self.term_to_string(&t))
                .ok_or_else(|| {
                    R2rmlError::MissingProperty("rr:parent in join condition".to_string())
                })?;

            conditions.push(JoinCondition::new(child, parent));
        }

        Ok(conditions)
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /// Get all triples with a given subject (IRI or blank node)
    fn get_triples_for_subject(&self, subject: &str) -> Vec<&Triple> {
        self.by_subject.get(subject).cloned().unwrap_or_default()
    }

    /// Get triples for a term (handling both IRIs and blank nodes)
    fn get_triples_for_term(&self, term: &Term) -> Vec<&Triple> {
        match term {
            Term::Iri(iri) => self.get_triples_for_subject(iri),
            Term::BlankNode(blank) => self.get_triples_for_subject(blank.as_str()),
            _ => Vec::new(),
        }
    }

    /// Find the object of a required property
    fn find_object(&self, triples: &[&Triple], predicate: &str) -> R2rmlResult<Term> {
        self.find_object_optional(triples, predicate)
            .ok_or_else(|| R2rmlError::MissingProperty(predicate.to_string()))
    }

    /// Find the object of an optional property
    fn find_object_optional(&self, triples: &[&Triple], predicate: &str) -> Option<Term> {
        triples
            .iter()
            .find(|t| t.p.as_iri() == Some(predicate))
            .map(|t| t.o.clone())
    }

    /// Find all objects of a property (for multi-valued properties like rr:class)
    fn find_objects(&self, triples: &[&Triple], predicate: &str) -> Vec<Term> {
        triples
            .iter()
            .filter(|t| t.p.as_iri() == Some(predicate))
            .map(|t| t.o.clone())
            .collect()
    }

    /// Convert a term to a string (for literals)
    fn term_to_string(&self, term: &Term) -> Option<String> {
        match term {
            Term::Literal { value, .. } => Some(value.lexical()),
            Term::Iri(iri) => Some(iri.to_string()), // Sometimes table names are IRIs
            _ => None,
        }
    }

    /// Convert a term to an IRI string
    fn term_to_iri(&self, term: &Term) -> Option<String> {
        term.as_iri().map(std::string::ToString::to_string)
    }

    /// Create a constant ObjectMap from a term
    fn constant_from_term(&self, term: &Term) -> ObjectMap {
        match term {
            Term::Iri(iri) => ObjectMap::Constant {
                value: ConstantValue::Iri(iri.to_string()),
            },
            Term::Literal {
                value, language, ..
            } => ObjectMap::Constant {
                value: ConstantValue::Literal {
                    value: value.lexical(),
                    datatype: None, // TODO: extract datatype
                    language: language.as_ref().map(std::string::ToString::to_string),
                },
            },
            _ => ObjectMap::Constant {
                value: ConstantValue::Literal {
                    value: String::new(),
                    datatype: None,
                    language: None,
                },
            },
        }
    }
}

#[cfg(all(test, feature = "turtle"))]
mod tests {
    use super::*;
    use fluree_graph_ir::GraphCollectorSink;
    use fluree_graph_turtle::parse as parse_turtle;

    fn parse_r2rml(turtle: &str) -> Graph {
        let mut sink = GraphCollectorSink::new();
        parse_turtle(turtle, &mut sink).unwrap();
        sink.finish()
    }

    #[test]
    fn test_extract_simple_mapping() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#AirlineMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "airlines" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/airline/{id}" ;
                    rr:class ex:Airline
                ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:name ;
                    rr:objectMap [ rr:column "name" ]
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        assert_eq!(triples_maps.len(), 1);

        let tm = &triples_maps[0];
        assert_eq!(tm.iri, "http://example.org/mapping#AirlineMapping");
        assert_eq!(tm.table_name(), Some("airlines"));
        assert_eq!(
            tm.subject_map.template,
            Some("http://example.org/airline/{id}".to_string())
        );
        assert_eq!(tm.subject_map.classes, vec!["http://example.org/Airline"]);
        assert_eq!(tm.predicate_object_maps.len(), 1);

        let pom = &tm.predicate_object_maps[0];
        assert_eq!(
            pom.predicate_map.as_constant(),
            Some("http://example.org/name")
        );
        if let ObjectMap::Column { column, .. } = &pom.object_map {
            assert_eq!(column, "name");
        } else {
            panic!("Expected column object map");
        }
    }

    #[test]
    fn test_extract_ref_object_map() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#RouteMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "routes" ] ;
                rr:subjectMap [ rr:template "http://example.org/route/{id}" ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:airline ;
                    rr:objectMap [
                        rr:parentTriplesMap <http://example.org/mapping#AirlineMapping> ;
                        rr:joinCondition [
                            rr:child "airline_id" ;
                            rr:parent "id"
                        ]
                    ]
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        assert_eq!(triples_maps.len(), 1);

        let tm = &triples_maps[0];
        let pom = &tm.predicate_object_maps[0];

        if let ObjectMap::RefObjectMap(ref_map) = &pom.object_map {
            assert_eq!(
                ref_map.parent_triples_map,
                "http://example.org/mapping#AirlineMapping"
            );
            assert_eq!(ref_map.join_conditions.len(), 1);
            assert_eq!(ref_map.join_conditions[0].child_column, "airline_id");
            assert_eq!(ref_map.join_conditions[0].parent_column, "id");
        } else {
            panic!("Expected RefObjectMap");
        }
    }

    #[test]
    fn test_ref_object_map_requires_join_condition() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#RouteMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "routes" ] ;
                rr:subjectMap [ rr:template "http://example.org/route/{id}" ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:airline ;
                    rr:objectMap [
                        rr:parentTriplesMap <http://example.org/mapping#AirlineMapping>
                    ]
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let result = extractor.extract_all();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("requires at least one rr:joinCondition"),
            "Expected error about missing join condition, got: {err}"
        );
    }

    #[test]
    fn test_composite_join_conditions_sorted() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#FlightMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "flights" ] ;
                rr:subjectMap [ rr:template "http://example.org/flight/{id}" ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:route ;
                    rr:objectMap [
                        rr:parentTriplesMap <http://example.org/mapping#RouteMapping> ;
                        rr:joinCondition [
                            rr:child "dest_airport" ;
                            rr:parent "dest"
                        ] ;
                        rr:joinCondition [
                            rr:child "airline_code" ;
                            rr:parent "airline"
                        ]
                    ]
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        let tm = &triples_maps[0];
        let pom = &tm.predicate_object_maps[0];

        if let ObjectMap::RefObjectMap(ref_map) = &pom.object_map {
            assert_eq!(ref_map.join_conditions.len(), 2);
            // Should be sorted by (child, parent)
            assert_eq!(ref_map.join_conditions[0].child_column, "airline_code");
            assert_eq!(ref_map.join_conditions[0].parent_column, "airline");
            assert_eq!(ref_map.join_conditions[1].child_column, "dest_airport");
            assert_eq!(ref_map.join_conditions[1].parent_column, "dest");
        } else {
            panic!("Expected RefObjectMap");
        }
    }

    #[test]
    fn test_extract_multiple_classes() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#PersonMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "people" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/person/{id}" ;
                    rr:class ex:Person ;
                    rr:class ex:Agent
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        let tm = &triples_maps[0];
        assert_eq!(tm.subject_map.classes.len(), 2);
        assert!(tm
            .subject_map
            .classes
            .contains(&"http://example.org/Person".to_string()));
        assert!(tm
            .subject_map
            .classes
            .contains(&"http://example.org/Agent".to_string()));
    }

    #[test]
    fn test_extract_typed_literal() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            <http://example.org/mapping#PersonMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "people" ] ;
                rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:age ;
                    rr:objectMap [
                        rr:column "age" ;
                        rr:datatype xsd:integer
                    ]
                ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        let tm = &triples_maps[0];
        let pom = &tm.predicate_object_maps[0];

        if let ObjectMap::Column { datatype, .. } = &pom.object_map {
            assert_eq!(
                datatype.as_deref(),
                Some("http://www.w3.org/2001/XMLSchema#integer")
            );
        } else {
            panic!("Expected column object map");
        }
    }

    #[test]
    fn test_normalize_table_name() {
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .

            <http://example.org/mapping#Test> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "namespace/table" ] ;
                rr:subjectMap [ rr:template "http://example.org/{id}" ] .
        "#,
        );

        let extractor = MappingExtractor::new(&graph);
        let triples_maps = extractor.extract_all().unwrap();

        assert_eq!(triples_maps[0].table_name(), Some("namespace.table"));
    }
}
