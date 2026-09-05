//! R2RML mapping extractor
//!
//! Extracts TriplesMap definitions from a Graph IR.

use std::collections::{HashMap, HashSet};

use fluree_graph_ir::{Graph, Term, Triple};

use crate::error::{R2rmlError, R2rmlResult};
use crate::mapping::{
    ConstantValue, GraphMap, JoinCondition, LogicalTable, ObjectMap, PredicateMap,
    PredicateObjectMap, RefObjectMap, SubjectMap, TermType, TriplesMap,
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
        let mut seen: HashSet<&str> = HashSet::new();

        // Find all subjects that are rdf:type rr:TriplesMap
        for triple in self.graph.iter() {
            if triple.p.as_iri() == Some(R2RML::RDF_TYPE)
                && triple.o.as_iri() == Some(R2RML::TRIPLES_MAP)
            {
                if let Some(subj_iri) = triple.s.as_iri() {
                    // Each TriplesMap IRI is extracted exactly once. A repeated
                    // `a rr:TriplesMap` triple for an already-extracted subject is
                    // harmless redundancy and is skipped here.
                    if !seen.insert(subj_iri) {
                        continue;
                    }

                    // Hardening: a single TriplesMap IRI must carry exactly one
                    // logical table and one subject map. More than one means two
                    // or more `rr:TriplesMap` definitions collapsed onto the same
                    // IRI (classically: idiomatic relative `<#fragment>` subjects
                    // resolved against `@base` to the same IRI) and silently
                    // merged into first-wins table/subject + union-of-POMs data.
                    // Reject the collision loudly instead of returning
                    // plausible-but-wrong triples.
                    self.ensure_no_collision(subj_iri)?;

                    let tm = self.extract_triples_map(subj_iri)?;
                    triples_maps.push(tm);
                }
            }
        }

        Ok(triples_maps)
    }

    /// Reject a TriplesMap IRI that carries more than one logical table or
    /// subject map — the signature of two `rr:TriplesMap` subjects colliding to
    /// the same IRI and being merged.
    fn ensure_no_collision(&self, tm_iri: &str) -> R2rmlResult<()> {
        let triples = self.get_triples_for_subject(tm_iri);

        let table_count = triples
            .iter()
            .filter(|t| t.p.as_iri() == Some(R2RML::LOGICAL_TABLE))
            .count();
        let subject_count = triples
            .iter()
            .filter(|t| t.p.as_iri() == Some(R2RML::SUBJECT_MAP))
            .count();

        if table_count > 1 || subject_count > 1 {
            return Err(R2rmlError::DuplicateTriplesMap(format!(
                "{tm_iri} (found {table_count} rr:logicalTable and {subject_count} rr:subjectMap \
                 definitions). Two or more rr:TriplesMap subjects resolve to this IRI and would be \
                 silently merged (first-wins table/subject, union of predicate-object maps). Give \
                 each TriplesMap a distinct subject IRI — a common cause is relative <#fragment> \
                 references collapsing against @base."
            )));
        }

        Ok(())
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

        // rr:sqlQuery — scanned as a derived table by SQL graph sources;
        // Iceberg-backed sources refuse the alias at registration.
        if let Some(query) = self.find_object_optional(&table_triples, R2RML::SQL_QUERY) {
            if let Some(sql) = self.term_to_string(&query) {
                if sql.trim().is_empty() {
                    return Err(R2rmlError::InvalidValue {
                        property: "rr:sqlQuery".to_string(),
                        message: "query text is empty".to_string(),
                    });
                }
                return Ok(LogicalTable::sql_query(sql));
            }
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

        // Extract rr:graph / rr:graphMap (subject-map-level named-graph routing).
        subject_map.graph_map = self.extract_graph_map(&sm_triples)?;

        Ok(subject_map)
    }

    /// Extract a graph map from a term's triples: `rr:graph <iri>` (constant
    /// shortcut) or `rr:graphMap [ rr:template | rr:column | rr:constant ]`.
    /// `Ok(None)` means neither is present, so the triples land in the default
    /// graph. A graph term is always an IRI, so there is no term-type to parse.
    ///
    /// **Support here is deliberately a subset of R2RML, and the constructs
    /// outside it are refused rather than ignored.** Silently dropping a graph
    /// map is the failure mode with data consequences: `materialize_graph_from_batch`
    /// keys the accumulator on the resolved graph, so a dropped map leaves every
    /// row in the default graph, and two partitions holding the same subject IRI
    /// then collapse onto one key and overwrite each other per predicate. A
    /// mapping that asks for routing and silently gets none is worse than one
    /// told it cannot have it, so each case below returns an error naming the
    /// construct it refused.
    fn extract_graph_map(&self, triples: &[&Triple]) -> R2rmlResult<Option<GraphMap>> {
        let graph_shortcuts = self.find_objects(triples, R2RML::GRAPH);
        let graph_maps = self.find_objects(triples, R2RML::GRAPH_MAP);

        // R2RML treats rr:graph and rr:graphMap as cumulative and repeatable — a
        // term map may name several graphs and the triple goes into all of them.
        // Exactly one is implemented, so anything asking for more has to fail
        // rather than have all but the first silently discarded.
        if graph_shortcuts.len() + graph_maps.len() > 1 {
            return Err(R2rmlError::Unsupported(format!(
                "multiple graph maps on one term map ({} rr:graph + {} rr:graphMap): \
                 R2RML treats these as cumulative, but only a single graph per term \
                 map is supported. Use one rr:graph or one rr:graphMap.",
                graph_shortcuts.len(),
                graph_maps.len()
            )));
        }

        // rr:graph <iri> — constant shortcut.
        if let Some(graph_obj) = graph_shortcuts.first() {
            if let Some(iri) = self.term_to_iri(graph_obj) {
                if iri == R2RML::DEFAULT_GRAPH {
                    return Err(R2rmlError::Unsupported(
                        "rr:graph rr:defaultGraph is not supported. It would be parsed as \
                         an ordinary constant and mint a named graph called \
                         'http://www.w3.org/ns/r2rml#defaultGraph'. Omit the graph map \
                         entirely to target the default graph."
                            .to_string(),
                    ));
                }
                return Ok(Some(GraphMap::constant(iri)));
            }
        }

        // rr:graphMap [ ... ] — a term map producing the graph IRI.
        let Some(graph_map_obj) = graph_maps.into_iter().next() else {
            return Ok(None);
        };
        let gm_triples = self.get_triples_for_term(&graph_map_obj);
        if let Some(constant_obj) = self.find_object_optional(&gm_triples, R2RML::CONSTANT) {
            if self.term_to_iri(&constant_obj).as_deref() == Some(R2RML::DEFAULT_GRAPH) {
                return Err(R2rmlError::Unsupported(
                    "rr:graphMap [ rr:constant rr:defaultGraph ] is not supported. Omit the \
                     graph map entirely to target the default graph."
                        .to_string(),
                ));
            }
        }
        let mut graph_map = GraphMap::default();

        if let Some(template_obj) = self.find_object_optional(&gm_triples, R2RML::TEMPLATE) {
            if let Some(template) = self.term_to_string(&template_obj) {
                graph_map.template_columns = crate::mapping::extract_template_columns(&template);
                graph_map.template = Some(template);
            }
        }
        if let Some(column_obj) = self.find_object_optional(&gm_triples, R2RML::COLUMN) {
            if let Some(col) = self.term_to_string(&column_obj) {
                graph_map.column = Some(col);
            }
        }
        if let Some(constant_obj) = self.find_object_optional(&gm_triples, R2RML::CONSTANT) {
            if let Some(iri) = self.term_to_iri(&constant_obj) {
                graph_map.constant = Some(iri);
            }
        }

        // A graphMap that parsed no usable value source is a malformed mapping,
        // not an absent one. Treating it as absent is exactly how routing
        // silently degrades to the default graph.
        if graph_map.is_empty() {
            return Err(R2rmlError::Unsupported(
                "rr:graphMap has no rr:template, rr:column or rr:constant, so it names no \
                 graph. Omit it to target the default graph."
                    .to_string(),
            ));
        }
        Ok(Some(graph_map))
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

        // R2RML also allows a graph map on a predicate-object map, scoping just
        // that map's triples. `PredicateObjectMap` has no field for one and the
        // materializer resolves the graph once per row from the SUBJECT map, so a
        // POM-level graph map would be read and then ignored — the silent
        // cross-partition collapse described on `extract_graph_map`. Refuse it by
        // name; implementing it later can relax this without changing what the
        // error means today.
        if !self.find_objects(&pom_triples, R2RML::GRAPH).is_empty()
            || !self.find_objects(&pom_triples, R2RML::GRAPH_MAP).is_empty()
        {
            return Err(R2rmlError::Unsupported(
                "rr:graph / rr:graphMap on a predicate-object map is not supported. Only \
                 subject-map-level graph maps are honored; a POM-level graph map would be \
                 ignored and its triples would land in the subject's graph. Move the graph \
                 map to the subject map."
                    .to_string(),
            ));
        }

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
        sink.into_graph()
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
        // No graph map -> triples land in the default graph.
        assert!(tm.subject_map.graph_map.is_none());
    }

    #[test]
    fn test_extract_subject_graph_map_template() {
        // rr:graphMap [ rr:template ... ] on the subject map routes every row's
        // triples into a per-row named graph (e.g. one graph per tenant/user).
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#ActorMapping> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "actor" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:class ex:Profile ;
                    rr:graphMap [ rr:template "http://example.org/graph/tenant/{tenant_id}/user/{user_id}" ]
                ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:name ;
                    rr:objectMap [ rr:column "as_name" ]
                ] .
        "#,
        );
        let extractor = MappingExtractor::new(&graph);
        let tms = extractor.extract_all().unwrap();
        let gm = tms[0]
            .subject_map
            .graph_map
            .as_ref()
            .expect("graph map parsed");
        assert_eq!(
            gm.template.as_deref(),
            Some("http://example.org/graph/tenant/{tenant_id}/user/{user_id}")
        );
        assert_eq!(gm.template_columns, vec!["tenant_id", "user_id"]);
        assert!(gm.constant.is_none() && gm.column.is_none());
    }

    #[test]
    fn test_extract_subject_graph_constant_shortcut() {
        // rr:graph <iri> is the constant-graph shortcut for rr:graphMap [ rr:constant <iri> ].
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graph ex:g1
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        let extractor = MappingExtractor::new(&graph);
        let tms = extractor.extract_all().unwrap();
        let gm = tms[0]
            .subject_map
            .graph_map
            .as_ref()
            .expect("graph map parsed");
        assert_eq!(gm.constant.as_deref(), Some("http://example.org/g1"));
        assert!(gm.template.is_none() && gm.column.is_none());
    }

    // ------------------------------------------------------------------
    // Named-graph routing: what is refused, and why refusing beats ignoring.
    //
    // Each of these parsed successfully before, yielding `None` — a mapping that
    // asked for routing, got none, and said nothing. Every assertion below names
    // the construct in the message so the mapping author can find it.
    // ------------------------------------------------------------------

    /// Extract the message of an `Unsupported` error, or panic saying what came
    /// back instead. Asserting on the message rather than just `is_err()` is what
    /// stops one refusal standing in for another.
    fn unsupported_message(r2rml: &str) -> String {
        let graph = parse_r2rml(r2rml);
        let extractor = MappingExtractor::new(&graph);
        match extractor.extract_all() {
            Err(R2rmlError::Unsupported(msg)) => msg,
            Err(other) => panic!("expected Unsupported, got {other:?}"),
            Ok(_) => panic!("this mapping must be refused, not silently accepted"),
        }
    }

    #[test]
    fn refuses_multiple_cumulative_graph_maps() {
        // R2RML would put the triples in BOTH graphs. One graph is implemented,
        // so accepting this would discard the second without saying so.
        let msg = unsupported_message(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graph ex:g1 ;
                    rr:graph ex:g2
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        assert!(
            msg.contains("multiple graph maps"),
            "message must name the construct: {msg}"
        );
    }

    #[test]
    fn refuses_rr_graph_default_graph() {
        // As an ordinary constant this mints a named graph literally called
        // 'http://www.w3.org/ns/r2rml#defaultGraph' — the opposite of what the
        // mapping asked for, and silent.
        let msg = unsupported_message(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graph rr:defaultGraph
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        assert!(
            msg.contains("rr:defaultGraph"),
            "message must name the construct: {msg}"
        );
    }

    #[test]
    fn refuses_graph_map_constant_default_graph() {
        // The long form of the same mistake.
        let msg = unsupported_message(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graphMap [ rr:constant rr:defaultGraph ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        assert!(
            msg.contains("rr:defaultGraph"),
            "message must name the construct: {msg}"
        );
    }

    #[test]
    fn refuses_a_graph_map_that_names_no_graph() {
        // No template, column or constant: malformed, not absent. Treating it as
        // absent is precisely how routing degrades to the default graph unseen.
        let msg = unsupported_message(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graphMap [ rr:termType rr:IRI ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        assert!(
            msg.contains("no rr:template, rr:column or rr:constant"),
            "message must say which value sources were missing: {msg}"
        );
    }

    #[test]
    fn refuses_a_predicate_object_map_graph_map() {
        // Valid R2RML that scopes just this map's triples. `PredicateObjectMap`
        // has no field for it and the graph is resolved once per row from the
        // subject map, so it would be read and ignored.
        let msg = unsupported_message(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [ rr:template "http://example.org/{id}" ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:p ;
                    rr:objectMap [ rr:column "c" ] ;
                    rr:graph ex:g1
                ] .
        "#,
        );
        assert!(
            msg.contains("predicate-object map"),
            "message must name the construct: {msg}"
        );
    }

    #[test]
    fn a_single_subject_graph_map_is_still_accepted() {
        // The refusals must not fire on the supported shape — this is the guard
        // against over-rejecting, and it is the case the feature exists for.
        let graph = parse_r2rml(
            r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .

            <http://example.org/mapping#M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "t" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/{id}" ;
                    rr:graphMap [ rr:template "http://example.org/g/{tenant_id}" ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:p ; rr:objectMap [ rr:column "c" ] ] .
        "#,
        );
        let tms = MappingExtractor::new(&graph)
            .extract_all()
            .expect("a single subject-map graph map is supported");
        let gm = tms[0]
            .subject_map
            .graph_map
            .as_ref()
            .expect("graph map parsed");
        assert_eq!(
            gm.template.as_deref(),
            Some("http://example.org/g/{tenant_id}")
        );
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
