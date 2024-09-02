(ns fluree.db.ledger
  (:require [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const predefined-properties
  (merge datatype/default-data-types
         {"http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" const/$rdf:Property
          const/iri-type                                        const/$rdf:type
          const/iri-rdf-type                                    const/$rdf:type
          ;; rdfs
          "http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
          "http://www.w3.org/2000/01/rdf-schema#subClassOf"     const/$rdfs:subClassOf
          "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"  const/$rdfs:subPropertyOf
          ;; owl
          "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
          "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
          "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty
          "http://www.w3.org/2002/07/owl#equivalentProperty"    const/$owl:equivalentProperty
          ;; shacl
          "http://www.w3.org/ns/shacl#NodeShape"                const/sh_NodeShape
          "http://www.w3.org/ns/shacl#PropertyShape"            const/sh_PropertyShape
          "http://www.w3.org/ns/shacl#deactivated"              const/sh_deactivated
          "http://www.w3.org/ns/shacl#IRI"                      const/sh_IRI
          "http://www.w3.org/ns/shacl#IRIOrLiteral"             const/sh_IRIOrLiteral
          "http://www.w3.org/ns/shacl#BlankNodeOrIRI"           const/sh_BlankNodeOrIRI
          "http://www.w3.org/ns/shacl#BlankNode"                const/sh_BlankNode
          "http://www.w3.org/ns/shacl#BlankNodeOrLiteral"       const/sh_BlankNodeOrLiteral
          "http://www.w3.org/ns/shacl#Literal"                  const/sh_Literal
          "http://www.w3.org/ns/shacl#targetClass"              const/sh_targetClass
          "http://www.w3.org/ns/shacl#targetNode"               const/sh_targetNode
          "http://www.w3.org/ns/shacl#targetObjectsOf"          const/sh_targetObjectsOf
          "http://www.w3.org/ns/shacl#targetSubjectsOf"         const/sh_targetSubjectsOf
          "http://www.w3.org/ns/shacl#closed"                   const/sh_closed
          "http://www.w3.org/ns/shacl#ignoredProperties"        const/sh_ignoredProperties
          "http://www.w3.org/ns/shacl#node"                     const/sh_node
          "http://www.w3.org/ns/shacl#property"                 const/sh_property
          "http://www.w3.org/ns/shacl#path"                     const/sh_path
          "http://www.w3.org/ns/shacl#inversePath"              const/sh_inversePath
          "http://www.w3.org/ns/shacl#alternativePath"          const/sh_alternativePath
          "http://www.w3.org/ns/shacl#zeroOrMorePath"           const/sh_zeroOrMorePath
          "http://www.w3.org/ns/shacl#oneOrMorePath"            const/sh_oneOrMorePath
          "http://www.w3.org/ns/shacl#zeroOrOnePath"            const/sh_zeroOrOnePath
          "http://www.w3.org/ns/shacl#minCount"                 const/sh_minCount
          "http://www.w3.org/ns/shacl#maxCount"                 const/sh_maxCount
          "http://www.w3.org/ns/shacl#datatype"                 const/sh_datatype
          "http://www.w3.org/ns/shacl#nodeKind"                 const/sh_nodeKind
          "http://www.w3.org/ns/shacl#minLength"                const/sh_minLength
          "http://www.w3.org/ns/shacl#maxLength"                const/sh_maxLength
          "http://www.w3.org/ns/shacl#equals"                   const/sh_equals
          "http://www.w3.org/ns/shacl#lessThan"                 const/sh_lessThan
          "http://www.w3.org/ns/shacl#lessThanOrEquals"         const/sh_lessThanOrEquals
          "http://www.w3.org/ns/shacl#disjoint"                 const/sh_disjoint
          "http://www.w3.org/ns/shacl#pattern"                  const/sh_pattern
          "http://www.w3.org/ns/shacl#flags"                    const/sh_flags
          "http://www.w3.org/ns/shacl#languageIn"               const/sh_languageIn
          "http://www.w3.org/ns/shacl#uniqueLang"               const/sh_uniqueLang
          "http://www.w3.org/ns/shacl#class"                    const/sh_class
          "http://www.w3.org/ns/shacl#hasValue"                 const/sh_hasValue
          "http://www.w3.org/ns/shacl#in"                       const/sh_in
          "http://www.w3.org/ns/shacl#minExclusive"             const/sh_minExclusive
          "http://www.w3.org/ns/shacl#minInclusive"             const/sh_minInclusive
          "http://www.w3.org/ns/shacl#maxExclusive"             const/sh_maxExclusive
          "http://www.w3.org/ns/shacl#maxInclusive"             const/sh_maxInclusive
          "http://www.w3.org/ns/shacl#not"                      const/sh_not
          "http://www.w3.org/ns/shacl#and"                      const/sh_and
          "http://www.w3.org/ns/shacl#or"                       const/sh_or
          "http://www.w3.org/ns/shacl#xone"                     const/sh_xone
          "http://www.w3.org/ns/shacl#qualifiedValueShape"      const/sh_qualifiedValueShape
          "http://www.w3.org/ns/shacl#qualifiedMinCount"        const/sh_qualifiedMinCount
          "http://www.w3.org/ns/shacl#qualifiedMaxCount"        const/sh_qualifiedMaxCount
          "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint" const/sh_qualifiedValueShapesDisjoint
          }))

(def class+property-iris #{const/iri-class
                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
                           "http://www.w3.org/2002/07/owl#Class"
                           "http://www.w3.org/2002/07/owl#ObjectProperty"
                           "http://www.w3.org/2002/07/owl#DatatypeProperty"})

(def class-or-property-sid
  (into #{} (map predefined-properties class+property-iris)))

(def predicate-refs
  "The following predicates have objects that are refs to other predicates."
  #{const/$owl:equivalentProperty
    const/$rdfs:Class
    const/$rdfs:subClassOf
    const/$rdfs:subPropertyOf
    const/sh_alternativePath
    const/sh_class
    const/sh_datatype
    const/sh_disjoint
    const/sh_equals
    const/sh_ignoredProperties
    const/sh_inversePath
    const/sh_lessThan
    const/sh_lessThanOrEquals
    const/sh_oneOrMorePath
    const/sh_path
    const/sh_targetClass
    const/sh_targetObjectsOf
    const/sh_targetSubjectsOf
    const/sh_zeroOrMorePath
    const/sh_zeroOrOnePath
    const/$rdf:type})

(defprotocol iCommit
  ;; retrieving/updating DBs
  (-commit! [ledger db] [ledger db opts] "Commits a db to a ledger.")
  (-notify [ledger commit-notification] "Notifies of an updated commit for a given ledger, will attempt cached ledger."))

(defprotocol iLedger
  ;; retrieving/updating DBs
  (-db [ledger] "Returns queryable db with specified options")
  ;; committing
  (-status [ledger] [ledger branch] "Returns status for branch (default branch if nil)")
  (-close [ledger] "Shuts down ledger processes and clears used resources."))
