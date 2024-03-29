(ns fluree.db.json-ld.ledger
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.core :as util]
            [clojure.set :as set]))

;; methods to link/trace back a ledger and return flakes
#?(:clj (set! *warn-on-reflection* true))

(def class+property-iris #{const/iri-class
                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
                           "http://www.w3.org/2002/07/owl#Class"
                           "http://www.w3.org/2002/07/owl#ObjectProperty"
                           "http://www.w3.org/2002/07/owl#DatatypeProperty"})


(defn class-or-property?
  [{:keys [type] :as _node}]
  (some class+property-iris (util/sequential type)))

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
          "http://www.w3.org/ns/shacl#NodeShape"                const/$sh:NodeShape
          "http://www.w3.org/ns/shacl#PropertyShape"            const/$sh:PropertyShape
          "http://www.w3.org/ns/shacl#IRI"                      const/$sh:IRI
          "http://www.w3.org/ns/shacl#IRIOrLiteral"             const/$sh:IRIOrLiteral
          "http://www.w3.org/ns/shacl#BlankNodeOrIRI"           const/$sh:BlankNodeOrIRI
          "http://www.w3.org/ns/shacl#BlankNode"                const/$sh:BlankNode
          "http://www.w3.org/ns/shacl#BlankNodeOrLiteral"       const/$sh:BlankNodeOrLiteral
          "http://www.w3.org/ns/shacl#Literal"                  const/$sh:Literal
          "http://www.w3.org/ns/shacl#targetClass"              const/$sh:targetClass
          "http://www.w3.org/ns/shacl#targetNode"               const/$sh:targetNode
          "http://www.w3.org/ns/shacl#targetObjectsOf"          const/$sh:targetObjectsOf
          "http://www.w3.org/ns/shacl#targetSubjectsOf"         const/$sh:targetSubjectsOf
          "http://www.w3.org/ns/shacl#closed"                   const/$sh:closed
          "http://www.w3.org/ns/shacl#ignoredProperties"        const/$sh:ignoredProperties
          "http://www.w3.org/ns/shacl#node"                     const/$sh:node
          "http://www.w3.org/ns/shacl#property"                 const/$sh:property
          "http://www.w3.org/ns/shacl#path"                     const/$sh:path
          "http://www.w3.org/ns/shacl#inversePath"              const/$sh:inversePath
          "http://www.w3.org/ns/shacl#alternativePath"          const/$sh:alternativePath
          "http://www.w3.org/ns/shacl#zeroOrMorePath"           const/$sh:zeroOrMorePath
          "http://www.w3.org/ns/shacl#oneOrMorePath"            const/$sh:oneOrMorePath
          "http://www.w3.org/ns/shacl#zeroOrOnePath"            const/$sh:zeroOrOnePath
          "http://www.w3.org/ns/shacl#minCount"                 const/$sh:minCount
          "http://www.w3.org/ns/shacl#maxCount"                 const/$sh:maxCount
          "http://www.w3.org/ns/shacl#datatype"                 const/$sh:datatype
          "http://www.w3.org/ns/shacl#nodeKind"                 const/$sh:nodeKind
          "http://www.w3.org/ns/shacl#minLength"                const/$sh:minLength
          "http://www.w3.org/ns/shacl#maxLength"                const/$sh:maxLength
          "http://www.w3.org/ns/shacl#equals"                   const/$sh:equals
          "http://www.w3.org/ns/shacl#lessThan"                 const/$sh:lessThan
          "http://www.w3.org/ns/shacl#lessThanOrEquals"         const/$sh:lessThanOrEquals
          "http://www.w3.org/ns/shacl#disjoint"                 const/$sh:disjoint
          "http://www.w3.org/ns/shacl#pattern"                  const/$sh:pattern
          "http://www.w3.org/ns/shacl#flags"                    const/$sh:flags
          "http://www.w3.org/ns/shacl#languageIn"               const/$sh:languageIn
          "http://www.w3.org/ns/shacl#uniqueLang"               const/$sh:uniqueLang
          "http://www.w3.org/ns/shacl#class"                    const/$sh:class
          "http://www.w3.org/ns/shacl#hasValue"                 const/$sh:hasValue
          "http://www.w3.org/ns/shacl#in"                       const/$sh:in
          "http://www.w3.org/ns/shacl#minExclusive"             const/$sh:minExclusive
          "http://www.w3.org/ns/shacl#minInclusive"             const/$sh:minInclusive
          "http://www.w3.org/ns/shacl#maxExclusive"             const/$sh:maxExclusive
          "http://www.w3.org/ns/shacl#maxInclusive"             const/$sh:maxInclusive
          "http://www.w3.org/ns/shacl#not"                      const/$sh:not
          "http://www.w3.org/ns/shacl#and"                      const/$sh:and
          "http://www.w3.org/ns/shacl#or"                       const/$sh:or
          "http://www.w3.org/ns/shacl#xone"                     const/$sh:xone
          "http://www.w3.org/ns/shacl#qualifiedValueShape"      const/$sh:qualifiedValueShape
          "http://www.w3.org/ns/shacl#qualifiedMinCount"        const/$sh:qualifiedMinCount
          "http://www.w3.org/ns/shacl#qualifiedMaxCount"        const/$sh:qualifiedMaxCount
          "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint" const/$sh:qualifiedValueShapesDisjoint
          ;; fluree
          const/iri-role                                        const/$f:role
          const/iri-target-class                                const/$fluree:targetClass}))

(def class-or-property-sid
  (into #{} (map predefined-properties class+property-iris)))

(def predefined-sids
  (set/map-invert predefined-properties))

(defn predefined-sids-compact
  "Allows predefined sids to be mapped to values based on supplied compacting function
  generated from a context"
  [compact-fn]
  (reduce-kv
    (fn [acc sid iri]
      (let [compacted-iri (json-ld/compact iri compact-fn)]
        (assoc acc sid compacted-iri)))
    {}
    predefined-sids))

(def predicate-refs
  "The following predicates have objects that are refs to other predicates."
  #{const/$fluree:targetClass
    const/$owl:equivalentProperty
    const/$rdfs:Class
    const/$rdfs:subClassOf
    const/$sh:alternativePath
    const/$sh:class
    const/$sh:datatype
    const/$sh:disjoint
    const/$sh:equals
    const/$sh:ignoredProperties
    const/$sh:inversePath
    const/$sh:lessThan
    const/$sh:lessThanOrEquals
    const/$sh:oneOrMorePath
    const/$sh:path
    const/$sh:targetClass
    const/$sh:targetObjectsOf
    const/$sh:targetSubjectsOf
    const/$sh:zeroOrMorePath
    const/$sh:zeroOrOnePath
    const/$rdf:type})
