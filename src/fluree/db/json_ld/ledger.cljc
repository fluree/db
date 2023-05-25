(ns fluree.db.json-ld.ledger
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.core :as util]
            [clojure.set :as set]))

;; methods to link/trace back a ledger and return flakes
#?(:clj (set! *warn-on-reflection* true))

(def class+property-iris #{"http://www.w3.org/2000/01/rdf-schema#Class"
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
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"     const/$rdf:type
          ;; rdfs
          "http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
          "http://www.w3.org/2000/01/rdf-schema#subClassOf"     const/$rdfs:subClassOf
          "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"  const/$rdfs:subPropertyOf
          ;; owl
          "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
          "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
          "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty
          "http://www.w3.org/2002/07/owl#equivalentProperty"    const/$_predicate:equivalentProperty
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
          "http://www.w3.org/ns/shacl#property"                 const/$sh:property
          "http://www.w3.org/ns/shacl#path"                     const/$sh:path
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
          ;; fluree
          "https://ns.flur.ee/ledger#context"                   const/$fluree:context
          const/iri-default-context                             const/$fluree:default-context}))

(def predefined-sids
  (-> predefined-properties
      set/map-invert
      ;; use @type json-ld shorthand instead of rdf:type full URL
      (assoc const/$rdf:type "@type")))

(defn predefined-sids-compact
  "Allows predefined sids to be mapped to values based on supplied compacting function
  generated from a context"
  [compact-fn]
  (reduce-kv
    (fn [acc k v]
      (let [v* (json-ld/compact v compact-fn)]
        (assoc acc k v*)))
    {}
    predefined-sids))

(defn last-pid
  [db]
  (-> db :ecount (get const/$_predicate)))

(defn last-sid
  "Last used sid - so next available would increment result of this by one."
  [db]
  (or (-> db :ecount (get const/$_default))
      ;; decrement because
      (dec (flake/->sid const/$_default 0))))

(defn last-commit-sid
  "Last used sid - so next available would increment result of this by one.
  Commits use a different address space than all other 'default' flakes."
  [db]
  (or (-> db :ecount (get const/$_shard))
      (dec (flake/->sid const/$_shard 0))))

(defn generate-new-sid
  "Generates a new subject ID. If it is known this is a property or class will
  assign the lower range of subject ids."
  [{:keys [id] :as node} referring-pid iris next-pid next-sid]
  (let [new-sid (or
                  (get predefined-properties id)
                  (if (or (class-or-property? node)
                          (#{const/$rdfs:subClassOf
                             const/$sh:path const/$sh:ignoredProperties
                             const/$sh:targetClass
                             const/$sh:targetSubjectsOf const/$sh:targetObjectsOf
                             const/$sh:equals
                             const/$sh:disjoint
                             const/$sh:lessThan
                             const/$sh:lessThanOrEquals}
                           referring-pid))
                    (next-pid)
                    (next-sid)))]
    (vswap! iris assoc id new-sid)
    new-sid))

(defn generate-new-pid
  "Generates a new pid for a property that has not yet been used.
  Optionally 'refs-v' is a volatile! set of refs that gets used in
  the pre-computed :schema of a db, and 'ref?', if truthy, indicates
  if this property should get added to the refs-v."
  [property-iri iris next-pid ref? refs-v]
  (let [new-pid (next-pid)]
    (vswap! iris assoc property-iri new-pid)
    (when ref?
      (vswap! refs-v conj new-pid))
    new-pid))
