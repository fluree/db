(ns fluree.db.json-ld.vocab
  "Generates vocabulary/schema pre-cached maps."
  (:require [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

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
          "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint" const/sh_qualifiedValueShapesDisjoint}))

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

(defn build-pred-map
  "In the schema map, we index properties by both sid :id and :iri for easy
  lookup of either."
  [properties]
  (reduce
   (fn [acc prop-map]
     (assoc acc (:id prop-map) prop-map
            (:iri prop-map) prop-map))
   {} properties))

(defn- recur-sub-classes
  "Once an initial parent->child relationship is established, recursively place
  children into parents to return a sorted set of all sub-classes regardless of depth
  Sorted set is used to ensure consistent query results.

  First takes predicate items and makes a map like this of parent -> children:
  {100 [200 201]
   201 [300 301]}

  Then recursively gets children's children to return a map like this:
  {100 #{200 201 300 301}
   201 #{300 301}}

   Initial pred-items argument looks like:
   #{{:id #fluree/SID[8 'address'],
      :iri 'http://schema.org/Patient',
      :subclassOf #{#fluree/SID[8 'location']}
      ...} ...}
   "
  [pred-items]
  (let [subclass-map (reduce
                      (fn [acc class]
                        (if-let [parent-classes (:subclassOf class)]
                          (reduce #(update %1 %2 conj (:id class)) acc parent-classes)
                          acc))
                      {} pred-items)]
    (reduce-kv
     (fn [acc parent children]
       (loop [[child & r] children
              all-children (apply sorted-set children)]
         (if (nil? child)
           (assoc acc parent all-children)
           (if-let [child-children (get subclass-map child)]
             (recur (into child-children r) (into all-children child-children))
             (recur r all-children)))))
     {} subclass-map)))

(defn calc-subclass
  "Calculates subclass map for use with queries for rdf:type."
  [property-maps]
  (let [subclass-map (recur-sub-classes (vals property-maps))]
    ;; map subclasses for both subject-id and iri
    (reduce-kv
     (fn [acc class-id subclasses]
       (let [iri (get-in property-maps [class-id :iri])]
         (assoc acc iri subclasses)))
     subclass-map subclass-map)))

(def ^:const base-property-map
  {:id          nil
   :iri         nil
   :subclassOf  #{}
   :parentProps #{}
   :childProps  #{}})

(defn initial-property-map*
  [iri sid]
  (assoc base-property-map :id sid, :iri iri))

(defn initial-property-map
  [db sid]
  (let [iri (iri/decode-sid db sid)]
    (initial-property-map* iri sid)))

(defn add-subclass
  [prop-map subclass]
  (update prop-map :subclassOf conj subclass))

(defn add-child-properties
  [prop-map child-properties]
  (update prop-map :childProps into child-properties))

(defn add-parent-properties
  [prop-map parent-properties]
  (update prop-map :parentProps into parent-properties))

(defn update-parent-with-children
  [prop-map db parent-prop child-props]
  (let [initial-map       (initial-property-map db parent-prop)
        with-new-children (fnil add-child-properties initial-map)]
    (update prop-map parent-prop with-new-children child-props)))

(defn update-child-with-parents
  [prop-map db child-prop parent-props]
  (let [initial-map      (initial-property-map db child-prop)
        with-new-parents (fnil add-parent-properties initial-map)]
    (update prop-map child-prop with-new-parents parent-props)))

(defn add-new-children-to-parents
  "Adds new :childProps to parents in the schema map all the way up
  the hierarchy"
  [pred-map db all-parents new-child-properties]
  (reduce (fn [p-map parent-prop]
            (update-parent-with-children p-map db parent-prop new-child-properties))
          pred-map all-parents))

(defn add-new-parents-to-children
  "Adds new :parentProps to children in the schema map all the way down
  the hierarchy"
  [pred-map db all-children new-parent-properties]
  (reduce (fn [p-map child-prop]
            (update-child-with-parents p-map db child-prop new-parent-properties))
          pred-map all-children))

(defn update-rdfs-subproperty-of
  "Updates the schema map with the rdfs:subPropertyOf relationship
  between parent and child properties.

  owl:equivalentProperty also uses this, as an equivalent property
  relationship is where each property is a subproperty of the other."
  [pred-map db parent-prop child-prop]
  (let [parent-parents      (get-in pred-map [parent-prop :parentProps])
        child-children      (get-in pred-map [child-prop :childProps])
        new-parent-children (conj child-children child-prop)
        new-child-parents   (conj parent-parents parent-prop)]
    (-> pred-map
        (add-new-children-to-parents db new-child-parents new-parent-children)
        (add-new-parents-to-children db new-parent-children new-child-parents))))

(defn update-related-properties
  "Adds owl:equivalentProperty and rdfs:subPropertyOf rules to the schema map as the
  appropriate equivalent properties for query purposes only.

  For owl:equivalentProperty, the relationships go both ways. e.g.:
  [ex:givenName owl:equivalentProperty ex:firstName]
  Means that two entries must be made for equivalence
   - ex:givenName -> ex:firstName
   - ex:firstName -> ex:givenName.

  For subPropertyOf, the relationship is one way. e.g.:
  [ex:father rdfs:subPropertyOf ex:parent]
   - ex:parent -> ex:father"
  [pred-map db sid pid obj]
  (if (iri/sid? obj)
    (if (= const/$owl:equivalentProperty pid)
      (-> pred-map
          (update-rdfs-subproperty-of db sid obj)
          (update-rdfs-subproperty-of db obj sid))
      (update-rdfs-subproperty-of pred-map db obj sid))
    (do
      (log/warn (str "Triple of ["
                     (iri/decode-sid db sid) " "
                     (iri/decode-sid db pid) " "
                     obj "] is not being enforced because "
                     obj " is not an IRI."
                     (when (string? obj)
                       (str
                        " It is a string, and likely was intended to be "
                        "input as {\"@id\": \"" obj "\"} instead of as a "
                        "string datatype."))))
      pred-map)))

(defn update-pred-map
  [pred-map db vocab-flake]
  (let [[sid pid obj]   ((juxt flake/s flake/p flake/o) vocab-flake)
        initial-map     (initial-property-map db sid)
        with-subclass   (fnil add-subclass initial-map)]
    (cond
      (= const/$rdfs:subClassOf pid)
      (update pred-map sid with-subclass obj)

      (or (= const/$owl:equivalentProperty pid)
          (= const/$rdfs:subPropertyOf pid))
      (update-related-properties pred-map db sid pid obj)

      :else pred-map)))

(def initial-type-map
  (initial-property-map* const/iri-type const/$rdf:type))

(def initial-class-map
  (initial-property-map* const/iri-class const/$rdfs:Class))

(defn with-vocab-flakes
  [pred-map db vocab-flakes]
  (let [new-pred-map  (reduce (fn [pred-map* vocab-flake]
                                (update-pred-map pred-map*  db vocab-flake))
                              pred-map vocab-flakes)]
    (reduce-kv (fn [preds k v]
                 (if (iri/sid? k)
                   (assoc preds k v, (:iri v) v)
                   preds))
               {const/iri-type initial-type-map} new-pred-map)))

(defn refresh-subclasses
  [{:keys [pred] :as schema}]
  (assoc schema :subclasses (delay (calc-subclass pred))))

(defn update-with
  [schema db t vocab-flakes]
  (-> schema
      (assoc :t t)
      (update :pred with-vocab-flakes db vocab-flakes)
      refresh-subclasses))

(defn base-schema
  []
  (let [pred (build-pred-map [initial-type-map initial-class-map])]
    {:t          0
     :pred       pred
     :shapes     (atom {})
     :subclasses (delay {})}))

;; TODO - do we need this any longer?
(defn infer-predicate-ids
  [f]
  (let [[s p o] ((juxt flake/s flake/p flake/o) f)]
    (cond (and (= const/$rdf:type p)
               (contains? class-or-property-sid o))
          [s p]

          (contains? predicate-refs p)
          [p o]

          :else
          [p])))

(defn collect-predicate-ids
  [db flakes]
  (let [pred-map (get-in db [:schema :pred])]
    (into #{}
          (comp (filter flake/op)
                (mapcat infer-predicate-ids)
                (filter (fn [pid]
                          (not (contains? pred-map pid)))))
          flakes)))

(defn descending
  [x y]
  (compare y x))

(defn add-pid
  [preds db pid]
  (if (contains? preds pid)
    preds
    (let [{:keys [iri] :as p-map} (initial-property-map db pid)]
      (assoc preds pid p-map, iri p-map))))

(defn add-predicates
  [pred-map db pids]
  (reduce (fn [pred-map* pid]
            (add-pid pred-map* db pid))
          pred-map pids))

(defn update-schema
  [{:keys [schema t] :as db} pids vocab-flakes]
  (-> schema
      (update :pred add-predicates db pids)
      (update-with db t vocab-flakes)))

(defn hydrate-schema
  "Updates the :schema key of db by processing just the vocabulary flakes out of
  the new flakes."
  [db new-flakes]
  (let [pred-sids      (collect-predicate-ids db new-flakes)
        vocab-flakes   (into #{}
                             (filter (fn [f]
                                       (or (contains? pred-sids (flake/s f))
                                           (contains? predicate-refs (flake/p f)))))
                             new-flakes)
        schema         (update-schema db pred-sids vocab-flakes)]
    (assoc db :schema schema)))

(defn load-schema
  [{:keys [t] :as db} preds]
  (go-try
    (loop [[[pid] & r]  preds
           vocab-flakes (flake/sorted-set-by flake/cmp-flakes-spot)
           pred-map     (-> db :schema :pred)]
      (if pid
        (let [pred-flakes   (<? (query-range/index-range db :spot = [pid]))
              vocab-flakes* (into vocab-flakes pred-flakes)
              pred-map*     (add-pid db pred-map pid)]
          (recur r vocab-flakes* pred-map*))
        (-> db
            :schema
            (assoc :pred pred-map)
            (update-with db t vocab-flakes))))))

;; schema serialization
(def ^:const serialized-pred-keys
  [:id :subclassOf :parentProps :childProps])

(def ^:const serialized-pred-keys-reverse
  (reverse serialized-pred-keys))

(defn serialize-property-set
  [tuple st]
  (if (seq st)
    (conj tuple
          (mapv #(if (iri/sid? %)
                   (iri/serialize-sid %)
                   %)
                st))
    (if (seq tuple) ; if 'tuple' is still empty, keep it that if nothing to add
      (conj tuple nil)
      tuple)))

(defn schema-tuple
  [pred-map]
  (reduce
   (fn [acc next-key]
     (let [next-val (get pred-map next-key)]
       (cond
         (set? next-val)
         (serialize-property-set acc next-val)

         (iri/sid? next-val)
         (conj acc (iri/serialize-sid next-val))

         (nil? next-val)
         (if (empty? acc)
           acc
           (conj acc nil))

         :else
         (conj acc next-val))))
   (list)
   serialized-pred-keys-reverse))

(defn serialize-schema
  "Serializes the schema map to a semi-compact json form which can be stored
  in the index root file, allowing fast reification of the schema map without
  requiring database queries to do so."
  [{:keys [t pred] :as _db-schema}]
  (let [pred-keys (mapv name serialized-pred-keys)
        pred-vals (->> pred
                       (filter #(string? (key %))) ; every pred map is
                                                   ; duplicated for both keys
                                                   ; iri, and sid - keep only 1
                       vals
                       (mapv schema-tuple))]
    {"t"    t
     "pred" {"keys" pred-keys
             "vals" pred-vals}}))

(defn deserialize-pred-tuple
  "Takes list of keys and tuples containing values
  and turns them into a map with the respective keys and tuples"
  [namespace-codes positions pred-vals]
  (let [max-idx (dec (count pred-vals))]
    (loop [[[idx k] & r] positions
           acc base-property-map]
      (let [acc* (if-let [raw-val (nth pred-vals idx)]
                   (case k
                     :id (let [sid (iri/deserialize-sid raw-val)]
                           (assoc acc :id sid
                                  :iri (iri/sid->iri sid namespace-codes)))
                     :subclassOf (assoc acc :subclassOf (into (:subclassOf base-property-map) (map iri/deserialize-sid raw-val)))
                     :parentProps (assoc acc :parentProps (into (:parentProps base-property-map) (map iri/deserialize-sid raw-val)))
                     :childProps (assoc acc :childProps (into (:childProps base-property-map) (map iri/deserialize-sid raw-val)))
                     ;; else
                     acc)
                   acc)]
        (if (= idx max-idx)
          acc*
          (recur r acc*))))))

(defn deserialize-preds
  [namespace-codes serialized-pred-map]
  (let [{pred-keys :keys
         pred-vals :vals} serialized-pred-map
        pred-positions (map-indexed #(vector %1 (keyword %2)) pred-keys)
        pred-maps      (map
                        (partial deserialize-pred-tuple namespace-codes pred-positions)
                        pred-vals)]
    (reduce
     (fn [acc pred-map]
       (assoc acc (:id pred-map) pred-map
              (:iri pred-map) pred-map))
     {}
     pred-maps)))

(defn deserialize-schema
  "Deserializes the schema map from a semi-compact json."
  [serialized-schema namespace-codes]
  (let [{:keys [pred t]} serialized-schema
        pred* (deserialize-preds namespace-codes pred)]
    (-> (base-schema)
        (assoc :t t
               :pred pred*)
        (refresh-subclasses))))
