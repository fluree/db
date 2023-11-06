(ns fluree.db.json-ld.vocab
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.schema :as schema-util]
            [clojure.set :as set]
            [fluree.db.json-ld.ledger :as jld-ledger]))

#?(:clj (set! *warn-on-reflection* true))

;; generates vocabulary/schema pre-cached maps.

(defn map-pred-id+iri
  "In the schema map, we index properties by both integer :id and :iri for easy lookup of either."
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
   #{{:iri 'http://schema.org/Patient', :class true, :subclassOf [1002], :id 1003} ...}
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

(defn extract-ref-sids
  [property-maps]
  (into #{} (keep #(when (true? (:ref? %)) (:id %)) property-maps)))


(def property-sids #{const/$rdf:Property
                     const/$owl:DatatypeProperty
                     const/$owl:ObjectProperty})

(defn initial-property-map
  [sid]
  (if (= sid const/$rdf:type)
    {:id    sid ; rdf:type is predefined, so flakes to build map won't be present.
     :class false
     :idx?  true
     :ref?  true}
    {:id                 sid
     :class              true ; default
     :idx?               true
     :ref?               false ; could go from false->true if defined in vocab but hasn't been used yet
     :subclassOf         #{}
     :equivalentProperty #{}}))

(defn add-subclass
  [prop-map subclass]
  (update prop-map :subclassOf conj subclass))

(defn add-equivalent-property
  [prop-map prop]
  (update prop-map :equivalentProperty conj prop))

(defn update-equivalent-property
  [prop-map sid prop]
  (let [initial-map              (initial-property-map sid)
        with-equivalent-property (fnil add-equivalent-property initial-map)]
    (update prop-map sid with-equivalent-property prop)))

(defn update-all-equivalent-properties
  [prop-map sid o-props]
  (reduce (fn [p-map o-prop]
            (-> p-map
                (update-equivalent-property sid o-prop)
                (update-equivalent-property o-prop sid)))
          prop-map o-props))

(defn update-equivalent-properties
  [pred-map sid obj]
  (let [s-props (-> pred-map
                    (get-in [sid :equivalentProperty])
                    (conj sid))
        o-props (-> pred-map
                    (get-in [obj :equivalentProperty])
                    (conj obj))]
    (reduce (fn [p-map s-prop]
              (update-all-equivalent-properties p-map s-prop o-props))
            pred-map s-props)))

(defn update-pred-map
  [pred-map vocab-flake]
  (let [[sid pid obj]   ((juxt flake/s flake/p flake/o) vocab-flake)
        initial-map     (initial-property-map sid)
        with-properties (fnil assoc initial-map)
        with-subclass   (fnil add-subclass initial-map)]
    (cond
      (= const/$xsd:anyURI pid)
      (update pred-map sid with-properties :iri obj)

      (= const/$rdf:type pid)
      (if (property-sids obj)
        (if (= const/$owl:ObjectProperty obj)
          (update pred-map sid with-properties :class false, :ref? true)
          (update pred-map sid with-properties :class false))
        (if (= const/$xsd:anyURI obj)
          (update pred-map sid with-properties :class false, :ref? true)
          ;; it is a class, but we already did :class true as a default
          pred-map))

      (= const/$rdfs:subClassOf pid)
      (update pred-map sid with-subclass obj)

      (= const/$_predicate:equivalentProperty pid)
      (update-equivalent-properties pred-map sid obj)

      :else pred-map)))

(defn with-vocab-flakes
  [pred-map vocab-flakes]
  (let [new-pred-map  (reduce update-pred-map pred-map vocab-flakes)]
    (reduce-kv (fn [preds k v]
                 (if (number? k)
                   (assoc preds k v, (:iri v) v)
                   preds))
               {"@type" {:iri  "@type"
                         :ref? true
                         :idx? true
                         :id   const/$rdf:type}}
               new-pred-map)))

(defn refresh-subclasses
  [{:keys [pred] :as schema}]
  (assoc schema :subclasses (delay (calc-subclass pred))))

(defn update-with*
  [schema t vocab-flakes]
  (-> schema
      (assoc :t t)
      (update :pred with-vocab-flakes vocab-flakes)
      refresh-subclasses))

(defn update-with
  "When creating a new db from a transaction, merge new schema changes
  into existing schema of previous db."
  [schema db-t new-refs vocab-flakes]
  (if (empty? vocab-flakes)
    schema
    (-> schema
        (update :refs into new-refs)
        (update-with* db-t vocab-flakes))))

(defn base-schema
  []
  (let [coll {-1           {:name "_tx" :id -1 :sid -1}
              "_tx"        {:name "_tx" :id -1 :sid -1}
              0            {:name "_predicate" :id 0 :sid nil}
              "_predicate" {:name "_predicate" :id 0 :sid nil}
              11           {:name "_default" :id 11 :sid nil}
              "_default"   {:name "_default" :id 11 :sid nil}}
        pred (map-pred-id+iri [{:iri const/iri-id
                                :idx? true
                                :id   const/$xsd:anyURI}
                               {:iri  "@type"
                                :ref? true
                                :idx? true
                                :id   const/$rdf:type}
                               {:iri  "http://www.w3.org/2000/01/rdf-schema#Class"
                                :ref? true
                                :idx? true
                                :id   const/$rdfs:Class}])]
    {:t           0
     :refs        #{}
     :coll        coll
     :pred        pred
     :shapes      (atom {:class {} ; TODO: Does this need to be an atom?
                         :pred  {}})
     :prefix      {}
     :fullText    #{}
     :subclasses  (delay {})}))

(defn reset-shapes
  "Resets the shapes cache - called when new shapes added to db"
  [{:keys [shapes] :as _schema}]
  (reset! shapes {:class {}
                  :pred  {}}))

(defn vocab-map
  "Returns a map of the schema for a db to allow quick lookups of schema properties.
  Schema is a map with keys:
  - :t - the 't' value when schema built, allows schema equality checks
  - :coll - collection info, mapping cid->name and name->cid all within the same map
  - :pred - predicate info, mapping pid->properties and name->properties for quick lookup based on id or name respectively
  - :fullText - contains predicate ids that need fulltext search
  "
  [{:keys [t] :as db}]
  (go-try
    (let [vocab-flakes (<? (query-range/index-range db :spot
                                                    >= [schema-util/schema-sid-end]
                                                    <= [0]))
          base-schema  (base-schema)
          schema       (update-with* base-schema t vocab-flakes)
          refs         (extract-ref-sids (:pred schema))]
      (-> schema
          (assoc :refs refs)))))

(defn refresh-schema
  "Updates the schema map of a db."
  [db]
  (go-try
    (let [schema  (<? (vocab-map db))]
      (assoc db :schema schema))))

(defn predicate-sids
  "Extract predicate sids from flakes."
  [flakes]
  (into #{}
        (comp (filter flake/op)
              (map
                (fn [f]
                  (let [p (flake/p f)
                        o (flake/o f)]
                    (->> [p
                          ;; if p is a predicate ref, we know o is a predicate sid as well
                          (when (contains? jld-ledger/predicate-refs p)
                            o)
                          ;; if p (type) has an o that says s is a predicate, include s as well
                          (when (and (= p const/$rdf:type)
                                     (jld-ledger/class-or-property-sid o))
                            (flake/s f))]
                         (remove nil?)))))
              cat)
        flakes))

(defn pred-dt-constraints
  "Collect any shacl datatype constraints and the predicates they apply to."
  [new-flakes]
  (loop [[s-flakes & r] (partition-by flake/s new-flakes)
         res []]
    (println "DEP s-flakes" (pr-str s-flakes))
    (if s-flakes
      (if-let [dt-constraints (->> s-flakes
                                   (filterv #(= const/$sh:datatype (flake/p %)))
                                   (mapv #(flake/o %))
                                   (first))]
        (let [path (->> s-flakes
                        (filterv #(= const/$sh:path (flake/p %)))
                        (sort-by #(:i (flake/m %)))
                        (mapv #(flake/o %))
                        (last))]
          (recur r (conj res [path dt-constraints])))
        (recur r res))
      res)))

(defn add-pred-datatypes
  "Add a :datatype key to the pred meta map for any predicates with a sh:datatype
  constraint. Only one datatype constraint can be valid for a given datatype, most
  recent wins."
  [{:keys [pred] :as schema} pred-tuples]
  (reduce (fn [schema [pid dt]]
            (let [{:keys [iri] :as pred-meta} (-> schema :pred (get pid)
                                                  (assoc :datatype dt))]
              (-> schema
                  (assoc-in [:pred pid] pred-meta)
                  (assoc-in [:pred iri] pred-meta))))
          schema
          pred-tuples))

(defn build-schema
  [vocab-flakes t]
  (let [base-schema (base-schema)
        schema      (update-with* base-schema t vocab-flakes)
        refs        (extract-ref-sids (:pred schema))]
    (-> schema
        (assoc :refs refs))))

(defn hydrate-schema
  "Updates the :schema key of a by processing just the vocabulary flakes out of the new flakes."
  [db new-flakes]
  (let [pred-sids    (predicate-sids new-flakes)
        vocab-flakes (filterv #(pred-sids (flake/s %)) new-flakes)
        {:keys [t refs coll pred shapes prefix fullText subclasses]}
        (-> (build-schema vocab-flakes (:t db))
            (add-pred-datatypes (pred-dt-constraints new-flakes)))]
    (-> db
        (assoc-in [:schema :t] t)
        (update-in [:schema :refs] into refs)
        (update-in [:schema :coll] (partial merge-with merge) coll)
        (update-in [:schema :pred] (partial merge-with merge) pred)
        (update-in [:schema :prefix] merge prefix)
        (update-in [:schema :fullText] into fullText)
        (assoc-in [:schema :subclasses] subclasses)
        (assoc-in [:schema :shapes] shapes))))

(defn load-schema
  [{:keys [preds t] :as db}]
  (go-try
    (loop [[[pred-sid datatype] & r] preds
           vocab-flakes (flake/sorted-set-by flake/cmp-flakes-spot)]
      (if pred-sid
        (let [pred-flakes (<? (query-range/index-range db :spot = [pred-sid]))]
          (recur r (into vocab-flakes pred-flakes)))
        (-> (build-schema vocab-flakes (:t db))
            ;; only use predicates that have a dt
            (add-pred-datatypes (filterv #(> (count %) 1) preds)))))))
