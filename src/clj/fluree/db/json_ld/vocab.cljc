(ns fluree.db.json-ld.vocab
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.iri :as iri]))

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
  (into #{}
        (keep (fn [p-map]
                (when (-> p-map :ref? true?)
                  (:id p-map))))
        property-maps))


(def property-sids #{const/$rdf:Property
                     const/$owl:DatatypeProperty
                     const/$owl:ObjectProperty})

(defn initial-property-map
  [db sid]
  (if (= sid const/$rdf:type)
    {:id    sid ; rdf:type is predefined, so flakes to build map won't be present.
     :class false
     :ref?  true}
    (let [iri (iri/sid->iri sid (:namespace-codes db))]
      {:id                 sid
       :iri                iri
       :class              true ; default
       :ref?               false ; could go from false->true if defined in vocab but hasn't been used yet
       :subclassOf         #{}
       :equivalentProperty #{}})))

(defn add-subclass
  [prop-map subclass]
  (update prop-map :subclassOf conj subclass))

(defn add-equivalent-property
  [prop-map prop]
  (update prop-map :equivalentProperty conj prop))

(defn update-equivalent-property
  [db prop-map sid prop]
  (let [initial-map              (initial-property-map db sid)
        with-equivalent-property (fnil add-equivalent-property initial-map)]
    (update prop-map sid with-equivalent-property prop)))

(defn update-all-equivalent-properties
  [db prop-map sid o-props]
  (reduce (fn [p-map o-prop]
            (as-> p-map props
              (update-equivalent-property db props sid o-prop)
              (update-equivalent-property db props o-prop sid)))
          prop-map o-props))

(defn update-equivalent-properties
  [db pred-map sid obj]
  (let [s-props (-> pred-map
                    (get-in [sid :equivalentProperty])
                    (conj sid))
        o-props (-> pred-map
                    (get-in [obj :equivalentProperty])
                    (conj obj))]
    (reduce (fn [p-map s-prop]
              (update-all-equivalent-properties db p-map s-prop o-props))
            pred-map s-props)))

(defn update-pred-map
  [db pred-map vocab-flake]
  (let [[sid pid obj]   ((juxt flake/s flake/p flake/o) vocab-flake)
        initial-map     (initial-property-map db sid)
        with-properties (fnil assoc initial-map)
        with-subclass   (fnil add-subclass initial-map)]
    (cond
      (= const/$rdf:type pid)
      (if (contains? property-sids obj)
        (if (= const/$owl:ObjectProperty obj)
          (update pred-map sid with-properties :class false, :ref? true)
          (update pred-map sid with-properties :class false))
        (if (= const/$xsd:anyURI obj)
          (update pred-map sid with-properties :class false, :ref? true)
          ;; it is a class, but we already did :class true as a default
          pred-map))

      (= const/$rdfs:subClassOf pid)
      (update pred-map sid with-subclass obj)

      (= const/$owl:equivalentProperty pid)
      (update-equivalent-properties db pred-map sid obj)

      :else pred-map)))

(defn with-vocab-flakes
  [db pred-map vocab-flakes]
  (let [new-pred-map  (reduce (partial update-pred-map db) pred-map vocab-flakes)]
    (reduce-kv (fn [preds k v]
                 (if (iri/sid? k)
                   (assoc preds k v, (:iri v) v)
                   preds))
               {"@type" {:iri  "@type"
                         :ref? true
                         :id   const/$rdf:type}}
               new-pred-map)))

(defn refresh-subclasses
  [{:keys [pred] :as schema}]
  (assoc schema :subclasses (delay (calc-subclass pred))))

(defn update-with*
  [schema db t vocab-flakes]
  (-> schema
      (assoc :t t)
      (update :pred (partial with-vocab-flakes db) vocab-flakes)
      refresh-subclasses))

(defn update-with
  "When creating a new db from a transaction, merge new schema changes
  into existing schema of previous db."
  [schema db db-t new-refs vocab-flakes]
  (if (empty? vocab-flakes)
    schema
    (-> schema
        (update :refs into new-refs)
        (update-with* db db-t vocab-flakes))))

(defn base-schema
  []
  (let [pred (map-pred-id+iri [{:iri  "@type"
                                :ref? true
                                :id   const/$rdf:type}
                               {:iri  "http://www.w3.org/2000/01/rdf-schema#Class"
                                :ref? true
                                :id   const/$rdfs:Class}])]
    {:t           0
     :refs        #{}
     :pred        pred
     :shapes      (atom {:class {} ; TODO: Does this need to be an atom?
                         :pred  {}})
     :subclasses  (delay {})}))

(defn reset-shapes
  "Resets the shapes cache - called when new shapes added to db"
  [{:keys [shapes] :as _schema}]
  (reset! shapes {:class {}
                  :pred  {}}))

(defn infer-predicate-ids
  [f]
  (let [[s p o] ((juxt flake/s flake/p flake/o) f)]
    (cond (and (= const/$rdf:type p)
               (contains? jld-ledger/class-or-property-sid o))
          [s p]

          (contains? jld-ledger/predicate-refs p)
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

(defn datatype-constraint?
  [f]
  (-> f flake/p (= const/$sh:datatype)))

(defn descending
  [x y]
  (compare y x))

(defn list-index
  [f]
  (-> f flake/op :i))

(defn pred-dt-constraints
  "Collect any shacl datatype constraints and the predicates they apply to."
  [new-flakes]
  (loop [[s-flakes & r] (partition-by flake/s new-flakes)
         res []]
    (if s-flakes
      (if-let [dt-constraints (->> s-flakes
                                   (filter datatype-constraint?)
                                   (map flake/o)
                                   first)]
        (let [path (->> s-flakes
                        (filter #(= const/$sh:path (flake/p %)))
                        (sort-by list-index descending)
                        (map flake/o)
                        first)]
          (recur r (conj res [path dt-constraints])))
        (recur r res))
      res)))

(defn add-pred-datatypes
  "Add a :datatype key to the pred meta map for any predicates with a sh:datatype
  constraint. Only one datatype constraint can be valid for a given datatype, most
  recent wins."
  [{:keys [pred] :as schema} pred-tuples]
  (reduce (fn [schema [pid dt]]
            (let [{:keys [iri] :as pred-meta} (-> pred
                                                  (get pid)
                                                  (assoc :datatype dt))]
              (-> schema
                  (assoc-in [:pred pid] pred-meta)
                  (assoc-in [:pred iri] pred-meta))))
          schema
          pred-tuples))

(defn add-pid
  [db preds pid]
  (let [{:keys [iri] :as p-map} (initial-property-map db pid)]
    (assoc preds pid p-map, iri p-map)))

(defn add-predicates
  [db pred-map pids]
  (reduce (fn [preds pid]
            (if (contains? preds pid)
              preds
              (add-pid db preds pid)))
          pred-map pids))

(defn build-schema
  [db pids vocab-flakes]
  (let [t        (:t db)
        schema   (-> (base-schema)
                     (update :pred (partial add-predicates db) pids)
                     (as-> s (update-with* s db t vocab-flakes)))
        refs     (extract-ref-sids (:pred schema))]
    (assoc schema :refs refs)))

(defn hydrate-schema
  "Updates the :schema key of db by processing just the vocabulary flakes out of
  the new flakes."
  [db new-flakes]
  (let [pred-sids    (collect-predicate-ids db new-flakes)
        vocab-flakes (into #{}
                           (filter (fn [f]
                                     (or (contains? pred-sids (flake/s f))
                                         (contains? jld-ledger/predicate-refs (flake/p f)))))
                           new-flakes)
        {:keys [t refs pred shapes subclasses]}
        (-> (build-schema db pred-sids vocab-flakes)
            (add-pred-datatypes (pred-dt-constraints new-flakes)))]
    (-> db
        (assoc-in [:schema :t] t)
        (update-in [:schema :refs] into refs)
        (update-in [:schema :pred] (partial merge-with merge) pred)
        (assoc-in [:schema :subclasses] subclasses)
        (assoc-in [:schema :shapes] shapes))))

(defn load-schema
  [{:keys [preds t] :as db}]
  (go-try
    (loop [[[pred-sid] & r] preds
           vocab-flakes (flake/sorted-set-by flake/cmp-flakes-spot)]
      (if pred-sid
        (let [pred-flakes (<? (query-range/index-range db :spot = [pred-sid]))]
          (recur r (into vocab-flakes pred-flakes)))
        (-> (build-schema db preds vocab-flakes)
            ;; only use predicates that have a dt
            (add-pred-datatypes (filterv #(> (count %) 1) preds)))))))
