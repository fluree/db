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

(def property-sids #{const/$rdf:Property
                     const/$owl:DatatypeProperty
                     const/$owl:ObjectProperty})

(defn initial-property-map
  [db sid]
  (if (= sid const/$rdf:type)
    {:id    sid ; rdf:type is predefined, so flakes to build map won't be present.
     :class false}
    (let [iri (iri/decode-sid db sid)]
      {:id          sid
       :iri         iri
       :class       true ; default
       :subclassOf  #{}
       :parentProps #{}
       :childProps  #{}})))

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
  [db prop-map parent-prop child-props]
  (let [initial-map       (initial-property-map db parent-prop)
        with-new-children (fnil add-child-properties initial-map)]
    (update prop-map parent-prop with-new-children child-props)))

(defn update-child-with-parents
  [db prop-map child-prop parent-props]
  (let [initial-map      (initial-property-map db child-prop)
        with-new-parents (fnil add-parent-properties initial-map)]
    (update prop-map child-prop with-new-parents parent-props)))

(defn add-new-children-to-parents
  "Adds new :childProps to parents in the schema map all the way up
  the hierarchy"
  [db pred-map all-parents new-child-properties]
  (reduce (fn [p-map parent-prop]
            (update-parent-with-children db p-map parent-prop new-child-properties))
          pred-map all-parents))

(defn add-new-parents-to-children
  "Adds new :parentProps to children in the schema map all the way down
  the hierarchy"
  [db pred-map all-children new-parent-properties]
  (reduce (fn [p-map child-prop]
            (update-child-with-parents db p-map child-prop new-parent-properties))
          pred-map all-children))

(defn update-rdfs-subproperty-of
  "Updates the schema map with the rdfs:subPropertyOf relationship
  between parent and child properties.

  owl:equivalentProperty also uses this, as an equivalent property
  relationship is where each property is a subproperty of the other."
  [db pred-map parent-prop child-prop]
  (let [parent-parents      (get-in pred-map [parent-prop :parentProps])
        child-children      (get-in pred-map [child-prop :childProps])
        new-parent-children (conj child-children child-prop)
        new-child-parents   (conj parent-parents parent-prop)]
    (as-> pred-map props
          (add-new-children-to-parents db props new-child-parents new-parent-children)
          (add-new-parents-to-children db props new-parent-children new-child-parents))))

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
  [db pred-map sid pid obj]
  (if (iri/sid? obj)
    (if (= const/$owl:equivalentProperty pid)
      (as-> pred-map props
            (update-rdfs-subproperty-of db props sid obj)
            (update-rdfs-subproperty-of db props obj sid))
      (update-rdfs-subproperty-of db pred-map obj sid))
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
  [db pred-map vocab-flake]
  (let [[sid pid obj]   ((juxt flake/s flake/p flake/o) vocab-flake)
        initial-map     (initial-property-map db sid)
        with-properties (fnil assoc initial-map)
        with-subclass   (fnil add-subclass initial-map)]
    (cond
      (= const/$rdf:type pid)
      (if (contains? property-sids obj)
        (if (= const/$owl:ObjectProperty obj)
          (update pred-map sid with-properties :class false)
          (update pred-map sid with-properties :class false))
        (if (= const/$xsd:anyURI obj)
          (update pred-map sid with-properties :class false)
          ;; it is a class, but we already did :class true as a default
          pred-map))

      (= const/$rdfs:subClassOf pid)
      (update pred-map sid with-subclass obj)

      (or (= const/$owl:equivalentProperty pid)
          (= const/$rdfs:subPropertyOf pid))
      (update-related-properties db pred-map sid pid obj)

      :else pred-map)))

(defn with-vocab-flakes
  [db pred-map vocab-flakes]
  (let [new-pred-map  (reduce (partial update-pred-map db) pred-map vocab-flakes)]
    (reduce-kv (fn [preds k v]
                 (if (iri/sid? k)
                   (assoc preds k v, (:iri v) v)
                   preds))
               {"@type" {:iri "@type"
                         :id  const/$rdf:type}}
               new-pred-map)))

(defn refresh-subclasses
  [{:keys [pred] :as schema}]
  (assoc schema :subclasses (delay (calc-subclass pred))))

(defn update-with
  [schema db t vocab-flakes]
  (-> schema
      (assoc :t t)
      (update :pred (partial with-vocab-flakes db) vocab-flakes)
      refresh-subclasses))

(defn base-schema
  []
  (let [pred (map-pred-id+iri [{:iri "@type"
                                :id  const/$rdf:type}
                               {:iri "http://www.w3.org/2000/01/rdf-schema#Class"
                                :id  const/$rdfs:Class}])]
    {:t          0
     :pred       pred
     :shapes     (atom {:class {} ; TODO: Does this need to be an atom?
                        :pred  {}})
     :subclasses (delay {})}))

(defn reset-shapes
  "Resets the shapes cache - called when new shapes added to db"
  [{:keys [shapes] :as _schema}]
  (reset! shapes {:class {}
                  :pred  {}}))

(defn modified-shape?
  [s-flakes]
  (some (fn [[_ p o :as _f]]
          (or
            ;; modified a subject with a shape type
            (and (= p const/$rdf:type)
                 (or (= o const/sh_NodeShape)
                     (= o const/sh_PropertyShape)))
            ;; most property shapes don't have a type, but do need a path
            (= p const/sh_path)))
        s-flakes))

(defn invalidate-shape-cache!
  "Invalidates the shape cache if _any_ shape is modified."
  [db subject-mods]
  (when (some modified-shape? (vals subject-mods))
    (reset! (-> db :schema :shapes) {})))

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
  (-> f flake/p (= const/sh_datatype)))

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
                        (filter #(= const/sh_path (flake/p %)))
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
  (let [{:keys [schema t]} db
        schema* (-> schema
                    (update :pred (partial add-predicates db) pids)
                    (as-> s (update-with s db t vocab-flakes)))]
    schema*))

(defn hydrate-schema
  "Updates the :schema key of db by processing just the vocabulary flakes out of
  the new flakes."
  ([db new-flakes]
   (hydrate-schema db new-flakes {}))
  ([db new-flakes mods]
   (let [pred-sids    (collect-predicate-ids db new-flakes)
         vocab-flakes (into #{}
                            (filter (fn [f]
                                      (or (contains? pred-sids (flake/s f))
                                          (contains? jld-ledger/predicate-refs (flake/p f)))))
                            new-flakes)
         schema       (-> (build-schema db pred-sids vocab-flakes)
                          (add-pred-datatypes (pred-dt-constraints new-flakes)))]
     (invalidate-shape-cache! db mods)
     (assoc db :schema schema))))

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
