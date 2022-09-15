(ns fluree.db.json-ld.vocab
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

;; generates vocabulary/schema pre-cached maps.

(def property-sids #{const/$rdf:Property
                     const/$owl:DatatypeProperty
                     const/$owl:ObjectProperty})

(defn is-property?
  "Takes a list of flakes for a specific subject and returns
  truthy if any of them are of rdf:type rdf:Property, or the
  owl-specific versions of properties as defined by property-sids."
  [s-flakes]
  (some (fn [f]
          (and (= const/$rdf:type (flake/p f))
               (property-sids (flake/o f))))
        s-flakes))


(defn schema-details
  [sid s-flakes]
  (loop [[f & r] s-flakes
         details (if (= sid const/$rdf:type)
                   {:id    sid                              ;; rdf:type is predefined, so flakes to build map won't be present.
                    :class false
                    :idx?  true
                    :ref?  true}
                   {:id                 sid
                    :class              true                ;; default
                    :idx?               true
                    :ref?               false               ;; could go from false->true if defined in vocab but hasn't been used yet
                    :subclassOf         []
                    :equivalentProperty []})]
    (if f
      (let [pid      (flake/p f)
            details* (cond
                       (= const/$iri pid)
                       (assoc details :iri (flake/o f))

                       (= const/$rdf:type pid)
                       (if (property-sids (flake/o f))
                         (if (= const/$owl:ObjectProperty (flake/o f))
                           (assoc details :class false
                                          :ref? true)
                           (assoc details :class false))
                         (if (= const/$iri (flake/o f))
                           (assoc details :class false
                                          :ref? true)
                           ;; it is a class, but we already did :class true as a default
                           details))

                       (= const/$rdfs:subClassOf pid)
                       (update details :subclassOf conj (flake/o f))

                       (= const/$_predicate:equivalentProperty pid)
                       (update details :equivalentProperty conj (flake/o f))

                       :else details)]
        (recur r details*))
      details)))


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
  (let [classes      (filter #(true? (:class %)) property-maps)
        subclass-map (recur-sub-classes classes)]
    ;; map subclasses for both subject-id and iri
    (reduce
      (fn [acc class]
        (assoc acc (:id class) (get subclass-map (:id class))
                   (:iri class) (get subclass-map (:id class))))
      {} classes)))

(defn extract-ref-sids
  [property-maps]
  (into #{} (keep #(when (true? (:ref? %)) (:id %)) property-maps)))

(defn parse-new-context
  "Retrieve context json out of default context flakes, and returns a fully parsed context."
  [context-flakes]
  (let [context-json (some #(when (= const/$fluree:context (flake/p %))
                              (flake/o %))
                           context-flakes)]
    (try*
      (let [keywordized (-> context-json
                            json/parse
                            json-ld/parse-context)
            stringified (-> context-json
                            (json/parse false)
                            json-ld/parse-context)]
        [keywordized stringified])
      (catch* e (log/warn (str "Invalid db default context, unable to parse: " (pr-str context-json)))
              nil))))

(defn update-with*
  [{:keys [pred] :as schema} t vocab-flakes]
  (loop [[s-flakes & r] (partition-by flake/s vocab-flakes)
         pred*       pred
         context-kw  nil
         context-str nil]
    (if s-flakes
      (let [sid (flake/s (first s-flakes))]
        (cond
          (= sid const/$fluree:default-context)
          (let [[context-kw context-str] (parse-new-context s-flakes)]
            (recur r pred* context-kw context-str))

          :else
          (let [prop-map (schema-details sid s-flakes)]
            (recur r
                   (assoc pred* (:id prop-map) prop-map
                                (:iri prop-map) prop-map)
                   context-kw context-str))))
      (cond-> (assoc schema :t t
                            :pred pred*
                            :subclasses (delay (calc-subclass pred*)))
              context-kw (assoc :context context-kw
                                :context-str context-str)))))


(defn update-with
  "When creating a new db from a transaction, merge new schema changes
  into existing schema of previous db."
  [{:keys [schema] :as _db-before} db-t new-refs vocab-flakes]
  (if (empty? vocab-flakes)
    schema
    (let [{:keys [refs]} schema
          refs* (into refs new-refs)]
      (-> (assoc schema :refs refs*)
          (update-with* db-t vocab-flakes)
          (assoc :refs refs*)))))

(defn base-schema
  []
  (let [coll {-1           {:name "_tx" :id -1 :sid -1}
              "_tx"        {:name "_tx" :id -1 :sid -1}
              0            {:name "_predicate" :id 0 :sid nil}
              "_predicate" {:name "_predicate" :id 0 :sid nil}
              11           {:name "_default" :id 11 :sid nil}
              "_default"   {:name "_default" :id 11 :sid nil}}
        pred (map-pred-id+iri [{:iri  "@id"
                                :idx? true
                                :id   0}
                               {:iri  "@type"
                                :ref? true
                                :idx? true
                                :id   200}
                               {:iri  "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                                :ref? true
                                :idx? true
                                :id   200}])]
    {:t           0
     :refs        #{}
     :coll        coll
     :pred        pred
     :context     nil
     :context-str nil
     :prefix      {}
     :fullText    #{}
     :subclasses  (delay {})}))


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
                                                    >= [(flake/max-subject-id const/$_collection)]
                                                    <= [0]))
          base-schema  (base-schema)
          schema       (update-with* base-schema t vocab-flakes)
          refs         (extract-ref-sids (:pred schema))]
      (-> schema
          (assoc :refs refs)))))
