(ns fluree.db.json-ld.vocab
  (:require [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.iri :as iri-util]
            [fluree.db.util.log :as log])
  #?(:clj (:import (fluree.db.flake Flake))))

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
  (some (fn [^Flake f]
          (and (= const/$rdf:type (.-p f))
               (property-sids (.-o f))))
        s-flakes))


(defn schema-details
  [refs s-flakes]
  (let [sid  (.-s ^Flake (first s-flakes))
        ref? (boolean (refs sid))]
    (loop [[^Flake f & r] s-flakes
           details (if (= sid const/$rdf:type)
                     {:id    sid                            ;; rdf:type is predefined, so flakes to build map won't be present.
                      :class false
                      :ref?  true}
                     {:id                 sid
                      :class              true              ;; default
                      :ref?               ref?              ;; could go from false->true if defined in vocab but hasn't been use dyet
                      :subclassOf         []
                      :equivalentProperty []})]
      (if f
        (let [pid      (.-p f)
              details* (cond
                         (= const/$iri pid)
                         (assoc details :iri (.-o f))

                         (= const/$rdf:type pid)
                         (if (property-sids (.-o f))
                           (if (= const/$owl:ObjectProperty (.-o f))
                             (assoc details :class false
                                            :idx? true
                                            :ref? true)
                             (assoc details :class false
                                            :idx? true))
                           ;; it is a class, but we already did :class true as a default
                           details)

                         (= const/$rdfs:subClassOf pid)
                         (update details :subclassOf conj (.-o f))

                         (= const/$_predicate:equivalentProperty)
                         (update details :equivalentProperty conj (.-o f))

                         :else details)]
          (recur r details*))
        details))))


(defn hash-map-both-id-iri
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

(defn vocab-map*
  "Helper to vocab-map that does core vocab mapping logic with already resolved flakes
  so does not return asyn chan.

  refs is a set of predicate ids (pids) that are refs to other properties."
  [db-t refs vocab-flakes]
  (let [coll          {-1           {:name "_tx" :id -1 :sid -1}
                       "_tx"        {:name "_tx" :id -1 :sid -1}
                       0            {:name "_predicate" :id 0 :sid nil}
                       "_predicate" {:name "_predicate" :id 0 :sid nil}
                       11           {:name "_default" :id 11 :sid nil}
                       "_default"   {:name "_default" :id 11 :sid nil}}
        property-maps (->> vocab-flakes
                           (partition-by #(.-s ^Flake %))
                           (map #(schema-details refs %)))]
    {:t          db-t                                       ;; record time of spec generation, can use to determine cache validity
     :coll       coll
     :refs       refs                                       ;; Any properties defined (or inferred) as @id
     :pred       (-> property-maps
                     (conj {:iri "@id"
                            :id  0}
                           {:iri  "@type"
                            :ref? true
                            :id   200})
                     hash-map-both-id-iri)
     :prefix     {}
     :fullText   #{}
     :subclasses (delay (calc-subclass property-maps))      ;; delay because might not be needed
     }))


(defn update-with
  "When creating a new db from a transaction, merge new schema changes
  into existing schema of previous db."
  [db-before db-t new-refs vocab-flakes]
  (let [{:keys [schema]} db-before
        {:keys [refs pred]} schema]
    (if (empty? pred)
      ;; new/blank db, create new base schema
      (vocab-map* db-t refs vocab-flakes)
      ;; schema exists, merge new vocab in
      (let [refs*             (into refs new-refs)
            new-property-maps (->> vocab-flakes
                                   (partition-by #(.-s ^Flake %))
                                   (map #(schema-details refs* %))
                                   hash-map-both-id-iri)
            property-maps     (merge pred new-property-maps)]
        (assoc schema :t db-t
                      :pred property-maps
                      :subclasses (delay (calc-subclass property-maps)))))))


(defn vocab-map
  "Returns a map of the schema for a db to allow quick lookups of schema properties.
  Schema is a map with keys:
  - :t - the 't' value when schema built, allows schema equality checks
  - :coll - collection info, mapping cid->name and name->cid all within the same map
  - :pred - predicate info, mapping pid->properties and name->properties for quick lookup based on id or name respectively
  - :fullText - contains predicate ids that need fulltext search
  "
  ([db] (vocab-map db nil))
  ([db new-refs]
   (go-try
     (let [vocab-flakes (<? (query-range/index-range db :spot
                                                     >= [(flake/max-subject-id const/$_collection)]
                                                     <= [0]))
           refs         (-> (get-in db [:schema :refs])
                            (into new-refs))]
       (vocab-map* (:t db) refs vocab-flakes)))))