(ns fluree.db.serde.json
  (:require #?(:clj  [fluree.db.util.clj-const :as uc]
               :cljs [fluree.db.util.cljs-const :as uc])
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.serde :as serde]
            [fluree.db.util :as util])
  #?(:clj (:import (java.time.format DateTimeFormatter))))
#?(:clj (set! *warn-on-reflection* true))

(defn deserialize-meta
  [serialized-meta]
  (some-> serialized-meta util/keywordize-keys))

(defn subject-reference?
  [dt]
  (= const/$id dt))

(defn deserialize-object
  [serialized-obj dt]
  (if (subject-reference? dt)
    (iri/deserialize-sid serialized-obj)
    (datatype/coerce serialized-obj dt)))

(defn deserialize-flake
  [flake-vec]
  (let [s  (-> flake-vec (get flake/subj-pos) iri/deserialize-sid)
        p  (-> flake-vec (get flake/pred-pos) iri/deserialize-sid)
        dt (-> flake-vec (get flake/dt-pos) iri/deserialize-sid)
        o  (-> flake-vec (get flake/obj-pos) (deserialize-object dt))
        t  (get flake-vec flake/t-pos)
        op (get flake-vec flake/op-pos)
        m  (-> flake-vec (get flake/m-pos) deserialize-meta)]
    (flake/create s p o dt t op m)))

(defn deserialize-flake-bound
  [flake-bound]
  (when flake-bound
    (deserialize-flake flake-bound)))

(defn deserialize-child-node
  "Turns :first and :rhs into flakes"
  [child-node]
  (-> child-node
      (update :first deserialize-flake-bound)
      (update :rhs deserialize-flake-bound)))

(defn parse-int
  [k]
  (-> k name util/str->int))

(defn numerize-keys
  "Convert the keys of the provided map `m` to integers. Assumes that the keys are
  either stringified or keywordized integers and will throw an exception
  otherwise."
  [m]
  (reduce-kv (fn [numerized k v]
               (let [int-k (parse-int k)]
                 (assoc numerized int-k v)))
             {} m))

(def property-stats-packer
  "Pack property stats into a 6-tuple: [count ndv-values ndv-subjects selectivity-value selectivity-subject last-modified-t]"
  (juxt :count :ndv-values :ndv-subjects :selectivity-value :selectivity-subject :last-modified-t))

(defn unpack-property-stats
  "Unpack 6-tuple back into property stats map"
  [[count ndv-values ndv-subjects selectivity-value selectivity-subject last-modified-t]]
  {:count count
   :ndv-values ndv-values
   :ndv-subjects ndv-subjects
   :selectivity-value selectivity-value
   :selectivity-subject selectivity-subject
   :last-modified-t last-modified-t})

(defn serialize-property-stats
  "Serialize property stats map using compact tuple format.
   Each entry: [serialized-sid [count ndv-values ndv-subjects selectivity-value selectivity-subject last-modified-t]]"
  [properties]
  (when properties
    (reduce-kv
     (fn [acc k v]
       (conj acc [(iri/serialize-sid k) (property-stats-packer v)]))
     []
     properties)))

(defn deserialize-property-stats
  "Deserialize property stats from compact tuple format."
  [properties]
  (when properties
    (reduce
     (fn [acc [sid-vec tuple]]
       (let [[ns-code nme] sid-vec
             sid (iri/->sid ns-code nme)]
         (assoc acc sid (unpack-property-stats tuple))))
     {}
     properties)))

(defn serialize-class-property-data
  "Serialize property data map for a class: {:types {sid count} :ref-classes {sid count} :langs {lang count}}
   Uses compact tuple format: [types-vec, ref-classes-vec, langs-vec]
   Each vec contains [sid/lang, count] tuples."
  [prop-data]
  (let [types (if (seq (:types prop-data))
                (vec (map (fn [[sid cnt]] [(iri/serialize-sid sid) cnt])
                          (:types prop-data)))
                [])
        ref-classes (if (seq (:ref-classes prop-data))
                      (vec (map (fn [[sid cnt]] [(iri/serialize-sid sid) cnt])
                                (:ref-classes prop-data)))
                      [])
        langs (if (seq (:langs prop-data))
                (vec (map (fn [[lang cnt]] [lang cnt])
                          (:langs prop-data)))
                [])]
    [types ref-classes langs]))

(defn serialize-class-properties
  "Serialize properties map for a class.
   Returns vector of [prop-sid-vec [types ref-classes langs]] tuples using compact format."
  [properties]
  (when (seq properties)
    (reduce-kv
     (fn [acc prop-sid prop-data]
       (conj acc [(iri/serialize-sid prop-sid)
                  (serialize-class-property-data prop-data)]))
     []
     properties)))

(defn serialize-class-stats
  "Serialize class stats map using compact tuple format.
   Format: [class-sid-vec [count properties-vec]]
   Where properties-vec is [[prop-sid [types ref-classes langs]] ...]
   This eliminates string keys like 'count', 'properties', 'types', etc."
  [classes]
  (when classes
    (reduce-kv
     (fn [acc class-sid class-data]
       (let [count (:count class-data)
             props (serialize-class-properties (:properties class-data))]
         (conj acc [(iri/serialize-sid class-sid) [count props]])))
     []
     classes)))

(defn deserialize-class-property-data
  "Deserialize property data from compact tuple format: [types ref-classes langs]
   Also handles legacy map format for backward compatibility."
  [prop-data]
  (if (vector? prop-data)
    ;; New compact tuple format: [types ref-classes langs]
    (let [[types-val ref-classes-val langs-val] prop-data]
      (cond-> {:types {} :ref-classes {} :langs {}}
        (seq types-val)
        (assoc :types (reduce (fn [acc [[ns-code nme] cnt]]
                                (assoc acc (iri/->sid ns-code nme) cnt))
                              {}
                              types-val))

        (seq ref-classes-val)
        (assoc :ref-classes (reduce (fn [acc [[ns-code nme] cnt]]
                                      (assoc acc (iri/->sid ns-code nme) cnt))
                                    {}
                                    ref-classes-val))

        (seq langs-val)
        (assoc :langs (reduce (fn [acc [lang cnt]]
                                (assoc acc lang cnt))
                              {}
                              langs-val))))
    ;; Legacy map format: {"types" [...] "refClasses" [...] "langs" [...]}
    (let [types-val (or (get prop-data :types) (get prop-data "types"))
          ref-classes-val (or (get prop-data :refClasses) (get prop-data "refClasses"))
          langs-val (or (get prop-data :langs) (get prop-data "langs"))]
      (cond-> {:types {} :ref-classes {} :langs {}}
        types-val
        (assoc :types (reduce (fn [acc [[ns-code nme] cnt]]
                                (assoc acc (iri/->sid ns-code nme) cnt))
                              {}
                              types-val))

        ref-classes-val
        (assoc :ref-classes (reduce (fn [acc [[ns-code nme] cnt]]
                                      (assoc acc (iri/->sid ns-code nme) cnt))
                                    {}
                                    ref-classes-val))

        langs-val
        (assoc :langs (reduce (fn [acc [lang cnt]]
                                (assoc acc lang cnt))
                              {}
                              langs-val))))))

(defn deserialize-class-properties
  "Deserialize properties map for a class.
   Expects vector of [prop-sid-vec prop-data] tuples where prop-data is compact tuple format."
  [properties]
  (when properties
    (reduce
     (fn [acc [prop-sid-vec prop-data]]
       (let [[ns-code nme] prop-sid-vec
             prop-sid (iri/->sid ns-code nme)]
         (assoc acc prop-sid (deserialize-class-property-data prop-data))))
     {}
     properties)))

(defn deserialize-class-stats
  "Deserialize class stats from compact tuple format or legacy formats.
   New format: [sid-vec [count properties-vec]]
   Legacy map format: [sid-vec {:count ... :properties ...}]
   Oldest format: [sid-vec count]"
  [classes]
  (when classes
    (reduce
     (fn [acc [sid-vec class-data]]
       (let [[ns-code nme] sid-vec
             class-sid (iri/->sid ns-code nme)]
         (cond
           ;; New compact tuple format: [count props]
           (and (vector? class-data) (number? (first class-data)))
           (let [[count-val props] class-data
                 deserialized (cond-> {:count count-val}
                                (seq props)
                                (assoc :properties (deserialize-class-properties props)))]
             (assoc acc class-sid deserialized))

           ;; Legacy map format: {:count ... :properties ...}
           (map? class-data)
           (let [count-val (or (get class-data :count) (get class-data "count"))
                 props (or (get class-data :properties) (get class-data "properties"))
                 deserialized (cond-> {:count count-val}
                                props
                                (assoc :properties (deserialize-class-properties props)))]
             (assoc acc class-sid deserialized))

           ;; Oldest format: just count number
           :else
           (assoc acc class-sid {:count class-data}))))
     {}
     classes)))

(defn serialize-stats
  "Serializes the stats structure using compact tuple format for properties."
  [stats]
  (when stats
    (-> stats
        (update :properties serialize-property-stats)
        (update :classes serialize-class-stats))))

(defn deserialize-stats
  "Deserializes the stats structure from compact tuple format.
   Only deserializes properties/classes for v2 indexes."
  [stats version]
  (when stats
    (if (= 2 version)
      (-> stats
          (update :properties deserialize-property-stats)
          (update :classes deserialize-class-stats))
      (dissoc stats :properties :classes))))

(defn deserialize-db-root
  "Assumes all data comes in as keywordized JSON."
  [db-root]
  (let [version  (or (:v db-root) 1)  ; default to v1 for legacy indexes
        db-root* (reduce (fn [root-data idx]
                           (update root-data idx deserialize-child-node))
                         db-root
                         (index/indexes-for db-root))]
    (-> db-root*
        (update :namespace-codes numerize-keys)
        (update :stats deserialize-stats version))))

(defn deserialize-children
  [children]
  (mapv deserialize-child-node children))

(defn deserialize-branch-node
  [branch]
  (-> branch
      deserialize-child-node
      (update :children deserialize-children)))

(defn deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv deserialize-flake (:flakes leaf))))

#?(:clj (def ^DateTimeFormatter xsdDateTimeFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS[XXXXX]")))

#?(:clj (def ^DateTimeFormatter xsdTimeFormatter
          (DateTimeFormatter/ofPattern "HH:mm:ss.SSSSSSSSS[XXXXX]")))

#?(:clj (def ^DateTimeFormatter xsdDateFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd[XXXXX]")))

(defn serialize-object
  "Flakes with time types will have time objects as values.
  We need to serialize these into strings that will be successfully re-coerced into
  the same objects upon loading."
  [val dt]
  (if (datatype/inferable? dt)
    val
    (uc/case dt
      const/$id (iri/serialize-sid val)
      const/$xsd:dateTime #?(:clj  (.format xsdDateTimeFormatter val)
                             :cljs (.toJSON val))
      const/$xsd:date #?(:clj  (.format xsdDateFormatter val)
                         :cljs (.toJSON val))
      const/$xsd:time #?(:clj  (.format xsdTimeFormatter val)
                         :cljs (.toJSON val))
      (str val))))

(defn serialize-meta
  [m]
  (when m
    (util/stringify-keys m)))

(defn serialize-flake
  "Serializes flakes into vectors, ensuring values are written such that they will
  be correctly coerced when loading.

  Flakes with an 'm' value need keys converted from keyword keys into strings."
  [flake]
  (let [s   (-> flake flake/s iri/serialize-sid)
        p   (-> flake flake/p iri/serialize-sid)
        dt* (flake/dt flake)
        dt  (iri/serialize-sid dt*)
        o   (-> flake flake/o (serialize-object dt*))
        t   (flake/t flake)
        op  (flake/op flake)
        m   (-> flake flake/m serialize-meta)]
    [s p o dt t op m]))

(defn- deserialize-garbage
  [garbage-data]
  garbage-data)

(defn- stringify-child
  "Stringifies keys for child/index branches, and converts #Flake data
  types into seq."
  [m]
  (reduce-kv
   (fn [acc k v]
     (assoc acc (name k) (if (flake/flake? v)
                           (serialize-flake v)
                           v)))
   {} m))

(defn serialize-garbage
  [{:keys [alias t garbage]}]
  ;; alias now includes branch in "ledger@branch" format, no separate branch needed
  {"alias"   alias
   "t"       t
   "garbage" (vec garbage)})

(defrecord Serializer []
  serde/StorageSerializer
  (-serialize-db-root [_ db-root]
    (reduce-kv
     (fn [acc k v]
       (assoc acc (name k)
              (case k
                :stats
                (serialize-stats v)

                (:config :garbage :prev-index)
                (util/stringify-keys v)

                (:spot :psot :post :opst :tspo)
                (stringify-child v)

                v)))
     {} db-root))
  (-deserialize-db-root [_ db-root]
    (deserialize-db-root db-root))
  (-serialize-branch [_ {:keys [children] :as _branch}]
    {"children" (map stringify-child children)})
  (-deserialize-branch [_ branch]
    (deserialize-branch-node branch))
  (-serialize-leaf [_ leaf]
    {"flakes" (map serialize-flake (:flakes leaf))})
  (-deserialize-leaf [_ leaf]
    (deserialize-leaf-node leaf))
  (-serialize-garbage [_ garbage-map]
    (serialize-garbage garbage-map))
  (-deserialize-garbage [_ garbage]
    (deserialize-garbage garbage))

  serde/BM25Serializer
  (-serialize-bm25 [_ bm25]
   ;; output as JSON, no additional parsing of keys/vals needed
    bm25)
  (-deserialize-bm25 [_ bm25]
    (-> bm25
        util/keywordize-keys
        (update :namespace-codes numerize-keys)
        (update :index-state util/keywordize-keys))))

(defn json-serde
  "Returns a JSON serializer / deserializer"
  []
  (->Serializer))
