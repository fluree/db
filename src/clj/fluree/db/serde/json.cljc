(ns fluree.db.serde.json
  (:require [fluree.db.constants :as const]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util]
            [fluree.db.flake.index :as index]
            [fluree.db.util.json :as json]
            #?(:clj  [fluree.db.util.clj-const :as uc]
               :cljs [fluree.db.util.cljs-const :as uc]))
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

(defn keyword->int
  [k]
  (-> k name util/str->int))

(defn numerize-keys
  "Convert the keys of the provided map `m` to integers. Assumes that the keys are
  keywordized integers and will throw an exception otherwise."
  [m]
  (reduce-kv (fn [numerized k v]
               (let [int-k (keyword->int k)]
                 (assoc numerized int-k v)))
             {} m))

(defn deserialize-db-root
  "Assumes all data comes in as keywordized JSON."
  [db-root]
  (let [db-root* (reduce (fn [root-data idx]
                           (update root-data idx deserialize-child-node))
                         db-root index/types)]
    (update db-root* :namespace-codes numerize-keys)))

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
      const/$rdf:json (json/parse val false)
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
  [{:keys [alias branch t garbage]}]
  {"alias"   alias
   "branch"  (name branch)
   "t"       t
   "garbage" (vec garbage)})

(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-db-root [_ db-root]
    (reduce-kv
      (fn [acc k v]
        (assoc acc (name k)
                   (case k
                     (:stats :config :garbage :prev-index)
                     (util/stringify-keys v)

                     (:spot :post :opst :tspo)
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
    (deserialize-garbage garbage)))


(defn json-serde
  "Returns a JSON serializer / deserializer"
  []
  (->Serializer))
