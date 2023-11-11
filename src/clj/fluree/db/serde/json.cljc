(ns fluree.db.serde.json
  (:require [fluree.db.constants :as const]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util]
            #?(:clj  [fluree.db.util.clj-const :as uc]
               :cljs [fluree.db.util.cljs-const :as uc]))
  #?(:clj (:import (java.time.format DateTimeFormatter))))
#?(:clj (set! *warn-on-reflection* true))

(defn deserialize-subject
  [serialized-sid]
  (let [ns  (nth serialized-sid 0)
        nme (nth serialized-sid 1)]
    (iri/append-name-codes [ns] nme)))

(defn deserialize-meta
  [serialized-meta]
  (util/keywordize-keys serialized-meta))

(defn subject-reference?
  [dt]
  (= const/$xsd:anyURI dt))

(defn deserialize-object
  [serialized-obj dt]
  (if (subject-reference? dt)
    (deserialize-subject serialized-obj)
    (datatype/coerce serialized-obj dt)))

(defn deserialize-flake
  [flake-vec]
  (let [s  (-> flake-vec (get flake/subj-pos) deserialize-subject)
        p  (-> flake-vec (get flake/pred-pos) deserialize-subject)
        dt (get flake-vec flake/dt-pos)
        o  (-> flake-vec (get flake/obj-pos) (deserialize-object dt))
        t  (get flake-vec flake/t-pos)
        op (get flake-vec flake/op-pos)
        m  (-> flake-vec (get flake/m-pos) (deserialize-meta))]
    (flake/create s p o dt t op m)))

(defn- deserialize-child-node
  "Turns :first and :rhs into flakes"
  [child-node]
  (assoc child-node
         :first (some-> child-node :first deserialize-flake)
         :rhs   (some-> child-node :rhs deserialize-flake)))

(defn- deserialize-ecount
  "Converts ecount from keywordized keys back to integers."
  [ecount]
  (reduce-kv
    (fn [acc k v]
      (if (keyword? k)
        (assoc acc (-> k name util/str->int) v)
        (throw (ex-info (str "Expected serialized ecount values to be keywords, instead found: " ecount)
                        {:status 500 :error :db/invalid-index}))))
    {} ecount))

(defn- deserialize-db-root
  "Assumes all data comes in as keywordized JSON.
  :ecount will have string keys converted to keywords. Need to re-convert
  them to integer keys."
  [db-root]
  (let [{:keys [spot post opst tspo ecount]} db-root]
    (assoc db-root
           :ecount (deserialize-ecount ecount)
           :spot   (deserialize-child-node spot)
           :post   (deserialize-child-node post)
           :opst   (deserialize-child-node opst)
           :tspo   (deserialize-child-node tspo))))


(defn- deserialize-branch-node
  [branch]
  (assoc branch
         :children (mapv deserialize-child-node (:children branch))
         :rhs (some-> (:rhs branch)
                      (deserialize-flake))))

(defn- deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv deserialize-flake (:flakes leaf))))

(defn serialize-sid
  [sid]
  (let [ns-code (iri/get-ns-code sid)
        nme     (iri/get-name sid)]
    [ns-code nme]))

#?(:clj (def ^DateTimeFormatter xsdDateTimeFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS[XXXXX]")))

#?(:clj (def ^DateTimeFormatter xsdTimeFormatter
          (DateTimeFormatter/ofPattern "HH:mm:ss.SSSSSSSSS[XXXXX]")))

#?(:clj (def ^DateTimeFormatter xsdDateFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd[XXXXX]")))

(defn serialize-subject
  [sid]
  (serialize-sid sid))

(defn serialize-predicate
  [pid]
  (serialize-sid pid))

(defn serialize-object
  "Flakes with time types will have time objects as values.
  We need to serialize these into strings that will be successfully re-coerced into
  the same objects upon loading."
  [val dt]
  (uc/case (int dt)
    const/$xsd:anyURI    (serialize-subject val)
    const/$xsd:dateTime  #?(:clj (.format xsdDateTimeFormatter val)
                            :cljs (.toJSON val))
    const/$xsd:date      #?(:clj (.format xsdDateFormatter val)
                            :cljs (.toJSON val))
    const/$xsd:time      #?(:clj (.format xsdTimeFormatter val)
                            :cljs (.toJSON val))
    val))

(defn serialize-datatype
  [dt]
  (serialize-sid dt))

(defn serialize-meta
  [m]
  (when m
    (util/stringify-keys m)))

(defn serialize-flake
  "Serializes flakes into vectors, ensuring values are written such that they will
  be correctly coerced when loading.

  Flakes with an 'm' value need keys converted from keyword keys into strings."
  [flake]
  (let [s   (-> flake flake/s serialize-subject)
        p   (-> flake flake/p serialize-predicate)
        dt* (flake/dt flake)
        dt  (serialize-datatype dt*)
        o   (-> flake flake/o (serialize-object dt))
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


(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-db-root [_ db-root]
    (reduce-kv
      (fn [acc k v]
        (assoc acc (name k)
                   (case k
                     :stats (util/stringify-keys v)
                     (:spot :post :opst :tspo) (stringify-child v)
                     ;; else
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
  (-serialize-garbage [_ garbage]
    (util/stringify-keys garbage))
  (-deserialize-garbage [_ garbage]
    (deserialize-garbage garbage)))


(defn json-serde
  "Returns a JSON serializer / deserializer"
  []
  (->Serializer))
