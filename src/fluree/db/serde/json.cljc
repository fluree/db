(ns fluree.db.serde.json
  (:require [fluree.db.constants :as const]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            #?(:clj [fluree.db.util.clj-const :as uc]))
  #?(:clj (:import (java.time OffsetDateTime OffsetTime LocalDate LocalTime
                              LocalDateTime ZoneOffset)
                   (java.time.format DateTimeFormatter))))
#?(:clj (set! *warn-on-reflection* true))

(def time-types
  #{const/$xsd:date
    const/$xsd:dateTime
    const/$xsd:time})

(defn deserialize-flake
  [flake-vec]
  (if-let [flake-time-dt (time-types (get flake-vec 3))]
    (let [flake-value (get flake-vec 2)]
      (-> flake-vec
          ;;TODO: coercion will fail in js for `xsd:time` and `xsd:date`
          ;;objects, because we append data to make them dateTimes
          (update 2 #(datatype/coerce % flake-time-dt))
          (flake/parts->Flake)))
    (flake/parts->Flake flake-vec)))


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
  (assoc branch :children (mapv deserialize-child-node (:children branch))
         :rhs (some-> (:rhs branch)
                      (deserialize-flake))))

(defn- deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv deserialize-flake (:flakes leaf))))

#?(:clj (def ^DateTimeFormatter xsdDateTimeFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS[XXXXX]")))

#?(:clj (def ^DateTimeFormatter xsdTimeFormatter
          (DateTimeFormatter/ofPattern "HH:mm:ss.SSSSSSSSS[XXXXX]")))

;;xsd:date
#?(:clj (def ^DateTimeFormatter xsdDateFormatter
          (DateTimeFormatter/ofPattern "uuuu-MM-dd[XXXXX]")))


(defn format-value
  [val dt]
  (uc/case (int dt)
    const/$xsd:dateTime #?(:clj (cond->> val
                                  (or (instance? java.time.OffsetDateTime val)
                                      (instance? java.time.LocalDateTime val))
                                  (.format xsdDateTimeFormatter))
                           :cljs (.toJSON val))
    const/$xsd:date      #?(:clj (cond->> val
                                   (or (instance? java.time.OffsetDateTime val)
                                       (instance? java.time.LocalDate val))
                                   (.format xsdDateFormatter))
                            :cljs (.toJSON val))
    const/$xsd:time #?(:clj (cond->> val
                              (or (instance? java.time.OffsetTime val)
                                  (instance? java.time.LocalTime val))
                              (.format xsdTimeFormatter))
                       :cljs (.toJSON val))
    val))

(defn serialize-flake
  "Flakes with time types will have time objects as values.
  We need to serialize these into strings that will be successfully re-coerced into
  the same objects upon loading.

  Flakes with an 'm' value need keys converted from keyword keys into strings."
  [flake]
  (-> (vec flake)
      (update 2 format-value (flake/dt flake))
      (cond-> (flake/m flake) (assoc 5 (util/stringify-keys (flake/m flake))))))

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
