(ns fluree.db.serde.json
  (:require [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.util.json :as json]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.string :as str]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))


(defn deserialize-block
  [block]
  (assoc block :flakes (mapv flake/parts->Flake (:flakes block))))

(defn- deserialize-child-node
  "Turns :first and :rhs into flakes"
  [child-node]
  (assoc child-node
         :first (some-> child-node :first flake/parts->Flake)
         :rhs   (some-> child-node :rhs flake/parts->Flake)))

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
  (let [{:keys [spot psot post opst tspo ecount]} db-root]
    (assoc db-root
           :ecount (deserialize-ecount ecount)
           :spot   (deserialize-child-node spot)
           :psot   (deserialize-child-node psot)
           :post   (deserialize-child-node post)
           :opst   (deserialize-child-node opst)
           :tspo   (deserialize-child-node tspo))))


(defn- deserialize-branch-node
  [branch]
  (assoc branch :children (mapv deserialize-child-node (:children branch))
         :rhs (some-> (:rhs branch)
                       (flake/parts->Flake))))


(defn- deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv flake/parts->Flake (:flakes leaf))))

(defn serialize-flake
  "Flake with an 'm' value need keys converted from keyword keys into strings."
  [flake]
  (if-let [m (flake/m flake)]
    (-> (vec flake)
        (assoc 5 (util/stringify-keys m)))                  ;; flake 'm' value is at index #5 (6th flake element)
    (vec flake)))

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
                            (vec v)
                            v)))
    {} m))


(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-block [_ block]
    (throw (ex-info "-serialize-block not supported for JSON." {})))
  (-deserialize-block [_ block]
    (-> (json/parse block)
        (deserialize-block)))
  (-serialize-db-root [_ {:keys [t block prevIndex timestamp stats
                                 ledger-id ecount fork forkBlock
                                 spot psot post opst tspo] :as db-root}]
    (reduce-kv
      (fn [acc k v]
        (assoc acc (name k)
                   (case k
                     :stats (util/stringify-keys v)
                     (:spot :psot :post :opst :tspo) (stringify-child v)
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
