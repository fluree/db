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

(defn- deserialize-db-root
  [db-root]
  (let [{:keys [spot psot post opst tspo]} db-root]
    (assoc db-root
           :spot (deserialize-child-node spot)
           :psot (deserialize-child-node psot)
           :post (deserialize-child-node post)
           :opst (deserialize-child-node opst)
           :tspo (deserialize-child-node tspo))))


(defn- deserialize-branch-node
  [branch]
  (assoc branch :children (mapv deserialize-child-node (:children branch))
         :rhs (some-> (:rhs branch)
                       (flake/parts->Flake))))


(defn- deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv flake/parts->Flake (:flakes leaf))))

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
    (-> (json/parse db-root)
        (deserialize-db-root)))
  (-serialize-branch [_ {:keys [children] :as _branch}]
    {"children" (map stringify-child children)})
  (-deserialize-branch [_ branch]
    (-> (json/parse branch)
        (deserialize-branch-node)))
  (-serialize-leaf [_ leaf]
    {"flakes" (map vec (:flakes leaf))})
  (-deserialize-leaf [_ leaf]
    (-> (json/parse leaf)
        (deserialize-leaf-node)))
  (-serialize-garbage [_ garbage]
    (util/stringify-keys garbage))
  (-deserialize-garbage [_ garbage]
    (json/parse garbage))
  (-serialize-db-pointer [_ pointer]
    (util/stringify-keys pointer))
  (-deserialize-db-pointer [_ pointer]
    (json/parse pointer)))


(defn json-serde
  "Returns a JSON serializer / deserializer"
  []
  (->Serializer))
