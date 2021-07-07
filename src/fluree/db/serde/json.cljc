(ns fluree.db.serde.json
  (:require [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.util.json :as json]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]))


(defn deserialize-block
  [block]
  (assoc block :flakes (mapv flake/parts->Flake (:flakes block))))

(defn- deserialize-child-node
  "Turns :floor and :ciel into flakes"
  [child-node]
  (assoc child-node
         :floor (some-> child-node :floor flake/parts->Flake)
         :ciel  (some-> child-node :ciel flake/parts->Flake)))

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
         :ciel (some-> (:ciel branch)
                       (flake/parts->Flake))))


(defn- deserialize-leaf-node
  [leaf]
  (assoc leaf :flakes (mapv flake/parts->Flake (:flakes leaf))))


(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-block [_ block]
    (throw (ex-info "-serialize-block not supported for JSON." {})))
  (-deserialize-block [_ block]
    (-> (json/parse block)
        (deserialize-block)))
  (-serialize-db-root [_ db-root]
    ;; turn stats keys into proper strings
    (throw (ex-info "-serialize-db-root not supported for JSON." {})))
  (-deserialize-db-root [_ db-root]
    (-> (json/parse db-root)
        (deserialize-db-root)))
  (-serialize-branch [_ branch]
    (throw (ex-info "-serialize-branch not supported for JSON." {})))
  (-deserialize-branch [_ branch]
    (-> (json/parse branch)
        (deserialize-branch-node)))
  (-serialize-leaf [_ leaf]
    (throw (ex-info "-serialize-leaf not supported for JSON." {})))
  (-deserialize-leaf [_ leaf]
    (-> (json/parse leaf)
        (deserialize-leaf-node)))
  (-serialize-garbage [_ garbage]
    (throw (ex-info "-serialize-garbage not supported for JSON." {})))
  (-deserialize-garbage [_ garbage]
    (json/parse garbage))
  (-serialize-db-pointer [_ pointer]
    (throw (ex-info "-serialize-db-pointer not supported for JSON." {})))
  (-deserialize-db-pointer [_ pointer]
    (json/parse pointer)))


(defn json-serde
  "Returns a JSON serializer / deserializer"
  []
  (->Serializer))
