(ns fluree.db.serde.none
  (:require [fluree.db.serde.protocol :as serde]))

(set! *warn-on-reflection* true)


;; Identity serializer doens't serialize anything.

(defrecord Serializer []
  serde/StorageSerializer
  (-serialize-db-root [_ db-root]
    db-root)
  (-deserialize-db-root [_ db-root]
    db-root)
  (-serialize-branch [_ branch]
    branch)
  (-deserialize-branch [_ branch]
    branch)
  (-serialize-leaf [_ leaf]
    leaf)
  (-deserialize-leaf [_ leaf]
    leaf)
  (-serialize-garbage [_ garbage]
    garbage)
  (-deserialize-garbage [_ garbage]
    garbage))
