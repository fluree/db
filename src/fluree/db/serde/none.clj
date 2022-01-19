(ns fluree.db.serde.none
  (:require [fluree.db.serde.protocol :as serdeproto]))

(set! *warn-on-reflection* true)


;; Identity serializer doens't serialize anything.

(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-transaction [_ tx-data]
    tx-data)
  (-deserialize-transaction [_ tx]
    tx)
  (-serialize-block [_ block-data]
    block-data)
  (-deserialize-block [_ block-key]
    block-key)
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
    garbage)
  (-serialize-db-pointer [_ pointer]
    pointer)
  (-deserialize-db-pointer [_ pointer]
    pointer))
