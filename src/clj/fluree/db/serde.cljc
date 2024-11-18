(ns fluree.db.serde)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol StorageSerializer
  (serialize-db-root [this db-root] "Serializes the database index root.")
  (deserialize-db-root [this db-root] "Deserializes the database index root.")
  (serialize-branch [this branch] "Serializes a branch.")
  (deserialize-branch [this branch] "Deserializes a branch.")
  (serialize-leaf [this leaf] "Serializes a leaf.")
  (deserialize-leaf [this leaf] "Deserializes a leaf.")
  (serialize-garbage [this garbage] "Serializes database garbage for later cleanup.")
  (deserialize-garbage [this garbage] "Deserializes database garbage."))
