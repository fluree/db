(ns fluree.db.connection.ipfs
  (:require [fluree.db.connection.system :as system]
            [fluree.db.util.async :refer [go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(def default-ipfs-server "http://127.0.0.1:5001/")

(defn connect
  [{:keys [server storage-path parallelism cache-max-mb defaults]
    :or   {server default-ipfs-server
           parallelism  4
           cache-max-mb 100}}]
  (go-try
    (-> (system/ipfs-config server storage-path parallelism cache-max-mb defaults)
        system/start)))
