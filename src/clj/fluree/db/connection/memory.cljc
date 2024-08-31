(ns fluree.db.connection.memory
  (:require [fluree.db.connection.system :as system]
            [fluree.db.util.async :refer [go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn connect
  [{:keys [parallelism cache-max-mb defaults]
    :or   {parallelism  4
           cache-max-mb 100}}]
  (go-try
    (-> (system/memory-config parallelism cache-max-mb defaults)
        system/start)))
