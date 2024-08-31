(ns fluree.db.connection.file
  (:require [fluree.db.connection.system :as system]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn connect
  [{:keys [defaults parallelism storage-path cache-max-mb]}]
  (go-try
    (-> (system/file-config storage-path parallelism cache-max-mb defaults)
        system/start)))
