(ns fluree.db.connection.s3
  (:require [fluree.db.connection.system :as system]
            [fluree.db.util.async :refer [go-try]]))

(set! *warn-on-reflection* true)

(defn connect
  "Create a new S3 connection."
  [{:keys [defaults parallelism s3-endpoint s3-bucket s3-prefix cache-max-mb]
    :or   {parallelism  4
           cache-max-mb 100}}]
  (go-try
    (-> (system/s3-config s3-endpoint s3-bucket s3-prefix
                          parallelism cache-max-mb defaults)
        system/start)))
