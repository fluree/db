(ns fluree.indexer.api
  (:require [fluree.indexer.core :as idxr-impl]
            [fluree.indexer.model :as idxr-model]))

(defn start
  "Start the indexer with the given config."
  [config]
  (idxr-impl/start config))

(defn stop
  "Gracefully shut down the indexer."
  [idxr]
  (idxr-impl/stop idxr))

(defn init
  "Initialize a db index and returns the db-address"
  [idxr opts]
  (idxr-impl/init idxr opts))


(defn stage
  "Index some data and return a db-address."
  [idxr db-address data]
  (idxr-impl/stage idxr db-address data))

(defn query
  "Run a query against the specified db to get the query results."
  [idxr db-address query]
  (idxr-impl/query idxr db-address query))

;; models

(def IndexerConfig idxr-model/IndexerConfig)

(def Indexer idxr-model/Indexer)

(def Db idxr-model/Db)

(def DbInfo idxr-model/DbInfo)
