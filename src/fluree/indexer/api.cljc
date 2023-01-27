(ns fluree.indexer.api
  (:refer-clojure :exclude [load])
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
  ([idxr ledger-name]
   (idxr-impl/init idxr ledger-name {}))
  ([idxr ledger-name {:keys [reindex-min-bytes reindex-max-bytes] :as opts}]
   (idxr-impl/init idxr ledger-name opts)))

(defn load
  "Load the db and prepare it for staging and querying."
  ([idxr db-address]
   (idxr-impl/load idxr db-address {}))
  ([idxr db-address {:keys [reindex-min-bytes reindex-max-bytes] :as opts}]
   (idxr-impl/load idxr db-address opts)))

(defn stage
  "Index some data and return a db-address."
  ([idxr db-address data]
   (idxr-impl/stage idxr db-address data {}))
  ([idxr db-address data opts]
   (idxr-impl/stage idxr db-address data opts)))

(defn query
  "Run a query against the specified db to get the query results."
  [idxr db-address query]
  (idxr-impl/query idxr db-address query))

;; models

(def IndexerConfig idxr-model/IndexerConfig)

(def Indexer idxr-model/Indexer)

(def Db idxr-model/Db)

(def DbBlockSummary idxr-model/DbBlockSummary)
(def DbBlock idxr-model/DbBlock)
