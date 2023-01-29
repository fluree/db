(ns fluree.connector.api
  (:refer-clojure :exclude [load list])
  (:require [fluree.connector.core :as conn-impl]
            [fluree.connector.model :as conn-model]))

(defn connect
  "Takes a configuration and returns a Connection."
  [config]
  (conn-impl/connect config))

(defn close
  "Gracefully shuts down a connection."
  [conn]
  (conn-impl/close conn))

(defn list
  "List ledgers on this connection."
  [conn]
  (conn-impl/list conn))

(defn create
  "Create a ledger and return its address."
  ([conn ledger-name]
   (create conn ledger-name {}))
  ([conn ledger-name opts]
   (conn-impl/create conn ledger-name opts)))

(defn load
  "Prepare a ledger for transacting and querying."
  ([conn ledger-name]
   (conn-impl/load conn ledger-name {}))
  ([conn ledger-name opts]
   (conn-impl/load conn ledger-name opts)))

(defn transact
  "Transact data into a ledger."
  ([conn ledger-name tx]
   (transact conn ledger-name tx {}))
  ([conn ledger-name tx opts]
   (conn-impl/transact conn ledger-name tx opts)))

(defn query
  "Query a ledger."
  ([conn ledger-name query]
   (conn-impl/query conn ledger-name query {}))
  ([conn ledger-name query opts]
   (conn-impl/query conn ledger-name query opts)))

(defn subscribe
  "Register a listener with a ledger to receive new db-blocks and new db-root notifications."
  [conn ledger-name cb opts]
  (conn-impl/subscribe conn ledger-name cb opts))

(defn unsubscribe
  "Unregister the listener to stop receiving updates."
  [conn ledger-name subscription-key]
  (conn-impl/unsubscribe conn ledger-name subscription-key))

;; models

(def ConnectionConfig conn-model/ConnectionConfig)
(def Connection conn-model/Connection)
