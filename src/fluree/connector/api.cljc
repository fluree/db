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
  ([conn ledger-address]
   (conn-impl/load conn ledger-address {}))
  ([conn ledger-address opts]
   (conn-impl/load conn ledger-address opts)))

(defn transact
  "Transact data into a ledger."
  ([conn ledger-address tx]
   (transact conn ledger-address tx {}))
  ([conn ledger-address tx opts]
   (conn-impl/transact conn ledger-address tx opts)))

(defn query
  "Query a ledger."
  ([conn ledger-address query]
   (conn-impl/query conn ledger-address query {}))
  ([conn ledger-address query opts]
   (conn-impl/query conn ledger-address query opts)))

(defn subscribe
  "Register a listener with a ledger to receive new db-blocks and new db-root notifications."
  [conn ledger-address cb opts]
  (conn-impl/subscribe conn ledger-address cb opts))

(defn unsubscribe
  "Unregister the listener to stop receiving updates."
  [conn ledger-address subscription-key]
  (conn-impl/unsubscribe conn ledger-address subscription-key))

;; models

(def ConnectionConfig conn-model/ConnectionConfig)
(def Connection conn-model/Connection)
