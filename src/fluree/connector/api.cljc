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

(defn create
  "Create a ledger and return its address."
  ([conn ledger-name]
   (create conn ledger-name {}))
  ([conn ledger-name opts]
   (conn-impl/create conn ledger-name opts)))

(defn list
  "List ledgers on this connection."
  [conn]
  (conn-impl/list conn))

(defn transact
  "Transact data into a ledger."
  ([conn ledger-address tx]
   (transact conn ledger-address tx {}))
  ([conn ledger-address tx opts]
   (conn-impl/transact conn ledger-address tx opts)))

(defn query
  "Query a ledger."
  ([conn ledger-address q]
   (query conn ledger-address q {}))
  ([conn ledger-address query opts]
   (conn-impl/query conn ledger-address query opts)))

#_(defn subscribe
  "Subscribe to a ledger's updates."
  [conn query fn]
  (throw (ex-info "TODO" {})))

;; models

(def ConnectionConfig conn-model/ConnectionConfig)
(def Connection conn-model/Connection)
