(ns fluree.connector.api
  (:refer-clojure :exclude [load])
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
  "Create a ledger in the connection."
  ([conn ledger-name]
   (create conn ledger-name {}))
  ([conn ledger-name opts]
   (conn-impl/create conn ledger-name opts)))

(defn transact
  "Transact data into a ledger."
  ([conn ledger-address tx]
   (transact conn ledger-address tx {}))
  ([conn ledger-address tx opts]
   (conn-impl/transact conn ledger-address tx opts)))

(defn query
  "Query a db."
  ([conn db-address q]
   (query conn db-address q {}))
  ([conn db-address query opts]
   (conn-impl/query conn db-address query opts)))

(defn head-db-address
  "Retrieve the latest db-address for the given ledger."
  [conn ledger-address]
  (conn-impl/head-db-address conn ledger-address))

(defn head-commit-address
  "Retrieve the latest commit-address for the given ledger."
  [conn ledger-address]
  (conn-impl/head-commit-address conn ledger-address))

#_(defn load
    "Add a ledger to the connection."
    [conn query opts]
    (throw (ex-info "TODO" {})))

#_(defn subscribe
  "Subscribe to a ledger's updates."
  [conn query fn]
  (throw (ex-info "TODO" {})))

;; models

(def ConnectionConfig conn-model/ConnectionConfig)
(def Connection conn-model/Connection)
