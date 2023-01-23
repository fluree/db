(ns fluree.transactor.api
  (:refer-clojure :exclude [resolve load])
  (:require [fluree.transactor.core :as txr-impl]
            [fluree.transactor.model :as txr-model]))

(defn start
  "Returns a transactor created with given config."
  [config]
  (txr-impl/start config))

(defn stop
  "Gracefully shuts down the transactor."
  [txr]
  (txr-impl/stop txr))

(defn init
  "Establish a transaction head for the ledger."
  [txr ledger-name]
  (txr-impl/init txr ledger-name))

(defn load
  "Return the commit-summary for the transaction head for the ledger."
  [txr ledger-name]
  (txr-impl/load txr ledger-name))

(defn commit
  "Creates a commit from the tx."
  [txr ledger-name tx]
  (txr-impl/commit txr ledger-name tx))

(defn resolve
  "Returns the commit map corresponding to the commit-address. Returns `nil` if not found."
  [txr commit-address]
  (txr-impl/resolve txr commit-address))

;; models

(def Commit txr-model/Commit)
(def CommitSummary txr-model/CommitSummary)

(def TransactorConfig txr-model/TransactorConfig)

(def Transactor txr-model/Transactor)
