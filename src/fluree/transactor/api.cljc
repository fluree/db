(ns fluree.transactor.api
  (:refer-clojure :exclude [resolve])
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

(defn head
  "Return the summary of the transaction head for the ledger."
  [txr ledger-name]
  (txr-impl/head txr ledger-name))

(defn transact
  "Persist the transaction and return a tx summary."
  [txr ledger-name tx]
  (txr-impl/transact txr ledger-name tx))

(defn resolve
  "Returns the transaction that corresponds to the tx-address."
  [txr tx-address]
  (txr-impl/resolve txr tx-address))

;; models

(def TxSummary txr-model/TxSummary)
(def TxHead txr-model/TxHead)


(def TransactorConfig txr-model/TransactorConfig)

(def Transactor txr-model/Transactor)
