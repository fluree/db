(ns fluree.transactor.api
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

(defn commit
  "Creates a commit from the tx."
  [txr tx tx-info]
  (txr-impl/commit txr tx tx-info))

;; models

(def CommitTx txr-model/CommitTx)
(def CommitInfo txr-model/CommitInfo)
(def Commit txr-model/Commit)

(def TxInfo txr-model/TxInfo)

(def TransactorConfig txr-model/TransactorConfig)

(def Transactor txr-model/Transactor)
