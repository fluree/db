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

(defn commit
  "Creates a commit from the tx."
  [txr tx tx-info]
  (txr-impl/commit txr tx tx-info))

(defn resolve
  "Returns the commit map corresponding to the commit-address. Returns `nil` if not found."
  [txr commit-address]
  (txr-impl/resolve txr commit-address))

;; models

(def Commit txr-model/Commit)
(def CommitWrapper txr-model/CommitWrapper)
(def CommitSummary txr-model/CommitSummary)

(def TxInfo txr-model/TxInfo)

(def TransactorConfig txr-model/TransactorConfig)

(def Transactor txr-model/Transactor)
