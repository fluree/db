(ns fluree.indexer.tx-summary
  (:require [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]))

(defn create-tx-summary
  [{:keys [tx-summary] :as db} context asserts retracts]
  (cond-> {:db/v 0
           :db/t (- (:t db))
           :db/flakes (-> db :stats :flakes)
           :db/size (-> db :stats :size)
           :db/context context
           :db/assert asserts
           :db/retract retracts}
    tx-summary (assoc :db/previous tx-summary)))

(defn create-db-summary
  [tx-summary db-address]
  (-> tx-summary
      ;; assert, retract, and context depend on user-supplied information, can be large
      ;; previous is indexer-local state, by not sharing it we can collect as garbage if we need to
      (dissoc :db/assert :db/retract :db/context :db/previous)
      (assoc :db/address db-address)))

(defn tx-path
  [ledger-name]
  (str ledger-name "/tx/"))
