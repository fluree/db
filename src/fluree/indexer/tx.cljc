(ns fluree.indexer.tx
  (:require [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]))

(defn create-tx-summary
  [db-final context asserts retracts]
  {:db/v 0
   :db/t (- (:t db-final))
   :db/flakes (-> db-final :stats :flakes)
   :db/size (-> db-final :stats :size)
   :db/context context
   :db/assert asserts
   :db/retract retracts})

(defn create-db-summary
  [tx-summary db-address]
  (-> tx-summary
      (dissoc :db/assert :db/retract :db/context)
      (assoc :db/address db-address)))

(defn create-tx-summary-id
  [tx-summary]
  (crypto/sha2-256 (json-ld/normalize-data tx-summary)))

(defn tx-path
  [ledger-name tx-summary-id]
  (str ledger-name "/index/" tx-summary-id))
