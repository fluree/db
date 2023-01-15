(ns fluree.indexer.tx-summary
  (:require [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]))

(defn create-tx-summary
  [{:keys [tx-summary-id] :as db} context asserts retracts]
  (let [index-root (-> db :commit :index :address)
        {:keys [reindex-min-bytes reindex-max-bytes]} (-> db :ledger :indexer)]
    (cond-> {:db/v 0
             :db/t (:t db)
             :db/flakes (-> db :stats :flakes)
             :db/size (-> db :stats :size)
             :db/context context
             :db/assert asserts
             :db/retract retracts
             :db/opts {:reindex-min-bytes reindex-min-bytes
                       :reindex-max-bytes reindex-max-bytes}}
      index-root (assoc :db/root index-root)
      tx-summary-id (assoc :db/previous tx-summary-id))))

(defn create-db-summary
  [tx-summary db-address]
  (-> tx-summary
      ;; assert, retract, and context depend on user-supplied information, can be large
      ;; previous, root is indexer local state
      (dissoc :db/assert :db/retract :db/context :db/previous :db/root)
      (assoc :db/address db-address)))

(defn tx-path
  [ledger-name]
  (str ledger-name "/tx/"))
