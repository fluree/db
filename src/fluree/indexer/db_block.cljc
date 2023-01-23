(ns fluree.indexer.db-block
  (:require [fluree.common.iri :as iri]))

(defn create-db-block
  [{:keys [db-block-id] :as db} asserts retracts]
  (let [index-root                                    (-> db :commit :index :address)
        {:keys [reindex-min-bytes reindex-max-bytes]} (-> db :ledger :indexer)]
    (cond-> {iri/type iri/DbBlock
             iri/DbBlockV 0
             iri/DbBlockT (- (:t db))
             iri/DbBlockSize (-> db :stats :size)
             iri/DbBlockReindexMin reindex-min-bytes
             iri/DbBlockReindexMax reindex-max-bytes
             iri/DbBlockAssert asserts
             iri/DbBlockRetract retracts}
      index-root (assoc iri/DbBlockIndexRoot index-root)
      db-block-id (assoc iri/DbBlockPrevious db-block-id))))

(defn create-db-summary
  [db-block db-block-address]
  (-> db-block
      (dissoc iri/DbBlockAssert iri/DbBlockRetract iri/DbBlockPrevious iri/DbBlockIndexRoot)
      (assoc iri/DbBlockAddress db-block-address
             iri/type iri/DbBlockSummary)))

(defn db-block-path
  [ledger-name]
  (str ledger-name "/db/"))
