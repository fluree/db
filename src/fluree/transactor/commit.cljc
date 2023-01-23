(ns fluree.transactor.commit
  (:require
   [fluree.common.iri :as iri]
   [fluree.store.api :as store]))

(defn create-commit
  [previous-commit-summary tx]
  (let [{previous   iri/CommitAddress
         previous-t iri/CommitT} previous-commit-summary]
    (cond-> {iri/type iri/Commit
             iri/CommitTx tx
             iri/CommitSize (count tx)
             iri/CommitT ((fnil inc -1) previous-t)
             iri/CommitV 0}
      previous (assoc iri/CommitPrevious previous))))

(defn create-commit-summary
  [commit commit-address]
  (-> commit
      (dissoc iri/CommitTx)
      (assoc iri/CommitAddress commit-address
             iri/type iri/CommitSummary)))

(defn commit-path
  [ledger-name]
  (str ledger-name "/commit/"))

(defn create-commit-address
  [store path]
  (store/address store :commit path))
