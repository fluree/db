(ns fluree.transactor.tx-summary
  (:require
   [fluree.common.iri :as iri]
   [fluree.store.api :as store]
   [fluree.crypto :as crypto]))

(defn create-tx-summary
  [tx-head tx]
  (let [{previous iri/TxHeadAddress} tx-head]
    (cond-> {iri/type iri/TxSummary
             iri/TxSummaryTx tx
             ;; TODO: properly serialize tx
             iri/TxSummaryTxId (crypto/sha2-256 (pr-str tx))
             ;; TODO: properly calculate size
             iri/TxSummarySize (count tx)
             iri/TxSummaryV 0}
      previous (assoc iri/TxSummaryPrevious previous))))

(defn create-tx-head
  [tx-summary tx-summary-address]
  (-> tx-summary
      (dissoc iri/TxSummaryTx)
      (assoc iri/type iri/TxHead
             iri/TxHeadAddress tx-summary-address)))

(defn tx-summary-path
  [ledger-name]
  (str ledger-name "/tx-summary/"))

(defn create-tx-summary-address
  [store path]
  (store/address store "tx-summary" path))

(defn tx-path
  [ledger-name]
  (str ledger-name "/tx/"))
