(ns fluree.transactor.model
  (:require
   [fluree.common.iri :as iri]
   [fluree.store.api :as store]
   [fluree.common.model :as model]))

(def TxSummary
  "Metadata about the transaction, persisted to Store."
  [:map
   [iri/type [:enum iri/TxSummary]]
   [iri/TxSummaryV {:doc "The version of this TxSummary structure."} nat-int?]
   [iri/TxSummarySize {:doc "The size in bytes of the tx."} nat-int?]
   [iri/TxSummaryTx {:doc "The submitted transaction."} :any]
   [iri/TxSummaryTxId {:doc "The hash of the submitted transaction."} :string]
   [iri/TxSummaryPrevious {:doc "The address of the previous tx-summary." :optional true} :string]])

(def TxHead
  "Abridged TxSummary of constant size that is returned to the caller."
  [:map
   [iri/type [:enum iri/TxHead]]
   [iri/TxHeadAddress :string]
   [iri/TxSummaryV nat-int?]
   [iri/TxSummarySize nat-int?]
   [iri/TxSummaryPrevious {:optional true} :string]])

(def TransactorConfig
  [:and
   [:and
    [:map
     [:txr/did model/Did]
     [:txr/trust {:optional true} model/TrustPolicy]
     [:txr/distrust {:optional true} model/DistrustPolicy]]
    [:fn (fn [{:txr/keys [trust distrust]}]
           (model/valid-trust-policy? trust distrust))]]

   [:or
    [:map
     [:txr/store-config
      [:orn
       [:file store/FileStoreConfig]
       [:memory store/MemoryStoreConfig]]]]
    [:map
     [:txr/store store/Store]]]])

(def Transactor
  [:map
   [:id :any]
   [:store
    [:orn
     [:file store/FileStore]
     [:memory store/MemoryStore]]]])
