(ns fluree.publisher.model
  (:require [fluree.store.api :as store]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]
            [fluree.common.iri :as iri]
            [fluree.common.model :as model]))

(def PublisherConfig
  [:and
   [:and
    [:map
     [:pub/did model/Did]
     [:pub/trust {:optional true} model/TrustPolicy]
     [:pub/distrust {:optional true} model/DistrustPolicy]]
    [:fn (fn [{:pub/keys [trust distrust]}]
           (model/valid-trust-policy? trust distrust))]]
   [:or
    [:map [:pub/store-config {:optional true} store/StoreConfig]]
    [:map [:pub/store {:optional true} store/Store]]]])

(def Publisher
  [:map
   [:id :any]
   [:store
    [:orn
     [:file store/FileStore]
     [:memory store/MemoryStore]]]])

(def LedgerEntry
  [:map
   [iri/type [:enum iri/LedgerEntry]]
   [iri/LedgerEntryCreated :string]
   [iri/LedgerTxHead {:optional true} txr/TxHead]
   [iri/LedgerDbHead {:optional true} idxr/DbBlockSummary]])

(def Ledger
  [:map
   [iri/id :string]
   [iri/type [:enum iri/Ledger]]
   [iri/LedgerV nat-int?]
   [iri/LedgerName [:string {:max 1000}]]
   [iri/LedgerAddress :string]
   [iri/LedgerContext :map]
   [iri/LedgerHead LedgerEntry]])

;; TODO: move to credential component
(def LedgerCred
  [:map
   [:context :string]
   [:id :string]
   [:type [:sequential :string]]
   [:cred/issuer :string]
   [:cred/issuance-date :string]
   [:cred/credential-subject Ledger]
   [:cred/proof
    [:map
     [:type [:enum "EcdsaSecp256k1RecoverySignature2020"]]
     [:proof/created :string]
     [:proof/verification-method :string]
     [:proof/proof-purpose [:enum "assertionMethod"]]
     [:proof/jws :string]]]])
