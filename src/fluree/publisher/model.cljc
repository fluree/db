(ns fluree.publisher.model
  (:require [fluree.store.api :as store]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]
            [fluree.common.iri :as iri]))

(def PublisherConfig
  [:and
   [:map
    [:pub/defaults {:optional true}
     [:map
      [:pub/did
       [:map
        [:public :string]
        [:private :string]
        [:id :string]]]
      [:pub/context :map]]]]
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
   [iri/LedgerEntryCommit {:optional true} txr/CommitSummary]
   [iri/LedgerEntryDb {:optional true} idxr/DbBlockSummary]])

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
