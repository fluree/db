(ns fluree.publisher.model
  (:require [fluree.store.api :as store]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]))

(def EntryInfo
  [:map
   [:id :string]
   [:type [:enum :ledger-entry]]
   [:entry/address :string]
   [:entry/issuer {:optional true} :string]
   [:entry/time :string]])

(def LedgerEntry
  [:and
   EntryInfo
   [:entry/previous {:optional true} EntryInfo]
   [:entry/commit {:optional true} txr/CommitInfo]
   [:entry/index {:optional true}
    [:map
     [:db/address :string]
     [:db/t :int]
     [:db/v :int]
     [:db/size :int]
     [:db/flakes :int]]]])

(def Ledger
  [:map
   [:id :string]
   [:type [:enum :ledger]]
   [:ledger/v :int]
   [:ledger/address :string]
   [:ledger/name [:string {:max 1000}]]
   [:ledger/context :map]
   [:ledger/head {:optional true} LedgerEntry]])

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
