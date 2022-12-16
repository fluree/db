(ns fluree.transactor.model
  (:require [fluree.store.api :as store]
            [fluree.indexer.api :as idxr]))

(def CommitTx
  [:map
   [:commit/assert [:sequential :map]]
   [:commit/retract [:sequential :map]]
   [:commit/context :map]
   [:commit/t :int]
   [:commit/v :int]
   ;; this puts the method in the hashing zone... bad idea?
   [:commit/prev {:optional true} :string]])

(def CommitInfo
  [:map
   [:id :string]
   [:type [:enum :commit]]
   [:commit/address :string]
   [:commit/db-address :string]
   [:commit/hash :string]
   [:commit/size :number]
   [:commit/flakes :int]])

(def TxInfo
  [:and
   idxr/DbInfo
   [:map
    [:ledger/name :string]
    [:commit/prev {:optional true} :string]]])

(def Commit
  [:and
   CommitInfo
   [:map
    [:commit/tx CommitTx]]])

(def TransactorConfig
  [:or
   [:map
    [:txr/store-config
     [:orn
      [:file store/FileStoreConfig]
      [:memory store/MemoryStoreConfig]]]]
   [:map
    [:txr/store store/Store]]])

(def Transactor
  [:map
   [:id :any]
   [:store
    [:orn
     [:file store/FileStore]
     [:memory store/MemoryStore]]]])
