(ns fluree.transactor.model
  (:require [fluree.store.api :as store]
            [fluree.indexer.api :as idxr]))

(def Commit
  "This data structure gets written in the Store."
  [:map
   [:commit/t
    {:doc "The transaction `t`."}
    :int]
   [:commit/v
    {:doc "The version of this Commit structure."}
    :int]
   ;; this puts the method in the hashing zone... bad idea?
   [:commit/size
    {:doc "The size in bytes of the tx."}
    :number]

   [:commit/prev
    {:doc "The address of the previous commit."
     :optional true}
    :string]
   [:commit/tx
    {:doc "The "}
    :int]])

;; TODO: I think this is more generic and should live somewhere else.
(def CommitWrapper
  "This is returned by commit/create."
  [:map
   [:hash {:doc "The hash of the commit in the value."} :string]
   [:address {:doc "How to find the Store with the commit."} :string]
   [:value {:doc "The Commit."} Commit]])

(def CommitSummary
  "This is returned by transactor/commit."
  [:map
   [:commit/address :string]
   [:commit/hash :string]
   [:commit/t :int]
   [:commit/v :int]
   [:commit/prev {:optional true} :string]
   [:commit/size :int]])

(def TxInfo
  "This is the input to commit/create, along with the tx."
  [:map
   [:ledger/name :string]
   [:commit/t :int]
   [:commit/prev {:optional true} :string]])

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
