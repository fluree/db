(ns fluree.transactor.model
  (:require [fluree.store.api :as store]
            [fluree.indexer.api :as idxr]))

(def Commit
  "This data structure gets written in the Store."
  [:map
   [:commit/assert
    {:doc "The subjects and properties asserted in a transaction."}
    [:sequential :map]]
   [:commit/retract
    {:doc "The subjects and properties retracted in a transaction."}
    [:sequential :map]]
   ;; do we need this? could be expanded...
   [:commit/context
    {:doc "The JSON-LD context used in the transaction."}
    :map]

   [:commit/t
    {:doc "The transaction `t`."}
    :int]
   [:commit/v
    {:doc "The version of this Commit structure."}
    :int]
   ;; this puts the method in the hashing zone... bad idea?
   [:commit/prev
    {:doc "The address of the previous commit."
     :optional true}
    :string]

   [:commit/size
    {:doc "The size in bytes of the commit."}
    :number]
   [:commit/flakes
    {:doc "The number of flakes in the db associated with the commit."}
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
   [:address :string]
   [:hash :string]
   [:type [:enum :commit]]
   [:commit/t :int]
   [:commit/v :int]
   [:commit/prev {:optional true} :string]
   [:commit/size :int]
   [:commit/flakes :int]])

(def TxInfo
  "This is the input to commit/create, along with the tx."
  [:and
   idxr/DbSummary
   [:map
    [:ledger/name :string]
    [:commit/prev {:optional true} :string]]])

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
