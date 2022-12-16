(ns fluree.indexer.model
  (:require [fluree.store.api :as store]))

(def IndexerConfig
  [:and
   [:map
    [:reindex-min-bytes {:optional true} :int]
    [:reindex-max-bytes {:optional true} :int]]
   [:or
    [:map [:pub/store-config {:optional true} store/StoreConfig]]
    [:map [:pub/store {:optional true} store/Store]]]])

(def Indexer
  [:map
   [:store store/Store]])

(def Db
  [:map
   [:novelty :map]
   [:ledger/name :string]
   [:db/t :int]
   [:db/stats [:map
               [:db/flakes :int]
               [:db/size :int]
               [:db/indexed :int]]]
   [:db/spot :any]
   [:db/psot :any]
   [:db/post :any]
   [:db/opst :any]
   [:db/tspo :any]
   [:db/schema :any]
   [:db/permissions :any]
   [:db/ecount :any]])

(def DbInfo
  [:map
   [:db/address :string]
   [:db/t :int]
   [:db/flakes :int]
   [:db/size :int]
   [:db/assert [:sequential :any]]
   [:db/retract [:sequential :any]]])
