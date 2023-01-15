(ns fluree.indexer.model
  (:require [fluree.store.api :as store]))

(def IndexerConfig
  [:and
   [:map
    [:reindex-min-bytes {:optional true} :int]
    [:reindex-max-bytes {:optional true} :int]]
   [:or
    [:map [:idxr/store-config {:optional true} store/StoreConfig]]
    [:map [:idxr/store {:optional true} store/Store]]]])

(def Indexer
  [:map
   [:store store/Store]])

(def Db
  [:map
   [:ledger [:map
             [:method :keyword]
             [:alias :string]
             [:branch :keyword]
             [:state :any]
             [:indexer :map]
             [:conn store/Store]]]
   [:conn store/Store]
   [:branch :keyword]
   [:commit [:map
             [:alias :string]
             [:v :int]
             [:branch :keyword]
             [:data [:map [:t :int]]]]]
   [:t :int]

   [:tt-id :uuid]
   [:alias :string]
   [:method :any]
   [:block :any]
   [:stats :map]
   [:spot :any]
   [:psot :any]
   [:post :any]
   [:opst :any]
   [:tspo :any]
   [:schema :map]
   [:comparators :any]
   [:novelty :map]
   [:permissions :map]
   [:ecount :map]])

(def DbSummary
  [:map
   [:db/address :string]
   [:db/t :int]
   [:db/v :int]
   [:db/flakes :int]
   [:db/size :int]
   [:db/prev :string]])

(def TxSummary
  [:and
   DbSummary
   [:map
    [:db/assert [:sequential :any]]
    [:db/retract [:sequential :any]]]])
