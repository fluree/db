(ns fluree.indexer.model
  (:require [fluree.store.api :as store]
            [fluree.common.iri :as iri]))

(def IndexerConfig
  [:or
   [:map [:idxr/store-config {:optional true} store/StoreConfig]]
   [:map [:idxr/store {:optional true} store/Store]]])

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

(def DbBlock
  "Persisted to disk to track updates to novelty."
  [:map
   [iri/type [:enum iri/DbBlock]]

   [iri/DbBlockV nat-int?]
   [iri/DbBlockT nat-int?]
   [iri/DbBlockSize nat-int?]
   [iri/DbBlockPrevious {:optional true} :string]

   [iri/DbBlockReindexMin nat-int?]
   [iri/DbBlockReindexMax nat-int?]
   [iri/DbBlockAssert [:sequential :any]]
   [iri/DbBlockRetract [:sequential :any]]])

(def DbBlockSummary
  "Returned to caller."
  [:map
   [iri/type [:enum iri/DbBlockSummary]]

   [iri/DbBlockAddress :string]

   [iri/DbBlockV nat-int?]
   [iri/DbBlockT nat-int?]
   [iri/DbBlockSize nat-int?]
   [iri/DbBlockPrevious {:optional true} :string]])
