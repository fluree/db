(ns fluree.connector.model
  (:require [fluree.transactor.api :as txr]
            [fluree.publisher.api :as pub]
            [fluree.store.api :as store]
            [fluree.indexer.api :as idxr]))

(def BaseConnectionConfig
  [:map
   [:conn/mode [:enum :fluree :query]]])

(def FlureeConnectionConfig
  [:and
   BaseConnectionConfig
   [:map
    [:conn/mode [:enum :fluree]]
    [:conn/store-config {:optional true} store/StoreConfig]
    [:conn/transactor-config {:optional true} txr/TransactorConfig]
    [:conn/publisher-config {:optional true} pub/PublisherConfig]
    [:conn/indexer-config {:optional true} idxr/IndexerConfig]]])

(def ConnectionConfig
  [:orn
   [:fluree FlureeConnectionConfig]])

(def Connection
  [:map
   [:id :any]
   [:transactor txr/Transactor]
   [:publisher pub/Publisher]
   [:indexer idxr/Indexer]])
