(ns fluree.connector.model
  (:require [fluree.transactor.api :as txr]
            [fluree.publisher.api :as pub]
            [fluree.store.api :as store]
            [fluree.indexer.api :as idxr]
            [fluree.common.model :as model]))

(def BaseConnectionConfig
  [:map
   [:conn/mode [:enum :fluree :query]]
   [:conn/did {:optional true} model/Did]
   [:conn/trust {:optional true} model/TrustPolicy]
   [:conn/distrust {:optional true} model/DistrustPolicy]])

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
   [:indexer idxr/Indexer]
   ;; atom
   [:subscriptions
    [:map-of :string
     [:map-of :string
      [:map
       [:subscription/opts :map]
       [:subscription/cb [:fn [:catn
                               [:db-block :map]
                               [:opts [:map [:authClaims :map]]]]]]]]]]])
