(ns fluree.transactor.model
  (:require
   [fluree.common.iri :as iri]
   [fluree.store.api :as store]
   [fluree.common.model :as model]))

(def Commit
  "This data structure gets written in the Store."
  [:map
   [iri/type [:enum iri/Commit]]
   [iri/CommitT {:doc "The transaction `t`."} nat-int?]
   [iri/CommitV {:doc "The version of this Commit structure."} nat-int?]
   [iri/CommitSize {:doc "The size in bytes of the tx."} nat-int?]
   [iri/CommitTx {:doc "The submitted transaction."} :any]
   [iri/CommitPrevious {:doc "The address of the previous commit." :optional true} :string]])

(def CommitSummary
  "This is returned by transactor/commit."
  [:map
   [iri/type [:enum iri/CommitSummary]]
   [iri/CommitAddress :string]
   [iri/CommitV nat-int?]
   [iri/CommitT nat-int?]
   [iri/CommitSize nat-int?]
   [iri/CommitPrevious {:optional true} :string]])

(def TransactorConfig
  [:and
   [:and
    [:map
     [:txr/did model/Did]
     [:txr/trust {:optional true} model/TrustPolicy]
     [:txr/distrust {:optional true} model/DistrustPolicy]]
    [:fn (fn [{:txr/keys [trust distrust]}]
           (model/valid-trust-policy? trust distrust))]]

   [:or
    [:map
     [:txr/store-config
      [:orn
       [:file store/FileStoreConfig]
       [:memory store/MemoryStoreConfig]]]]
    [:map
     [:txr/store store/Store]]]])

(def Transactor
  [:map
   [:id :any]
   [:store
    [:orn
     [:file store/FileStore]
     [:memory store/MemoryStore]]]])
