(ns fluree.store.model)

(def BaseStoreConfig
  [:map
   [:store/method [:enum :file :memory #_:local-storage #_:s3 #_:ipfs #_:dynamodb]]])

(def FileStoreConfig
  [:and
   BaseStoreConfig
   [:map
    [:store/method [:enum :file]]
    [:file-store/storage-path {:doc "The base file path to base the Store on."} :string]
    [:file-store/serialize-to [:enum :edn :json]]]])

(def MemoryStoreConfig
  [:and BaseStoreConfig
   [:map
    [:store/method [:enum :memory]]
    [:memory-store/storage-atom {:optional true} :any]]])

(def StoreConfig
  [:orn
   [:file FileStoreConfig]
   [:memory MemoryStoreConfig]])

(def FileStore
  [:map
   [:id :any]
   [:storage-path :string]])

(def MemoryStore
  [:map
   [:id :any]
   [:storage-atom :any]])

(def Store
  [:orn
   [:file FileStore]
   [:memory MemoryStore]])
