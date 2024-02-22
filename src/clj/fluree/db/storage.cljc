(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.file :as file-store]
            [fluree.db.storage.ipfs :as ipfs-store]
            [fluree.db.storage.localstorage :as localstorage-store]
            [fluree.db.storage.memory :as mem-store]
            #?(:clj [fluree.db.storage.s3 :as s3-store])
            [malli.core :as m]))


(def BaseConfig
  [:map
   [:store/method [:enum :memory :localstorage :file :ipfs :s3 :remote]]])

(def FileConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :file]]
    [:file-store/storage-path :string]]])

(def LocalStorageConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :localstorage]]]])

(def MemoryConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :memory]]
    [:memory-store/storage-atom {:optional true} :any]]])

(def IpfsConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :ipfs]]
    [:ipfs-store/server {:optional true} [:maybe :string]]]])

(def S3Config
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :s3]]
    [:s3-store/endpoint {:optional true} :string]
    [:s3-store/bucket :string]
    [:s3-store/prefix :string]]])

(def StoreConfig
  [:or
   FileConfig
   LocalStorageConfig
   MemoryConfig
   IpfsConfig
   S3Config])

(defn start
  [{:keys [:store/method] :as config}]
  (if-let [config-error (m/explain StoreConfig config)]
    (throw (ex-info "Invalid Store config."
                    {:error  config-error
                     :config config}))
    (case method
      :file         (file-store/create-file-store config)
      :ipfs         (ipfs-store/create-ipfs-store config)
      :localstorage (localstorage-store/create-localstorage-store config)
      :memory       (mem-store/create-memory-store config)
      :s3           #?(:clj (s3-store/create-s3-store config)
                       :cljs (throw (ex-info "S3 storage not supported in ClojureScript."
                                             {:status 400, :error :store/unsupported-method})))


      (throw (ex-info (str "No Store implementation for :store/method: " (pr-str method))
                      config)))))

(defn address
  [store k]
  (store-proto/address store k))

(defn write
  ([store k v]
   (store-proto/write store k v nil))
  ([store k v opts]
   (store-proto/write store k v opts)))

(defn read
  [store address]
  (store-proto/read store address))

(defn list
  [store prefix]
  (store-proto/list store prefix))

(defn delete
  [store address]
  (store-proto/delete store address))

(defn exists?
  [store address]
  (store-proto/exists? store address))
