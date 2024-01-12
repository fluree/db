(ns fluree.store.core
  (:refer-clojure :exclude [read list])
  (:require [fluree.store.proto :as store-proto]
            [fluree.store.file :as file-store]
            [fluree.store.localstorage :as localstorage-store]
            [fluree.store.memory :as mem-store]
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

(def StoreConfig
  [:or
   FileConfig
   LocalStorageConfig
   MemoryConfig])

(defn start
  [{:keys [:store/method] :as config}]
  (if-let [config-error (m/explain StoreConfig config)]
    (throw (ex-info "Invalid Store config."
                    {:error  config-error
                     :config config}))
    (case method
      :file         (file-store/create-file-store config)
      :localstorage (localstorage-store/create-localstorage-store config)
      :memory       (mem-store/create-memory-store config)

      (throw (ex-info (str "No Store implementation for :store/method: " (pr-str method))
                      config)))))

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
