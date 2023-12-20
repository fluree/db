(ns fluree.store.core
  (:refer-clojure :exclude [read])
  (:require [fluree.store.proto :as store-proto]
            [fluree.store.file :as file-store]
            [fluree.store.memory :as mem-store]
            [malli.core :as m]))

(def BaseConfig
  [:map
   [:store/method [:enum :memory :file :ipfs :s3 :remote]]])

(def MemoryConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :memory]]
    [:memory-store/storage-atom {:optional true} :any]]])

(def FileConfig
  [:and
   BaseConfig
   [:map
    [:store/method [:enum :file]]
    [:file-store/storage-path :string]]])

(def StoreConfig
  [:or
   MemoryConfig
   FileConfig])

(defn start
  [{:keys [:store/method] :as config}]
  (if-let [config-error (m/explain StoreConfig config)]
    (throw (ex-info "Invalid Store config."
                    {:error  config-error
                     :config config}))
    (case method
      :memory (mem-store/create-memory-store config)
      :file   (file-store/create-file-store config)
      (throw (ex-info (str "No Store implementation for :store/method: " (pr-str method))
                      config)))))

(defn write
  [store k v opts]
  (store-proto/write store k v opts))

(defn read
  [store k]
  (store-proto/read store k))

(defn delete
  [store k]
  (store-proto/delete store k))

(defn exists?
  [store k]
  (store-proto/exists? store k))
