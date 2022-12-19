(ns fluree.store.api
  (:refer-clojure :exclude [read list])
  (:require [fluree.store.core :as store-impl]
            [fluree.store.model :as store-model]))

(defn start
  "Takes a config and returns a possibly stateful Store."
  [config]
  (store-impl/start config))

(defn stop
  "Gracefully shuts down a store."
  [store]
  (store-impl/stop store))

(defn address
  "Given a k and the k's type, return an address for k's data."
  [store type k]
  (store-impl/address store type k))

(defn write
  "Associate data with key k in store."
  [store k data]
  (store-impl/write store k data))

(defn read
  "Read data from key k in store."
  [store k]
  (store-impl/read store k))

(defn list
  "Return keys from store with given prefix."
  [store prefix]
  (store-impl/list store prefix))

(defn delete
  "Remove data for key k in store."
  [store k]
  (store-impl/delete store k))

;; model definitions
(def BaseStoreConfig store-model/BaseStoreConfig)
(def FileStoreConfig store-model/FileStoreConfig)
(def MemoryStoreConfig store-model/MemoryStoreConfig)
(def StoreConfig store-model/StoreConfig)

(def FileStore store-model/FileStore)
(def MemoryStore store-model/MemoryStore)
(def Store store-model/Store)
