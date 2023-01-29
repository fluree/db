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
  "Given a path and the path's type, return an address for path's data."
  [store type path]
  (store-impl/address store type path))

(defn write
  "Associate data with path in store. Returns map of :path and :hash.

  opts:
  :serializer - override Store default serializer.
  :content-address? - caculates the sha256 hash of `data` after serializing and appends it to the `path` before storing."
  ([store path data]
   (store-impl/write store path data {}))
  ([store path data {:keys [serializer content-address?] :as opts}]
   (store-impl/write store path data opts)))

(defn read
  "Read data from path in store.

  opts:
  :deserializer - override Store default deserializer."
  ([store path]
   (store-impl/read store path {}))
  ([store path {:keys [deserializer] :as opts}]
   (store-impl/read store path opts)))

(defn list
  "Return paths from store with given prefix."
  [store prefix]
  (store-impl/list store prefix))

(defn delete
  "Remove data for path in store."
  [store path]
  (store-impl/delete store path))

;; model definitions
(def BaseStoreConfig store-model/BaseStoreConfig)
(def FileStoreConfig store-model/FileStoreConfig)
(def MemoryStoreConfig store-model/MemoryStoreConfig)
(def StoreConfig store-model/StoreConfig)

(def FileStore store-model/FileStore)
(def MemoryStore store-model/MemoryStore)
(def Store store-model/Store)
