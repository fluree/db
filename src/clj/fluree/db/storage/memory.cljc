(ns fluree.db.storage.memory
  (:refer-clojure :exclude [read])
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.util :as store-util]))

(defn memory-address
  [path]
  (str "fluree:memory://" path))

(defn memory-write
  [contents k v {:keys [content-address?]}]
  (let [hashable (if (store-util/hashable? v)
                   v
                   (pr-str v))
        hash     (crypto/sha2-256 hashable)
        k*       (if content-address?
                   (str k hash)
                   k)]
    (swap! contents assoc k* v)
    (async/go
      {:k k*
       :address (memory-address k*)
       :hash hash
       :size (count hashable)})))

(defn memory-list
  [contents prefix]
  (async/go
    (filter #(when (string? %) (str/starts-with? % prefix))
            (keys contents))))

(defn memory-read
  [contents address]
  (let [k (:local (store-util/address-parts address))]
    (async/go (get @contents k))))

(defn memory-delete
  [contents address]
  (let [k (:local (store-util/address-parts address))]
    (async/go (swap! contents dissoc k))))

(defn memory-exists?
  [contents address]
  (let [k (:local (store-util/address-parts address))]
    (async/go (contains? @contents k))))

(defrecord MemoryStore [contents]
  store-proto/Store
  (address [_ k] (memory-address k))

  (write [_ k v opts] (memory-write contents k v opts))

  (list [_ prefix] (memory-list contents prefix))

  (read [_ address] (memory-read contents address))

  (delete [_ address] (memory-delete contents address))

  (exists? [_ address] (memory-exists? contents address)))

(defn create-memory-store
  [{:keys [:memory-store/contents] :as config}]
  (let [contents (or contents (atom {}))]
    (map->MemoryStore {:config config
                       :contents contents})))
