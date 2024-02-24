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
  [contents path v {:keys [content-address?]}]
  (let [hashable (if (store-util/hashable? v)
                   v
                   (pr-str v))
        hash     (crypto/sha2-256 hashable)
        path*       (if content-address?
                      (str path hash)
                      path)]
    (swap! contents assoc path* v)
    (async/go
      {:path path*
       :address (memory-address path*)
       :hash hash
       :size (count hashable)})))

(defn memory-list
  [contents prefix]
  (async/go
    (filter #(when (string? %) (str/starts-with? % prefix))
            (keys contents))))

(defn memory-read
  [contents address]
  (let [path (:local (store-util/address-parts address))]
    (async/go (get @contents path))))

(defn memory-delete
  [contents address]
  (let [path (:local (store-util/address-parts address))]
    (async/go (swap! contents dissoc path))))

(defn memory-exists?
  [contents address]
  (let [path (:local (store-util/address-parts address))]
    (async/go (contains? @contents path))))

(defrecord MemoryStore [contents]
  store-proto/Store
  (address [_ path] (memory-address path))

  (write [_ path v opts] (memory-write contents path v opts))

  (list [_ prefix] (memory-list contents prefix))

  (read [_ address] (memory-read contents address))

  (delete [_ address] (memory-delete contents address))

  (exists? [_ address] (memory-exists? contents address)))

(defn create-memory-store
  [{:keys [:memory-store/contents] :as config}]
  (let [contents (or contents (atom {}))]
    (map->MemoryStore {:config config
                       :contents contents})))
