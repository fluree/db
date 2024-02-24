(ns fluree.db.storage.memory
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
            [fluree.db.storage.util :as store-util]))

(defn memory-address
  [path]
  (str "fluree:memory://" path))

(defn memory-write
  [contents path v {:keys [content-address?]}]
  (go
    (let [hashable (if (store-util/hashable? v)
                     v
                     (pr-str v))
          hash     (crypto/sha2-256 hashable)
          path*       (if content-address?
                        (str path hash)
                        path)]
      (swap! contents assoc path* v)
      {:path path*
       :address (memory-address path*)
       :hash hash
       :size (count hashable)})))

(defn memory-list
  [contents prefix]
  (go
    (filter #(when (string? %) (str/starts-with? % prefix))
            (keys contents))))

(defn memory-read
  [contents address]
  (go
    (let [path (:local (store-util/address-parts address))]
      (get @contents path))))

(defn memory-delete
  [contents address]
  (go
    (let [path (:local (store-util/address-parts address))]
      (swap! contents dissoc path))))

(defn memory-exists?
  [contents address]
  (go
    (let [path (:local (store-util/address-parts address))]
      (contains? @contents path))))

(defrecord MemoryStore [contents]
  storage/Store
  (address [_ path] (memory-address path))

  (write [_ path v opts] (memory-write contents path v opts))

  (list [_ prefix] (memory-list contents prefix))

  (read [_ address] (memory-read contents address))

  (delete [_ address] (memory-delete contents address))

  (exists? [_ address] (memory-exists? contents address)))

(defn create
  []
  (let [contents (atom {})]
    (->MemoryStore contents)))
