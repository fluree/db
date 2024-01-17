(ns fluree.db.storage.memory
  (:refer-clojure :exclude [read])
  (:require [fluree.db.storage.proto :as store-proto]
            [fluree.crypto :as crypto]
            [fluree.db.storage.util :as store-util]
            [clojure.string :as str]))

(defn memory-address
  [path]
  (str "fluree:memory://" path))

(defn memory-write
  [storage-atom k v {:keys [content-address?]}]
  (let [hashable (if (store-util/hashable? v)
                   v
                   (pr-str v))
        hash     (crypto/sha2-256 hashable)
        k*       (if content-address?
                   (str k hash)
                   k)]
    (swap! storage-atom assoc k* v)
    {:k k*
     :address (memory-address k*)
     :hash hash
     :size (count hashable)}))

(defn memory-list
  [storage-atom prefix]
  (filter #(when (string? %) (str/starts-with? % prefix))
          (keys storage-atom)))

(defn memory-read
  [storage-atom address]
  (let [k (:local (store-util/address-parts address))]
    (get @storage-atom k)))

(defn memory-delete
  [storage-atom address]
  (let [k (:local (store-util/address-parts address))]
    (swap! storage-atom dissoc k)))

(defn memory-exists?
  [storage-atom address]
  (let [k (:local (store-util/address-parts address))]
    (contains? @storage-atom k)))

(defrecord MemoryStore [storage-atom]
  store-proto/Store
  (write [_ k v opts] (memory-write storage-atom k v opts))
  (list [_ prefix] (memory-list storage-atom prefix))
  (read [_ address] (memory-read storage-atom address))
  (delete [_ address] (memory-delete storage-atom address))
  (exists? [_ address] (memory-exists? storage-atom address)))

(defn create-memory-store
  [{:keys [:memory-store/storage-atom] :as config}]
  (let [storage-atom (or storage-atom (atom {}))]
    (map->MemoryStore {:config config
                       :storage-atom storage-atom})))
