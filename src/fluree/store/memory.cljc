(ns fluree.store.memory
  (:refer-clojure :exclude [read])
  (:require [fluree.store.proto :as store-proto]
            [fluree.crypto :as crypto]
            [fluree.store.util :as store-util]))

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
     :hash hash
     :size (count hashable)}))

(defn memory-read
  [storage-atom k]
  (get @storage-atom k))

(defn memory-delete
  [storage-atom k]
  (swap! storage-atom dissoc k))

(defn memory-exists?
  [storage-atom k]
  (contains? @storage-atom k))

(defrecord MemoryStore [storage-atom]
  store-proto/Store
  (write [_ k v opts] (memory-write storage-atom k v opts))
  (read [_ k] (memory-read storage-atom k))
  (delete [_ k] (memory-delete storage-atom k))
  (exists? [_ k] (memory-exists? storage-atom k)))

(defn create-memory-store
  [{:keys [:memory-store/storage-atom] :as config}]
  (let [storage-atom (or storage-atom (atom {}))]
    (map->MemoryStore {:config config
                       :storage-atom storage-atom})))
