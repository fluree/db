(ns fluree.store.memory
  (:refer-clojure :exclude [exists? list hash])
  (:require
   [clojure.core.async :as async]
   [clojure.string :as str]
   [fluree.common.identity :as ident]
   [fluree.common.protocols :as service-proto]
   [fluree.common.util :as util]
   [fluree.db.index]
   [fluree.db.serde.none :as none-serde]
   [fluree.db.util.log :as log]
   [fluree.store.protocols :as store-proto]
   [fluree.store.resolver :as resolver]
   [fluree.crypto :as crypto]))

(defn stop-memory-store [store]
  (log/info (str "Stopping MemoryStore " (service-proto/id store) "."))
  (reset! (:storage-atom store)  {})
  :stopped)

(defn address-memory
  [type k]
  (ident/create-address type :memory k))

(defn memory-write
  [storage-atom k data {:keys [serializer content-address?] :as _opts}]
  (let [serializer (or serializer pr-str)
        serialized (serializer data)
        hash       (crypto/sha2-256 serialized)
        path       (if content-address?
                     (str k hash)
                     k)]
    ;; for convenience, store the clj data instead of the serialized data
    (swap! storage-atom assoc path data)
    {:path    path
     :id      hash
     :address path
     :hash    hash}))

(defn memory-read
  [storage-atom k {:keys [deserializer] :as _opts}]
  (let [data (get @storage-atom k)]
    (if deserializer
      (deserializer data)
      data)))

(defrecord MemoryStore [id storage-atom async-cache]
  service-proto/Service
  (id [_] id)
  (stop [store] (stop-memory-store store))

  store-proto/Store
  (address [_ type k] (address-memory type k))
  (read [_ k] (async/go (memory-read storage-atom k {})))
  (read [_ k opts] (async/go (memory-read storage-atom k opts)))
  (list [_ prefix]  (async/go (filter #(str/starts-with? % prefix) (keys @storage-atom))))
  (write [_ k data] (async/go (memory-write storage-atom k data {})))
  (write [_ k data opts] (async/go (memory-write storage-atom k data opts)))
  (delete [_ k] (async/go (swap! storage-atom dissoc k) :deleted))

  fluree.db.index/Resolver
  (resolve [store node]
    (resolver/resolve-node store async-cache node)))

(defn create-memory-store
  [{:keys [store/id memory-store/storage-atom] :as config}]
  (let [id (or id (random-uuid))
        storage-atom (or storage-atom (atom {}))]
    (log/info "Started MemoryStore." id )
    (map->MemoryStore {:id id
                       :storage-atom storage-atom
                       :async-cache (resolver/create-async-cache config)
                       :serializer (none-serde/->Serializer)})))
