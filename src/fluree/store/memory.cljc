(ns fluree.store.memory
  (:refer-clojure :exclude [exists? list])
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
   [fluree.store.resolver :as resolver]))

(defn stop-memory-store [store]
  (log/info (str "Stopping MemoryStore " (service-proto/id store) "."))
  (reset! (:storage-atom store)  {})
  :stopped)

(defn address-memory
  [type k]
  (ident/create-address type :memory k))

(defrecord MemoryStore [id storage-atom async-cache]
  service-proto/Service
  (id [_] id)
  (stop [store] (stop-memory-store store))

  store-proto/Store
  (address [_ type k] (address-memory type k))
  (read [_ k] (async/go (get @storage-atom k)))
  (list [_ prefix]  (async/go
                      (let [ks (filter #(str/starts-with? % prefix) (keys @storage-atom))]
                        (map #(get @storage-atom %) ks))))
  (write [_ k data] (async/go (swap! storage-atom assoc k data) {:address k
                                                                 :hash (crypto/sha2-256 (pr-str data))}))
  (delete [_ k] (async/go (swap! storage-atom dissoc k) :deleted))

  fluree.db.index/Resolver
  (resolve [store node] (resolver/resolve-node store async-cache node)))

(defn create-memory-store
  [{:keys [store/id memory-store/storage-atom] :as config}]
  (let [id (or id (random-uuid))
        storage-atom (or storage-atom (atom {}))]
    (log/info "Starting MemoryStore " id "." config)
    (map->MemoryStore {:id id
                       :storage-atom storage-atom
                       :async-cache (resolver/create-async-cache config)
                       :serializer (none-serde/->Serializer)})))
