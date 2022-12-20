(ns fluree.store.memory
  (:refer-clojure :exclude [exists? list])
  (:require [clojure.core.async :as async]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.index :sa index]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.log :as log]
            [fluree.common.identity :as ident]
            [fluree.common.protocols :as service-proto]
            [fluree.common.util :as util]
            [fluree.store.protocols :as store-proto]
            [clojure.string :as str]
            [fluree.db.serde.none :as none-serde]))

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
  (write [_ k data] (async/go (swap! storage-atom assoc k data) :written))
  (delete [_ k] (async/go (swap! storage-atom dissoc k) :deleted))

  fluree.db.index/Resolver
  (resolve
    [store {:keys [id tempid] :as node}]
    (if (= :empty id)
      (storage/resolve-empty-leaf node)
      (async-cache
        [::resolve id tempid]
        (fn [_]
          (storage/resolve-index-node store node
                                      (fn []
                                        (async-cache [::resolve id tempid] nil))))))))

(defn- lookup-cache
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (swap! cache-atom cache/hit k)
      v)))

(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn- default-async-cache-fn
  "Default asynchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (let [out (async/chan)]
      (if-let [v (lookup-cache cache-atom k value-fn)]
        (async/put! out v)
        (async/go
          (let [v (async/<! (value-fn k))]
            (when-not (util/exception? v)
              (swap! cache-atom cache/miss k v))
            (async/put! out v))))
      out)))

(defn create-memory-store
  [{:keys [store/id memory-store/storage-atom] :as config}]
  (let [id (or id (random-uuid))
        storage-atom (or storage-atom (atom {}))

        memory  1000000 ;; default 1MB memory
        memory-object-size (quot memory 100000)
        default-cache-atom (atom (default-object-cache-factory memory-object-size))
        async-cache-fn (default-async-cache-fn default-cache-atom)]
    (log/info "Starting MemoryStore " id "." config)
    (map->MemoryStore {:id id
                       :storage-atom storage-atom
                       :async-cache async-cache-fn
                       :serializer (none-serde/->Serializer)})))
