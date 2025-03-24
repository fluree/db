(ns fluree.db.cache
  "A simple default connection-level cache."
  (:require [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util :refer [exception?]]
            [fluree.db.util.log :as log]))

(defn create-lru-cache
  "Create a cache that holds `cache-size` number of entries, bumping out the least
  recently used value after the size is exceeded."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn memory->cache-size
  "Validate system memory is enough to build a usable cache, then derive cache size."
  [cache-max-mb]
  (let [memory      (or cache-max-mb 100) ; default 100MB memory
        object-size 0.1 ; estimate 100kb index node size
        cache-size  (int (quot memory object-size))] ; number of objects to keep in cache
    (when (< cache-size 10)
      (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.")
                      {:status 400 :error :db/invalid-configuration})))
    cache-size))

(defn lru-lookup
  "Given an LRU cache atom, look up value for `k`. If not found, use `value-fn` (a
  function that accepts `k` as its only argument and returns an async channel) to
  produce the value and add it to the cache."
  [cache-atom k value-fn]
  (if-let [v (get @cache-atom k)]
    (do (swap! cache-atom cache/hit k)
        v)
    (let [c (async/promise-chan)]
      (swap! cache-atom cache/miss k c)
      (async/go
        (let [v (async/<! (value-fn k))]
          (async/put! c v)
          (when (exception? v)
            (log/error v "Error resolving cache value for key: " k "with exception:" (ex-message v))
            (swap! cache-atom cache/evict k))))
      c)))

(defn lru-evict
  "Evict the key `k` from the cache."
  [cache-atom k]
  (swap! cache-atom cache/evict k))
