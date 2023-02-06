(ns fluree.db.conn.cache
  "A simple default connection-level cache."
  (:require [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util
             #?@(:clj [:refer [try* catch* exception?]])
             #?@(:cljs [:refer-macros [try* catch*] :refer [exception?]])]))

(defn lookup-or-evict
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (do (swap! cache-atom cache/hit k)
          v))))

(defn create-lru-cache
  "Create a cache that starts holds `cache-size` number of entries, bumping out the least
  recently used value after the size is exceeded.."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn memory->cache-size
  "Validate system memory is enough to build a usable cache, then derive cache size."
  [memory]
  (let [memory      (or memory 1000000)        ; default 1MB memory
        object-size 100000                     ; estimate 100kb index node size
        cache-size  (quot memory object-size)] ; number of objects to keep in cache
    (when (< cache-size 10)
      (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.")
                      {:status 400 :error :db/invalid-configuration})))
    cache-size))

(defn lru-lookup
  [cache-atom k value-fn]
  "Given an LRU cache atom, look up value for `k`. If not found, use `value-fn` (a
  function that accepts `k` as its only argument) to produce the value and add it to the
  cache.  If `value-fn` is `nil`, evict the key from the cache."
  (let [out (async/chan)]
    (if-let [v (lookup-or-evict cache-atom k value-fn)]
      (async/put! out v)
      (async/go
        (let [v (async/<! (value-fn k))]
          (when-not (exception? v)
            (swap! cache-atom cache/miss k v))
          (async/put! out v))))
    out))
