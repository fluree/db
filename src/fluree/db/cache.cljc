(ns fluree.db.cache
  "A simple default connection-level cache."
  (:require [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.core.async :as async]
            [fluree.db.flake.index.novelty :as novelty]
            [fluree.db.util :as util :refer [exception?]]
            [fluree.db.util.log :as log]))

(defn create-lru-cache
  "Create a cache that holds `cache-size` number of entries, bumping out the least
  recently used value after the size is exceeded."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn memory->cache-size
  "Validate system memory is enough to build a usable cache, then derive cache size.

   Index leaves are rebalanced when they exceed *overflow-bytes* (default 375KB),
   so the average size is approximately 75% of overflow-bytes.

   Note that an index segment has at least 2 entries - one for the raw data, then
   a 'play to t' entry. The former takes the space, the latter is trivial. 
   Actual memory is ~3x raw JSON file size, but with play-to-t cache entries assume
   each entry is ~average size of JSON file for index.

   With default *overflow-bytes* = 375KB:
   - Average leaf size: ~281KB on-disk, ~375KB in-memory (1:1 with cache-max-mb)
   - 100MB cache holds ~266 segments
   - 1GB cache holds ~2,730 segments
   - 10GB cache holds ~27,306 segments"
  [cache-max-mb]
  (let [memory-mb       (or cache-max-mb 500) ; default 500MB memory
        overflow-mb     (/ novelty/*overflow-bytes* 1024.0 1024.0) ; convert bytes to MB
        avg-segment-mb  (* 0.75 overflow-mb) ; average including JVM overhead
        cache-size      (int (quot memory-mb avg-segment-mb))] ; number of segments to keep in cache
    (when (< cache-size 100)
      (throw (ex-info (str "Must allocate at least " (int (* 100 avg-segment-mb)) "MB of memory for Fluree. You've allocated: " memory-mb " MB.")
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
