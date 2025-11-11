(ns fluree.db.cache
  "A simple default connection-level cache."
  (:require #?(:clj [fluree.db.util.graalvm :as graalvm])
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.util :as util :refer [exception?]]
            [fluree.db.util.log :as log]))

(defn default-cache-max-mb
  "Calculates default cache size based on available memory.

   Three cases:
   1. JVM with -Xmx: Uses maxMemory() which returns the -Xmx setting
   2. GraalVM native-image (Lambda/containers): Uses totalMemory() which reflects
      actual container/cgroup memory limits
   3. Node.js/JavaScript: Returns conservative 1000 MB default

   Returns 50% of available memory for cache to leave room for other operations."
  []
  #?(:clj
     (let [runtime      (Runtime/getRuntime)
           max-memory   (.maxMemory runtime)
           total-memory (.totalMemory runtime)
           is-graalvm?  (graalvm/runtime?)
           effective-mb (if is-graalvm?
                          (/ total-memory 1024.0 1024.0)   ; Case 2: GraalVM
                          (/ max-memory 1024.0 1024.0))    ; Case 1: JVM
           cache-mb     (int (/ effective-mb 2.0))]
       (log/info (str "Detected " (int effective-mb) "MB available memory"
                      (when is-graalvm? " (GraalVM native-image)")
                      ", setting default cache to " cache-mb "MB"))
       cache-mb)
     :cljs
     1000)) ; Case 3: Node.js/JavaScript

(defn create-lru-cache
  "Create a cache that holds `cache-size` number of entries, bumping out the least
  recently used value after the size is exceeded."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn memory->cache-size
  "Validate system memory is enough to build a usable cache, then derive cache size.

   Index leaves are rebalanced when they exceed overflow-bytes (default 375KB),
   so the average size is approximately 75% of overflow-bytes (~281KB on-disk).

   Note that an index segment has at least 2 entries - one for the raw data, then
   a 'play to t' entry. The former takes the majority of the space. It can have other
   'play to t' entries as well, but to be conservative we assume 2 entries per segment.

   With default overflow-bytes = 375KB:
   - Average in-memory size per segment: ~183KB
   - 1GB cache holds ~5,592 segments
   - 10GB cache holds ~55,924 segments
   - 20GB cache holds ~111,848 segments

   Optional parameters:
   - cache-max-mb: Maximum memory in MB to use for cache (default 500MB)
   - overflow-bytes: Override for index leaf overflow threshold (defaults to const/default-overflow-bytes)"
  ([cache-max-mb] (memory->cache-size cache-max-mb const/default-overflow-bytes))
  ([cache-max-mb overflow-bytes]
   (let [memory-mb       (or cache-max-mb 500) ; default 500MB memory
         overflow-mb     (/ overflow-bytes 1024.0 1024.0) ; convert bytes to MB
         avg-segment-mb  (* 0.5 overflow-mb) ; average in-memory size per segment
         cache-size      (int (quot memory-mb avg-segment-mb))] ; number of segments to keep in cache
     (when (< cache-size 100)
       (throw (ex-info (str "Must allocate at least " (int (* 100 avg-segment-mb)) "MB of memory for Fluree. You've allocated: " memory-mb " MB.")
                       {:status 400 :error :db/invalid-configuration})))
     (log/info (str "Initialized LRU cache: " memory-mb "MB capacity, holding up to " cache-size " index segments"))
     cache-size)))

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
