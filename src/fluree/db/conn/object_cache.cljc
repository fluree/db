(ns fluree.db.conn.object-cache
  (:require [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]))

#?(:clj (set! *warn-on-reflection* true))

(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn default-object-cache
  "Default object cache. Returns an atom whose value is the
  default-object-cache-factory (an LRU cache).

  With the LRU cache, we are managing items, not memory consumption
  and utilize Fluree's conservative estimate that 100kb per item, as
  that is the threshold of Fluree indexes. Large blocks, or large
  query cache items could present a problem in edge cases."
  [memory]
  (let [cache-size (quot memory 100000)]                    ;; avg 100kb per cache object
    (when (< cache-size 10)
      (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: "
                           memory " bytes.")
                      {:status 400 :error :db/invalid-configuration})))
    (atom (default-object-cache-factory cache-size))))

(defn default-object-cache-fn
  "Default object cache to use for ledger.
  Supply the atom that contains the cache object."
  [cache-atom]
  (fn [k value-fn]
    (if (nil? value-fn)
      (swap! cache-atom cache/evict k)
      (if-let [v (get @cache-atom k)]
        (do (swap! cache-atom cache/hit k)
            v)
        (let [v (value-fn k)]
          (swap! cache-atom cache/miss k v)
          v)))))

(defn clear-cache
  "Clears a cache-atom, resetting original state."
  [cache-atom]
  (swap! cache-atom empty))
