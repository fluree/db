(ns fluree.db.conn.cache
  "A simple default connection-level cache."
  (:require [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util
             #?@(:clj [:refer [try* catch* exception?]])
             #?@(:cljs [:refer-macros [try* catch*] :refer [exception?]])]))

(defn- lookup-cache
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (do (swap! cache-atom cache/hit k)
          v))))

(defn- default-object-cache-fn
  "Default synchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (if-let [v (lookup-cache cache-atom k value-fn)]
      v
      (let [v (value-fn k)]
        (swap! cache-atom cache/miss k v)
        v))))

(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn- default-async-cache-fn*
  [cache-atom]
  (fn [k value-fn]
    (let [out (async/chan)]
      (if-let [v (lookup-cache cache-atom k value-fn)]
        (async/put! out v)
        (async/go
          (let [v (async/<! (value-fn k))]
            (when-not (exception? v)
              (swap! cache-atom cache/miss k v))
            (async/put! out v))))
      out)))


(defn default-async-cache-fn
  "Default asynchronous object cache to use for ledger."
  [memory]
  (let [memory             (or memory 1000000) ; default 1MB memory
        memory-object-size (quot memory 100000)]
    (when (< memory-object-size 10)
      (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.")
                      {:status 400 :error :db/invalid-configuration})))
    (default-async-cache-fn* (atom (default-object-cache-factory memory-object-size)))))
