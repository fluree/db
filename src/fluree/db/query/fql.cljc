(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [go-try <?]])
  (:refer-clojure :exclude [var? vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn cache-query
  "Returns already cached query from cache if available, else
  executes and stores query into cache."
  [{:keys [ledger-alias t auth conn] :as db} query-map]
  ;; TODO - if a cache value exists, should max-fuel still be checked and throw if not enough?
  (let [oc        (:object-cache conn)
        query*    (update query-map :opts dissoc :fuel :max-fuel)
        cache-key [:query ledger-alias t auth query*]]
    ;; object cache takes (a) key and (b) fn to retrieve value if null
    (oc cache-key
        (fn [_]
          (let [pc (async/promise-chan)]
            (go
              (let [res (<! (query db (assoc-in query-map [:opts :cache]
                                                false)))]
                (async/put! pc res)))
            pc)))))

#?(:clj
   (defn cache?
     "Returns true if query was requested to run from the cache."
     [query-map]
     (-> query-map :opts :cache))

   :cljs
   (defn cache?
     "Always returns false because caching is not supported from CLJS."
     [_]
     false))

(defn query
  "Returns core async channel with results or exception"
  ([ds query-map]
   (query ds nil query-map))
  ([ds tracker query-map]
   (if (cache? query-map)
     (cache-query ds query-map)
     (go-try
       (let [pq (parse/parse-query query-map)
             oq (<? (optimize/optimize ds pq))]
         (<? (exec/query ds tracker oq)))))))

(defn explain
  "Returns query execution plan without executing the query.
  Returns core async channel with query plan or exception."
  [ds query-map]
  (let [pq (try*
             (parse/parse-query query-map)
             (catch* e e))]
    (if (util/exception? pq)
      (async/go pq)
      (optimize/explain ds pq))))
