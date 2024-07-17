(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.query.subject-crawl.core :refer [simple-subject-crawl]]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.exec :as exec])
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

(defn cache?
  "Returns true if query was requested to run from the cache."
  [{:keys [opts] :as _query-map}]
  #?(:clj (:cache opts)
     :cljs false))

(defn query
  "Returns core async channel with results or exception"
  ([ds query-map]
   (query ds nil query-map))
  ([ds fuel-tracker query-map]
   (if (cache? query-map)
     (cache-query ds query-map)
     (let [q   (try*
                 (parse/parse-query query-map)
                 (catch* e e))]
       (if (util/exception? q)
         (async/to-chan! [q])
         (if (= :simple-subject-crawl (:strategy q))
           (simple-subject-crawl ds q)
           (exec/query ds fuel-tracker q)))))))
