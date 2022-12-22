(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [clojure.spec.alpha :as s]
            [clojure.walk :refer [keywordize-keys]]
            [fluree.db.util.core #?(:clj :refer :cljs :refer-macros) [try* catch*]
             :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [vswap! try* catch*]]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.query.subject-crawl.core :refer [simple-subject-crawl]]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.constants :as const])
  (:refer-clojure :exclude [var? vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn cache-query
  "Returns already cached query from cache if available, else
  executes and stores query into cache."
  [{:keys [network ledger-id block auth conn] :as db} query-map]
  ;; TODO - if a cache value exists, should max-fuel still be checked and throw if not enough?
  (let [oc        (:object-cache conn)
        query*    (update query-map :opts dissoc :fuel :max-fuel)
        cache-key [:query network ledger-id block auth query*]]
    ;; object cache takes (a) key and (b) fn to retrieve value if null
    (oc cache-key
        (fn [_]
          (let [pc (async/promise-chan)]
            (async/go
              (let [res (async/<! (query db (assoc-in query-map [:opts :cache]
                                                      false)))]
                (async/put! pc res)))
            pc)))))


(defn cache?
  "Returns true if query was requested to run from the cache."
  [{:keys [opts] :as _query-map}]
  #?(:clj (:cache opts) :cljs false))


(defn first-async
  "Returns first result of a sequence returned from an async channel."
  [ch]
  (go-try
    (let [res (<? ch)]
      (first res))))


(defn query
  [db query-map]
  (if (cache? query-map)
    (cache-query db query-map)))

(defn query
  "Returns core async channel with results or exception"
  [db query-map]
  (log/debug "Running query:" query-map)
  (if (cache? query-map)
    (cache-query db query-map)
    (let [parsed-query (-> query-map
                           syntax/validate
                           (parse/parse db))
          db*          (assoc db :ctx-cache (volatile! {}))] ;; allow caching of some functions when available
      (if (= :simple-subject-crawl (:strategy parsed-query))
        (simple-subject-crawl db* parsed-query)
        (exec/execute db* parsed-query)))))
