(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [vswap! try* catch*]]
            [fluree.db.query.analytical-parse :as q-parse]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.query.subject-crawl.core :refer [simple-subject-crawl]]
            [fluree.db.query.compound :as compound]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.constants :as const])
  (:refer-clojure :exclude [vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)


(defn process-where-item
  [db cache compact-fn fuel-vol fuel where-item spec inVector? error-ch]
  (go
    (try*
     (loop [[spec-item & r'] spec
            result-item []]
       (if spec-item
         (let [{:keys [selection in-n iri? o-var? grouped? function]} spec-item
               value  (nth where-item in-n)
               value* (cond
                        ;; there is a sub-selection (graph crawl)
                        selection
                        (let [flakes (<? (query-range/index-range db :spot = [value]))]
                          (<? (json-ld-resp/flakes->res db cache compact-fn fuel-vol fuel (:spec spec-item) 0 flakes)))

                        grouped?
                        (cond->> value
                          o-var?   (mapv first)
                          function function)

                        ;; subject id coming it, we know it is an IRI so resolve here
                        iri?
                        (or (get @cache value)
                            (let [c-iri (<? (db-proto/-iri db value compact-fn))]
                              (vswap! cache assoc value c-iri)
                              c-iri))

                        o-var?
                        (let [[val datatype] value]
                          (if (= const/$xsd:anyURI datatype)
                            (or (get @cache val)
                                (let [c-iri (<? (db-proto/-iri db val compact-fn))]
                                  (vswap! cache assoc val c-iri)
                                  c-iri))
                            val))

                        :else
                        value)]
           (recur r' (conj result-item value*)))
         (if inVector?
           result-item
           (first result-item))))
     (catch* e
             (log/error e "Error processing query")
             (>! error-ch e)))))


(defn process-select-results
  "Processes where results into final shape of specified select statement."
  [db {:keys [select fuel compact-fn group-by] :as _parsed-query} error-ch where-ch]
  (let [{:keys [spec inVector?]} select
        cache    (volatile! {})
        fuel-vol (volatile! 0)
        {:keys [group-finish-fn]} group-by
        finish-group-xf (if group-finish-fn
                          (map group-finish-fn)
                          (map identity))
        process-ch (async/chan 2 finish-group-xf)
        out-ch     (async/chan)]
    (->> (async/pipe where-ch process-ch)
         (async/pipeline-async 2
                               out-ch
                               (fn [where-item ch]
                                 (async/pipe (process-where-item db cache compact-fn fuel-vol fuel where-item spec inVector? error-ch)
                                             ch))))
    out-ch))

(defn extract-vals
  [input positions]
  (mapv (partial nth input)
        positions))

(defn add-group-values
  [group values]
  (-> group
      (or [])
      (conj values)))

(defn group-results
  [{:keys [parsed out-vars] :as group-by} result-ch]
  (if-let [grouping-positions (not-empty (mapv :in-n parsed))] ; returns 'n' positions of values used for grouping
    (let [grouped-positions (filterv                    ; returns 'n' positions of values that are being grouped
                             (complement (set grouping-positions))
                             (range (count out-vars)))
          group-ch (async/reduce (fn [groups result-chunk]
                                   (reduce (fn [groups result]
                                             (let [group-key  (extract-vals result grouping-positions)
                                                   group-vals (extract-vals result grouped-positions)]
                                               (update groups group-key add-group-values group-vals)))
                                           groups result-chunk))
                                 {} result-ch)]
      (async/pipe group-ch (async/chan 1 (comp cat
                                               (map (fn [[group-key group]]
                                                      (conj group-key group)))))))
    (async/reduce into [] result-ch)))


(defn compare-by-first
  [cmp]
  (fn [x y]
    (cmp (first x) (first y))))

(defn sort-groups
  [result-cmp groups]
  (let [group-cmp (compare-by-first result-cmp)]
    (->> groups
         (map (partial sort result-cmp)) ; sort results in each group
         (sort group-cmp))))             ; then sort all the groups

(defn order-result-groups
  [order-by group-ch]
  (if-let [cmp (:comparator order-by)]
    (let [group-coll-ch (async/into [] group-ch)
          sort-xf       (comp (map (partial sort-groups cmp))
                              cat)
          sorted-ch     (async/chan 1 sort-xf)]
      (async/pipe group-coll-ch sorted-ch))
    group-ch))

(defn- ad-hoc-query
  "Legacy ad-hoc query processor"
  [db {:keys [fuel order-by group-by] :as parsed-query}]
  (let [max-fuel fuel
        fuel     (volatile! 0)
        error-ch (async/chan) ]
    (->> (compound/where db parsed-query fuel max-fuel error-ch)
         (group-results group-by)
         (order-result-groups order-by)
         (process-select-results db parsed-query error-ch))))

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
  "Returns core async channel with results or exception"
  [db query-map]
  (log/debug "Running query:" query-map)
  (if (cache? query-map)
    (cache-query db query-map)
    (let [parsed-query (q-parse/parse db query-map)
          db*          (assoc db :ctx-cache (volatile! {}))] ;; allow caching of some functions when available
      (if (= :simple-subject-crawl (:strategy parsed-query))
        (simple-subject-crawl db* parsed-query)
        (cond-> (async/into [] (ad-hoc-query db* parsed-query))
                (:selectOne? parsed-query) (first-async))))))
