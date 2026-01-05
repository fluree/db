(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.constants :as const]
            [fluree.db.dataset :as dataset]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [go-try <?]])
  (:refer-clojure :exclude [var? vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn- get-aggregate-info
  "Extract aggregate metadata from a selector, if present."
  [selector]
  (-> selector meta ::select/aggregate-info))

(defn- simple-count-distinct-spec
  "Check a PARSED query for a simple COUNT(DISTINCT ?s) pattern that can be
  optimized with a direct index scan.

  Requirements:
    - Single selector that is a count-distinct aggregate
    - Single tuple pattern in :where
    - Subject is a variable matching the aggregated variable
    - Predicate is a concrete IRI (not a variable)
    - No GROUP BY, HAVING, ORDER BY, VALUES, or CONSTRUCT

  Returns a spec map {:pred-iri <iri>, :alias sym-or-nil, :output keyword}
  or nil if the optimization does not apply."
  [parsed-query]
  (let [{:keys [select where group-by order-by having values construct opts]} parsed-query
        output (or (:output opts) :fql)]
    (when (and (vector? select)
               (= 1 (count select))
               (vector? where)
               (= 1 (count where))
               (nil? group-by)
               (nil? order-by)
               (nil? having)
               (nil? values)
               (nil? construct))
      (let [selector  (first select)
            agg-info  (get-aggregate-info selector)
            pattern   (first where)
            ;; Pattern is either a MapEntry (with type key) or plain tuple
            ptype     (where/pattern-type pattern)
            pdata     (where/pattern-data pattern)]
        (when (and agg-info
                   (= 'count-distinct (:fn-name agg-info))
                   (= 1 (count (:vars agg-info)))
                   (#{:tuple :class} ptype)
                   (vector? pdata)
                   (= 3 (count pdata)))
          (let [[s-mch p-mch _o-mch] pdata
                agg-var    (first (:vars agg-info))
                subj-var   (where/get-variable s-mch)
                pred-iri   (where/get-iri p-mch)
                ;; For AsSelector, get the bind-var for the alias
                alias      (when (instance? fluree.db.query.exec.select.AsSelector selector)
                             (:bind-var selector))]
            (when (and subj-var
                       (= subj-var agg-var)
                       pred-iri
                       (not (where/get-variable p-mch)) ;; predicate must not be a variable
                       ;; SPARQL output needs alias for binding name
                       (or (= :fql output)
                           (and (= :sparql output) (some? alias))))
              {:pred-iri pred-iri
               :alias    alias
               :output   output})))))))

(defn- count-distinct-subject-fast
  "Execute a simple COUNT(DISTINCT ?s) query directly against an index without
  building full query solutions.

  Returns a channel containing either:
    - FQL output: a single FQL-style result vector [[n]]
    - SPARQL output: a SPARQL JSON results map

  Only used when simple-count-distinct-subject-spec has matched."
  [ds {:keys [pred-iri alias output]}]
  (go-try
    (let [;; unwrap dataset if present – for now we only handle a single active
          ;; graph, which matches the common FROM <graph> use case.
          db   (if (dataset/dataset? ds)
                 (dataset/get-active-graph ds)
                 ds)]
      (if (nil? db)
        ;; Shouldn't happen with properly constructed DataSets, but handle gracefully.
        (if (= :sparql output)
          (let [v (-> alias name (subs 1))]
            {"head" {"vars" [v]}
             "results" {"bindings" [{v {"value" "0"
                                        "type" "literal"
                                        "datatype" const/iri-xsd-integer}}]}})
          [[0]])
        (let [;; Scan the :post index for all flakes with the given predicate.
              ;; This returns a vector of flakes sorted by predicate, then
              ;; object, then subject. To ensure correctness even when a
              ;; subject has multiple objects for the same predicate, we count
              ;; distinct subject SIDs explicitly.
              ;; Use protocol method for AsyncDB/FlakeDB interoperability.
              flakes       (<? (dbproto/-index-range db :post = [pred-iri] {}))
              distinct-sids (persistent!
                             (reduce (fn [acc f]
                                       (let [sid (flake/s f)]
                                         (if (some? sid)
                                           (conj! acc sid)
                                           acc)))
                                     (transient #{})
                                     flakes))
              cnt          (count distinct-sids)]
          (if (= :sparql output)
            (let [v (-> alias name (subs 1))]
              {"head" {"vars" [v]}
               "results" {"bindings" [{v {"value" (str cnt)
                                          "type" "literal"
                                          "datatype" const/iri-xsd-integer}}]}})
            [[cnt]]))))))
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
       (let [pq (parse/parse-query query-map)]
         (if-let [spec (simple-count-distinct-spec pq)]
           (<? (count-distinct-subject-fast ds spec))
           (let [oq (<? (optimize/optimize ds pq))]
             (<? (exec/query ds tracker oq)))))))))

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
