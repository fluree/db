(ns fluree.db.query.fql
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.dataset :as dataset]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.json-ld :as json-ld])
  (:refer-clojure :exclude [var? vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn- parse-count-distinct-selector
  "Given a FQL :select vector, detect a single COUNT(DISTINCT ?var) selector and
  return {:agg-var sym, :alias sym-or-nil} if present."
  [select]
  (when (and (vector? select)
             (= 1 (count select)))
    (let [s (first select)]
      (when (string? s)
        (try
          (let [form (#?(:clj clojure.core/read-string
                         :cljs js/eval)
                      s)]
            (cond
              ;; (as (count-distinct ?v) ?alias)
              (and (seq? form)
                   (= 'as (first form)))
              (let [[_ inner alias] form]
                (when (and (seq? inner)
                           (= 'count-distinct (first inner))
                           (= 2 (count inner)))
                  {:agg-var (second inner)
                   :alias   alias}))

              ;; (count-distinct ?v)
              (and (seq? form)
                   (= 'count-distinct (first form))
                   (= 2 (count form)))
              {:agg-var (second form)
               :alias   nil}

              :else
              nil))
          (catch #?(:clj Exception :cljs :default) _nil
            nil))))))

(defn- simple-count-distinct-subject-spec
  "Detect a very simple COUNT(DISTINCT ?s) query shape that we can execute
  directly against the index without building full solutions:

    - single triple in :where
    - single aggregate COUNT(DISTINCT ?s)
    - no GROUP BY, HAVING, ORDER BY, VALUES, subqueries, or CONSTRUCT

  Note: FROM clauses are ignored when a db is passed directly to query.
  FROM is only meaningful for query-connection which loads ledgers.

  Returns a spec map {:subject-var \"?s\", :pred-iri <iri>, :alias sym-or-nil}
  or nil if the optimization does not apply."
  [q]
  (when (map? q)
    (let [{:keys [select where group-by groupBy having order-by orderBy values construct context]} q]
      (when (and (sequential? select)
                 (seq select)
                 ;; Guard against non-seq values in group-by keys (e.g. symbols or errors)
                 (not (or (and (sequential? group-by) (seq group-by))
                          (and (sequential? groupBy) (seq groupBy))))
                 (nil? having)
                 (nil? (:having q))
                 (nil? order-by)
                 (nil? orderBy)
                 ;; Only treat :values as present if it's a non-empty sequential;
                 ;; avoid calling seq on non-sequential values.
                 (not (and (sequential? values) (seq values)))
                 (nil? construct)
                 (vector? where)
                 (= 1 (count where)))
        (when-let [{:keys [agg-var alias]} (parse-count-distinct-selector select)]
          (let [subj-var-str (str agg-var)
                clause       (first where)]
            (when (and (map? clause)
                       (= subj-var-str (get clause "@id")))
              (let [pred-keys (remove #{"@id" "@context"} (keys clause))]
                (when (= 1 (count pred-keys))
                  (let [pred-key (first pred-keys)
                        ;; Expand predicate using the query context so it matches
                        ;; the IRI stored in flakes. Context must be parsed first.
                        parsed-ctx (json-ld/parse-context context)
                        pred-iri   (json-ld/expand-iri pred-key parsed-ctx)]
                    {:subject-var subj-var-str
                     :pred-iri    pred-iri
                     :alias       alias}))))))))))

(defn- count-distinct-subject-fast
  "Execute a simple COUNT(DISTINCT ?s) query directly against an index without
  building full query solutions.

  Returns a channel containing a single FQL-style result vector [[n]].
  Only used when simple-count-distinct-subject-spec has matched."
  [ds {:keys [pred-iri]}]
  (go-try
    (let [;; unwrap dataset if present – for now we only handle a single active
          ;; graph, which matches the common FROM <graph> use case.
          db   (if (dataset/dataset? ds)
                 (or (dataset/get-active-graph ds)
                     ;; no active graph – behave like empty result
                     (reduced nil))
                 ds)]
      (if (nil? db)
        []
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
          ;; FQL :output :fql expects a vector of rows, each a vector of values.
          ;; For COUNT queries there is a single row with a single numeric value.
          [[cnt]])))))
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
       (if-let [spec (simple-count-distinct-subject-spec query-map)]
         ;; Fast path: execute simple COUNT(DISTINCT ?s) directly against index.
         (<? (count-distinct-subject-fast ds spec))
         ;; General path: full parse/optimize/exec pipeline.
         (let [pq (parse/parse-query query-map)
               oq (<? (optimize/optimize ds pq))]
           (<? (exec/query ds tracker oq))))))))

(defn explain
  "Returns query execution plan without executing the query.
  Returns core async channel with query plan or exception."
  [ds query-map]
  (let [pq (try*
             (parse/parse-query query-map)
             (catch* e e))]
    (if (util/exception? pq)
      (async/go pq)
      (optimize/-explain ds pq))))
