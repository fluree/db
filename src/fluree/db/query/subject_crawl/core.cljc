(ns fluree.db.query.subject-crawl.core
  (:require [clojure.core.async :refer [go <!] :as async]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.subject-crawl.subject :refer [subj-crawl]]
            [fluree.db.query.subject-crawl.common :refer [order-results]]
            [fluree.db.query.fql.resp :as legacy-resp]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn retrieve-select-spec
  "Returns a parsed selection specification.

  This strategy is only deployed if there is a single selection graph crawl,
  so this assumes this case is true in code."
  [db {:keys [select opts] :as _parsed-query}]
  (:spec select))

(defn relationship-binding
  [{:keys [vars] :as opts}]
  (async/go-loop [[next-vars & rest-vars] vars
                  acc []]
    (if next-vars
      (let [opts' (assoc opts :vars next-vars)
            res   (<? (subj-crawl opts'))]
        (recur rest-vars (into acc res)))
      acc)))

(defn build-finishing-fn
  "After results are processed, the response may be modified if:
  - order-by exists, in which case we need to perform a sort
  - selectOne? exists, in which case we take the (first result)
  - pretty-print is true, in which case each result needs to get embedded in a map"
  [{:keys [selectOne? order-by pretty-print limit offset] :as parsed-query}]
  (let [fns (cond-> []
                    selectOne? (conj (fn [result] (first result)))
                    pretty-print (conj (let [select-var (-> parsed-query
                                                            :select
                                                            :select
                                                            first
                                                            :variable
                                                            str
                                                            (subs 1))]
                                         (fn [result]
                                           (mapv #(array-map select-var %) result))))
                    order-by (conj (fn [result]
                                     (order-results result order-by limit offset))))]
    (if (empty? fns)
      identity
      (apply comp fns))))

(defn simple-subject-crawl
  "Executes a simple subject crawl analytical query execution strategy.

  Strategy involves:
  (a) Get a list of subjects from first where clause
  (b) select all flakes for each subject
  (c) filter subjects based on subsequent where clause(s)
  (d) apply offset/limit for (c)
  (e) send result into :select graph crawl"
  [db {:keys [vars ident-vars where limit offset fuel rel-binding?
              order-by opts] :as parsed-query}]
  (log/trace "Running simple subject crawl query:" parsed-query)
  (let [error-ch    (async/chan)
        f-where     (first where)
        rdf-type?   (= :rdf/type (:type f-where))
        filter-map  (:s-filter (second where))
        cache       (volatile! {})
        fuel-vol    (volatile! 0)
        select-spec (retrieve-select-spec db parsed-query)
        compact-fn  (->> parsed-query :context json-ld/compact-fn)
        result-fn   (partial json-ld-resp/flakes->res db cache compact-fn fuel-vol fuel select-spec 0)
        finish-fn   (build-finishing-fn parsed-query)
        opts        {:rdf-type?     rdf-type?
                     :db            db
                     :cache         cache
                     :fuel-vol      fuel-vol
                     :max-fuel      fuel
                     :select-spec   select-spec
                     :error-ch      error-ch
                     :vars          vars
                     :ident-vars    ident-vars
                     :filter-map    filter-map
                     :limit         (if order-by util/max-long limit) ;; if ordering, limit performed by finish-fn after sort
                     :offset        (if order-by 0 offset)
                     :permissioned? (not (get-in db [:policy :f/view :root?]))
                     :parallelism   3
                     :f-where       f-where
                     :parse-json?   (:parse-json? opts)
                     :query         parsed-query
                     :result-fn     result-fn
                     :finish-fn     finish-fn}]
    (log/trace "simple-subject-crawl opts:" opts)
    (if rel-binding?
      (relationship-binding opts)
      (subj-crawl opts))))
