(ns fluree.db.query.subject-crawl.core
  (:require #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.query.fql-parser :refer [parse-db]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.query.subject-crawl.subject :refer [subj-crawl]]
            [fluree.db.query.subject-crawl.rdf-type :refer [rdf-type-crawl]]
            [fluree.db.query.subject-crawl.common :refer [order-results]]))

#?(:clj (set! *warn-on-reflection* true))

(defn retrieve-select-spec
  "Returns a parsed selection specification.

  This strategy is only deployed if there is a single selection graph crawl,
  so this assumes this case is true in code."
  [db {:keys [select opts] :as parsed-query}]
  (let [select-smt (-> select
                       :select
                       first
                       :selection)]
    (parse-db db select-smt opts)))

(defn relationship-binding
  [{:keys [rdf-type? vars] :as opts}]
  (async/go-loop [[next-vars & rest-vars] vars
                  acc []]
    (if next-vars
      (let [opts' (assoc opts :vars next-vars)
            res   (if rdf-type?
                    (<? (rdf-type-crawl opts'))
                    (<? (subj-crawl opts')))]
        (recur rest-vars (into acc res)))
      acc)))

(defn build-finishing-fn
  "After results are processed, the response may be modified if:
  - order-by exists, in which case we need to perform a sort
  - selectOne? exists, in which case we take the (first result)
  - pretty-print is true, in which case each result needs to get embedded in a map"
  [{:keys [selectOne? order-by pretty-print] :as parsed-query}]
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
                    order-by (conj (fn [result] (order-results result order-by))))]
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
  [db {:keys [vars where limit offset fuel rel-binding?] :as parsed-query}]
  (let [error-ch    (async/chan)
        f-where     (first where)
        rdf-type?   (= :rdf/type (:type f-where))
        filter-map  (:s-filter (second where))
        cache       (volatile! {})
        fuel-vol    (volatile! 0)
        select-spec (retrieve-select-spec db parsed-query)
        finish-fn   (build-finishing-fn parsed-query)
        opts        {:rdf-type?     rdf-type?
                     :db            db
                     :cache         cache
                     :fuel-vol      fuel-vol
                     :max-fuel      fuel
                     :select-spec   select-spec
                     :error-ch      error-ch
                     :vars          vars
                     :filter-map    filter-map
                     :limit         limit
                     :offset        offset
                     :permissioned? (not (get-in db [:permissions :root?]))
                     :parallelism   3
                     :f-where       f-where
                     :query         parsed-query
                     :finish-fn     finish-fn}]
    (if rel-binding?
      (relationship-binding opts)
      (if rdf-type?
        (rdf-type-crawl opts)
        (subj-crawl opts)))))

