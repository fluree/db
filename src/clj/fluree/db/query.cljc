(ns fluree.db.query
  (:refer-clojure :exclude [var? vswap!])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.order :as order]
            [fluree.db.query.exec.having :as having]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.walk :as walk]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.query.fql.parse :as parse])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))


(defn queryable?
  [x]
  (and (where/matcher? x)
       (subject/subject-formatter? x)))

(defn drop-offset
  "Returns a channel containing the stream of solutions from `solution-ch` after
  the `offset` specified by the supplied query. Returns the original
  `solution-ch` if no offset is specified."
  [{:keys [offset]} solution-ch]
  (if offset
    (async/pipe solution-ch
                (async/chan 2 (drop offset)))
    solution-ch))

(defn take-limit
  "Returns a channel that contains at most the specified `:limit` of the supplied
  query solutions from `solution-ch`, if the supplied query has a limit. Returns
  the original `solution-ch` if the supplied query has no specified limit."
  [{:keys [limit]} solution-ch]
  (if limit
    (async/take limit solution-ch)
    solution-ch))

(defn collect-results
  "Returns a channel that will eventually contain the stream of results from the
  `result-ch` channel collected into a single vector, but handles the special
  case of `:select-one` queries by only returning the first result from
  `result-ch` in the output channel. Note that this behavior is different from
  queries with `:limit` set to 1 as those queries will return a vector
  containing a single result to the output channel instead of the single result
  alone."
  [q result-ch]
  (if (:select-one q)
    (async/take 1 result-ch)
    (async/into [] result-ch)))

(defn execute
  ([ds fuel-tracker q error-ch]
   (execute ds fuel-tracker q error-ch nil))
  ([ds fuel-tracker q error-ch initial-soln]
   (->> (where/search ds q fuel-tracker error-ch initial-soln)
        (group/combine q)
        (having/filter q error-ch)
        (select/modify q)
        (order/arrange q)
        (drop-offset q)
        (take-limit q))))

;; TODO: refactor namespace heirarchy so this isn't necessary
(defn subquery-executor
  "Closes over a subquery to allow processing the whole query pipeline from within the
  search."
  [subquery]
  (fn [ds fuel-tracker error-ch]
    (->> (execute ds fuel-tracker subquery error-ch)
         (select/subquery-format ds subquery fuel-tracker error-ch))))

(defn prep-subqueries
  "Takes a query and returns a query with all subqueries within replaced by a subquery
  executor function."
  [q]
  (update q :where #(walk/postwalk (fn [x]
                                     (if (= :query (where/pattern-type x))
                                       (let [subquery (second x)]
                                         (where/->pattern :query (subquery-executor subquery)))
                                       x))
                                   %)))

(defn q
  "Returns core async channel with results or exception"
  ([ds query-map]
   (q ds nil query-map))
  ([ds fuel-tracker query-map]
   (go-try
     (let [query     (-> query-map parse/parse-query prep-subqueries)
           error-ch  (async/chan)
           result-ch (->> (execute ds fuel-tracker query error-ch)
                          (select/format ds query fuel-tracker error-ch)
                          (collect-results query))]
       (async/alt!
         error-ch ([e] (throw e))
         result-ch ([result] result))))))
