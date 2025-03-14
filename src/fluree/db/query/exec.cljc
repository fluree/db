(ns fluree.db.query.exec
  "Find and format results of queries against database values."
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.walk :as walk]
            [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.having :as having]
            [fluree.db.query.exec.order :as order]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]))

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
  `result-ch` channel collected into a single vector, but handles the special cases.
  `:select-one` queries are handled by only returning the first result from
  `result-ch` in the output channel. Note that this behavior is different from
  queries with `:limit` set to 1 as those queries will return a vector
  containing a single result to the output channel instead of the single result
  alone. `:construct` and output-type `:sparql` need to be collected into a wrapper."
  [q result-ch]
  (cond (:select-one q) (async/take 1 result-ch)

        (:construct q)
        (async/transduce identity (completing conj (partial select/wrap-construct q)) [] result-ch)

        (-> q :opts :output (= :sparql))
        (async/transduce identity (completing conj select/wrap-sparql) [] result-ch)

        :else
        (async/into [] result-ch)))

(defn execute*
  ([ds fuel-tracker q error-ch]
   (execute* ds fuel-tracker q error-ch nil))
  ([ds fuel-tracker q error-ch initial-soln]
   (->> (where/search ds q fuel-tracker error-ch initial-soln)
        (group/combine q)
        (having/filter q error-ch)
        (select/modify q)
        (order/arrange q)
        (drop-offset q)
        (take-limit q))))

(defn execute
  [ds fuel-tracker q error-ch]
  (->> (execute* ds fuel-tracker q error-ch)
       (select/format ds q fuel-tracker error-ch)
       (collect-results q)))

;; TODO: refactor namespace heirarchy so this isn't necessary
(defn subquery-executor
  "Closes over a subquery to allow processing the whole query pipeline from within the
  search."
  [subquery]
  (fn [ds fuel-tracker error-ch]
    (->> (execute* ds fuel-tracker subquery error-ch)
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

(defn query
  "Execute the parsed query `q` against the database value `db`. Returns an async
  channel which will eventually contain a single vector of results, or an
  exception if there was an error."
  [ds fuel-tracker q]
  (go
    (let [error-ch  (async/chan)
          prepped-q (prep-subqueries q)
          result-ch (execute ds fuel-tracker prepped-q error-ch)]
      (async/alt!
        error-ch ([e] e)
        result-ch ([result] result)))))
