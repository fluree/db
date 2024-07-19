(ns fluree.db.query.exec
  "Find and format results of queries against database values."
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.order :as order]
            [fluree.db.query.exec.having :as having]
            [fluree.db.util.core :as util]
            [fluree.db.query.exec.select.subject :as subject]
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

(defn extract-subquery
  [query-smt]
  (when (and (sequential? query-smt)
             (= :query (first query-smt)))
    (second query-smt)))

(defn execute
  [ds fuel-tracker q error-ch initial-soln]
  (->> (where/search ds q fuel-tracker error-ch initial-soln)
       (group/combine q)
       (having/filter q error-ch)
       (select/modify q)
       (order/arrange q)
       (select/format ds q fuel-tracker error-ch)
       (drop-offset q)
       (take-limit q)
       (collect-results q)))

(defn execute-subquery
  [ds fuel-tracker q error-ch initial-soln]
  (->> (where/search ds q fuel-tracker error-ch initial-soln)
       (group/combine q)
       (having/filter q error-ch)
       (select/modify q)
       (order/arrange q)
       (select/subquery-format ds q fuel-tracker error-ch)
       (drop-offset q)
       (take-limit q)))

(defn collect-subqueries
  "With multiple subqueries each having its own solution channel,
  merge them into a single solution."
  [subquery-chans]
  (when (seq subquery-chans)
    (if (= 1 (count subquery-chans))
      (first subquery-chans)
      (let [out-ch (async/chan)]
        (go
          (let [all-solns (loop [chans subquery-chans
                                 acc   []]
                            (if-let [next-chan (first chans)]
                              (let [solns (async/<! (async/into [] next-chan))]
                                (recur (rest chans) (conj acc solns)))
                              acc))
                results   (util/cartesian-merge all-solns)]
            (async/onto-chan! out-ch results)))
        out-ch))))

(defn query*
  "Iterates over query :where to identify any subqueries,
  and if they exist, executes them and collects them into initial-soln.

  Recursive, in that subqueries can have subqueries.

  Once subquery solution chans are identified, if any, the parent query is executed."
  [ds fuel-tracker q error-ch subquery?]
  (loop [[where-smt & r] (:where q)
         where*           [] ;; where clause with subqueries removed
         subquery-results []]
    (if where-smt
      (if-let [subquery (extract-subquery where-smt)]
        (let [result-ch (query* ds fuel-tracker subquery error-ch true)] ;; might be multiple nested subqueries
          (recur r where* (conj subquery-results result-ch)))
        (recur r (conj where* where-smt) subquery-results))
      ;; end of subqueries search... if result-chans extract as initial soln to where, else execute where
      (let [q*            (assoc q :where (not-empty where*))
            subquery-soln (collect-subqueries subquery-results)]
        (if subquery?
          (execute-subquery ds fuel-tracker q* error-ch subquery-soln)
          (execute ds fuel-tracker q* error-ch subquery-soln))))))

(defn query
  "Execute the parsed query `q` against the database value `db`. Returns an async
  channel which will eventually contain a single vector of results, or an
  exception if there was an error."
  [ds fuel-tracker q]
  (go
    (let [error-ch  (async/chan)
          result-ch (query* ds fuel-tracker q error-ch false)]
      (async/alt!
       error-ch ([e] e)
       result-ch ([result] result)))))
