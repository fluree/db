(ns fluree.db.query.exec
  "Find and format results of queries against database values."
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn split-solution-by
  [variables solution]
  (let [group-key   (mapv (fn [v]
                            (-> (get solution v)
                                (select-keys [::where/val ::where/datatype])))
                          variables)
        grouped-val (apply dissoc solution variables)]
    [group-key grouped-val]))

(defn assoc-coll
  [m k v]
  (update m k (fn [coll]
                (-> coll
                    (or [])
                    (conj v)))))

(defn group-solution
  [groups [group-key grouped-val]]
  (assoc-coll groups group-key grouped-val))

(defn merge-with-colls
  [m1 m2]
  (reduce (fn [merged k]
            (let [v (get m2 k)]
              (assoc-coll merged k v)))
          m1 (keys m2)))

(defn unwind-groups
  [grouping groups]
  (reduce-kv (fn [solutions group-key grouped-vals]
               (let [merged-vals (->> grouped-vals
                                      (reduce merge-with-colls {})
                                      (reduce-kv (fn [m k v]
                                                   (assoc m k {::where/var       k
                                                               ::where/val       v
                                                               ::where/datatype ::grouping}))
                                                 {}))
                     solution    (into merged-vals
                                       (map vector grouping group-key))]
                 (conj solutions solution)))
             [] groups))

(defn implicit-grouping
  [select]
  (when (some select/implicit-grouping? select)
    [nil]))

(defmethod select/display ::grouping
  [match db select-cache compact error-ch]
  (let [group (::where/val match)]
    (->> group
         (map (fn [grouped-val]
                (select/display grouped-val db select-cache compact error-ch)))
         (async/map vector))))

(defn group
  "Returns a channel of solutions from `solution-ch` collected into groups defined
  by the `:group-by` clause specified in the supplied query."
  [{:keys [group-by select]} solution-ch]
  (if-let [grouping (or group-by
                        (implicit-grouping select))]
    (-> (async/transduce (map (partial split-solution-by grouping))
                         (completing group-solution
                                     (partial unwind-groups grouping))
                         {}
                         solution-ch)
        (async/pipe (async/chan 2 cat)))
    solution-ch))

(defn compare-vals
  [x-val x-dt y-val y-dt]
  (let [dt-cmp (compare x-dt y-dt)]
    (if (zero? dt-cmp)
      (compare x-val y-val)
      dt-cmp)))

(defn compare-solutions-by
  [variable direction x y]
  (let [x-var (get x variable)
        x-val (::where/val x-var)
        x-dt  (::where/datatype x-var)

        y-var (get y variable)
        y-val (::where/val y-var)
        y-dt  (::where/datatype y-var)]
    (if (= direction :asc)
      (compare-vals x-val x-dt y-val y-dt)
      (compare-vals y-val y-dt x-val x-dt))))

(defn compare-solutions
  [ordering x y]
  (reduce (fn [comparison [variable direction]]
            (let [cmp (compare-solutions-by variable direction x y)]
              (if (zero? cmp)
                comparison
                (reduced cmp))))
          0 ordering))

(defn order
  "Returns a channel containing all solutions from `solution-ch` sorted by the
  ordering specified by the `:order-by` clause of the supplied parsed query.
  Note that all solutions from `solution-ch` are first loaded into memory before
  they are sorted in place and placed individually on the output channel."
  [{:keys [order-by]} solution-ch]
  (if order-by
    (let [comparator (partial compare-solutions order-by)
          coll-ch    (async/into [] solution-ch)
          ordered-ch (async/chan 2 (comp (map (partial sort comparator))
                                         cat))]
      (async/pipe coll-ch ordered-ch))
    solution-ch))

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
  (if (:selectOne q)
    (async/take 1 result-ch)
    (async/into [] result-ch)))

(defn query
  "Execute the parsed query `q` against the database value `db`. Returns an async
  channel which will eventually contain a single vector of results, or an
  exception if there was an error."
  [db q]
  (go
   (let [error-ch  (async/chan)
         result-ch (->> (where/search db q error-ch)
                        (group q)
                        (order q)
                        (drop-offset q)
                        (take-limit q)
                        (select/format db q error-ch)
                        (collect-results q))]
     (async/alt!
       error-ch  ([e] e)
       result-ch ([result] result)))))
