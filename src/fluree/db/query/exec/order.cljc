(ns fluree.db.query.exec.order
  (:require [clojure.core.async :as async]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.trace :as trace]))

#?(:clj (set! *warn-on-reflection* true))

(defn compare-vals
  [x-val x-dt y-val y-dt]
  (let [dt-cmp (compare x-dt y-dt)]
    (if (zero? dt-cmp)
      (compare x-val y-val)
      dt-cmp)))

(defn compare-solutions-by
  [variable direction x y]
  (let [x-var (get x variable)
        x-val (where/get-binding x-var)
        x-dt  (where/get-datatype-iri x-var)

        y-var (get y variable)
        y-val (where/get-binding y-var)
        y-dt  (where/get-datatype-iri y-var)]
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

(defn arrange
  "Returns a channel containing all solutions from `solution-ch` sorted by the
  ordering specified by the `:order-by` clause of the supplied parsed query.
  Note that all solutions from `solution-ch` are first loaded into memory before
  they are sorted in place and placed individually on the output channel."
  [{:keys [order-by]} solution-ch]
  (if order-by
    (let [comparator (partial compare-solutions order-by)
          coll-ch    (async/into [] solution-ch)
          ordered-ch (async/chan 2 (comp (trace/xf ::query-order {:order-by order-by})
                                         (map (partial sort comparator))
                                         cat))]
      (async/pipe coll-ch ordered-ch))
    solution-ch))
