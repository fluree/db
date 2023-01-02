(ns fluree.db.query.exec
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

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

(defn group
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

(defmethod select/display ::grouping
  [match db select-cache compact error-ch]
  (let [group (::where/val match)]
    (->> group
         (map (fn [grouped-val]
                (select/display grouped-val db select-cache compact error-ch)))
         (async/map vector))))

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
  [ordering solution-ch]
  (if ordering
    (let [comparator (partial compare-solutions ordering)
          coll-ch    (async/into [] solution-ch)
          ordered-ch (async/chan 2 (comp (map (partial sort comparator))
                                         cat))]
      (async/pipe coll-ch ordered-ch))
    solution-ch))

(defn offset
  [n solution-ch]
  (if n
    (async/pipe solution-ch
                (async/chan 2 (drop n)))
    solution-ch))

(defn limit
  [n solution-ch]
  (if n
    (async/take n solution-ch)
    solution-ch))

(defn collect-results
  [q result-ch]
  (if (:selectOne q)
    (async/take 1 result-ch)
    (async/into [] result-ch)))

(defn execute
  [db q]
  (let [error-ch (async/chan)]
    (->> (where/search db q error-ch)
         (group q)
         (order (:order-by q))
         (offset (:offset q))
         (limit (:limit q))
         (select/format db q error-ch)
         (collect-results q))))
