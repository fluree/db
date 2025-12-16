(ns fluree.db.query.exec.group
  (:require [clojure.core.async :as async]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.select.fql :as select.fql]
            [fluree.db.query.exec.select.sparql :as select.sparql]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]))

#?(:clj (set! *warn-on-reflection* true))

(defn- dissoc-many
  "Like `dissoc`, but accepts a set of keys and is implemented via a single
  pass over `m` using transients to reduce allocations. Returns a persistent
  map."
  [m ks-set]
  (if (or (nil? m) (empty? ks-set))
    m
    (persistent!
     (reduce-kv (fn [m* k v]
                  (if (contains? ks-set k)
                    m*
                    (assoc! m* k v)))
                (transient {})
                m))))

(defn split-solution-by
  [variables variable-set solution]
  (let [group-key   (mapv (fn [v]
                            (-> solution
                                (get v)
                                where/sanitize-match))
                          variables)
        grouped-val (dissoc-many solution variable-set)]
    [group-key grouped-val]))

(defn assoc-coll
  [m k v]
  (update m k (fnil conj []) v))

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
  (persistent!
   (reduce-kv
    (fn [solutions group-key grouped-vals]
      (let [merged-vals (reduce merge-with-colls {} grouped-vals)
            merged-vals* (persistent!
                          (reduce-kv
                           (fn [soln var val]
                             (let [match (-> var
                                             where/unmatched-var
                                             (where/match-value val ::grouping))]
                               (assoc! soln var match)))
                           (transient merged-vals)
                           merged-vals))
            solution     (persistent!
                          (reduce (fn [soln [var val]]
                                    (assoc! soln var val))
                                  (transient merged-vals*)
                                  (map vector grouping group-key)))]
        (conj! solutions solution)))
    (transient [])
    groups)))

(defn implicit-grouping
  [select]
  (when (some select/implicit-grouping? (util/sequential select))
    [nil]))

(defn display-aggregate
  [display-fn]
  (fn [match compact]
    (let [group (where/get-value match)]
      (mapv (fn [grouped-val] (display-fn grouped-val compact))
            group))))

(def display-fql-aggregate (display-aggregate select.fql/display))
(def display-sparql-aggregate (display-aggregate select.sparql/display))

(defmethod select.fql/display ::grouping
  [match compact]
  (display-fql-aggregate match compact))

(defmethod select.sparql/display ::grouping
  [match compact]
  (display-sparql-aggregate match compact))

(defn combine
  "Returns a channel of solutions from `solution-ch` collected into groups defined
  by the `:group-by` clause specified in the supplied query."
  [{:keys [group-by select]} solution-ch]
  (if-let [grouping (or group-by
                        (implicit-grouping select))]
    (let [grouping-set (set grouping)]
      (-> (async/transduce (map (partial split-solution-by grouping grouping-set))
                           (completing group-solution
                                       (partial unwind-groups grouping))
                           {}
                           solution-ch)
          (async/pipe (async/chan 2 cat))))
    solution-ch))
