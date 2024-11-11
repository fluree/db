(ns fluree.db.query.group
  (:require [clojure.core.async :as async]
            [fluree.db.query.select :as select]
            [fluree.db.query.where :as where]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn split-solution-by
  [variables solution]
  (let [group-key   (mapv (fn [v]
                            (-> solution
                                (get v)
                                where/sanitize-match))
                          variables)
        grouped-val (apply dissoc solution variables)]
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
  (reduce-kv (fn [solutions group-key grouped-vals]
               (let [merged-vals (->> grouped-vals
                                      (reduce merge-with-colls {})
                                      (reduce-kv (fn [soln var val]
                                                   (let [match (-> var
                                                                   where/unmatched-var
                                                                   (where/match-value val ::grouping))]
                                                     (assoc soln var match)))
                                                 {}))
                     solution    (into merged-vals
                                       (map vector grouping group-key))]
                 (conj solutions solution)))
             [] groups))

(defn implicit-grouping
  [select]
  (when (some select/implicit-grouping? (util/sequential select))
    [nil]))

(defmethod select/display ::grouping
  [match compact]
  (let [group (where/get-value match)]
    (mapv (fn [grouped-val]
            (select/display grouped-val compact))
          group)))

(defn combine
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
