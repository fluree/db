(ns fluree.db.query.exec.group
  (:require [clojure.core.async :as async]
            [fluree.db.query.exec.aggregate :as agg]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.select.fql :as select.fql]
            [fluree.db.query.exec.select.sparql :as select.sparql]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util])
  (:import [fluree.db.query.exec.select AsSelector StreamingAggregateSelector VariableSelector]))

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

(defn extract-group-key
  "Extracts the group key from a solution."
  [variables solution]
  (mapv (fn [v]
          (-> solution
              (get v)
              where/sanitize-match))
        variables))

(defn split-solution-by
  [variables variable-set solution]
  (let [group-key   (extract-group-key variables solution)
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

(defn- streaming-agg-selector?
  "Returns true if selector supports streaming aggregation."
  [sel]
  (or (instance? StreamingAggregateSelector sel)
      (and (instance? AsSelector sel)
           (some? (:streaming-agg sel)))))

(defn- streaming-eligible?
  "Returns true if the query can use streaming aggregation.
  Streaming is possible when:
  - No HAVING clause (requires all solutions to evaluate)
  - Has streaming aggregates
  - Either implicit grouping with all aggregate selectors,
    or explicit group-by with all selectors being group vars or aggregates"
  [having streaming-aggs implicit? selectors group-vars group-vars-set]
  (and (nil? having)
       (seq streaming-aggs)
       (if implicit?
         (every? streaming-agg-selector? selectors)
         (and (seq group-vars)
              (every? (fn [sel]
                        (or (and (instance? VariableSelector sel)
                                 (contains? group-vars-set (:var sel)))
                            (streaming-agg-selector? sel)))
                      selectors)))))

(defn- update-streaming-groups
  "Updates streaming aggregate states for a solution.
  Returns updated groups map with accumulated aggregate states per group key."
  [group-vars streaming-aggs groups solution]
  (let [group-key (extract-group-key group-vars solution)
        {:keys [group-vars-map agg-states]}
        (get groups group-key
             {:group-vars-map (zipmap group-vars group-key)
              :agg-states     {}})
        agg-states'
        (reduce (fn [states {:keys [arg-var result-var aggregator]}]
                  (let [agg    (get states result-var (aggregator))
                        tv     (when arg-var
                                 (some-> solution
                                         (get arg-var)
                                         where/mch->typed-val))
                        agg'   (agg/step agg tv)]
                    (assoc states result-var agg')))
                agg-states
                streaming-aggs)]
    (assoc groups group-key {:group-vars-map group-vars-map
                             :agg-states     agg-states'})))

(defn- finalize-streaming-groups
  "Completion function that converts accumulated aggregate states into final solutions.
  Called once after all solutions have been processed."
  [streaming-aggs groups]
  (reduce-kv
   (fn [solutions _group-key {:keys [group-vars-map agg-states]}]
     (let [solution-with-aggs
           (reduce (fn [sol {:keys [result-var]}]
                     (let [agg      (get agg-states result-var)
                           tv       (agg/complete agg)
                           base-mch (where/unmatched-var result-var)
                           mch      (where/typed-val->mch base-mch tv)]
                       (assoc sol result-var mch)))
                   group-vars-map
                   streaming-aggs)]
       (conj solutions solution-with-aggs)))
   []
   groups))

(defn combine
  "Returns a channel of solutions from `solution-ch` collected into groups defined
  by the `:group-by` clause specified in the supplied query."
  [{:keys [group-by select having]} solution-ch]
  (let [selectors      (util/sequential select)
        group-vars     (vec group-by)
        group-vars-set (set group-vars)
        streaming-aggs (->> selectors
                            (filter streaming-agg-selector?)
                            (mapv :streaming-agg))
        implicit?      (and (empty? group-vars)
                            (some select/implicit-grouping? selectors))]
    (if (streaming-eligible? having streaming-aggs implicit?
                             selectors group-vars group-vars-set)
      (-> (async/transduce (map identity)
                           (completing (partial update-streaming-groups group-vars streaming-aggs)
                                       (partial finalize-streaming-groups streaming-aggs))
                           {}
                           solution-ch)
          (async/pipe (async/chan 2 cat)))

      (if-let [grouping (or group-by
                            (implicit-grouping select))]
        (let [grouping-set (set grouping)]
          (-> (async/transduce (map (partial split-solution-by grouping grouping-set))
                               (completing group-solution
                                           (partial unwind-groups grouping))
                               {}
                               solution-ch)
              (async/pipe (async/chan 2 cat))))
        solution-ch))))
