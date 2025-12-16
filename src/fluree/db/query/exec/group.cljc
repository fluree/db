(ns fluree.db.query.exec.group
  (:require [clojure.core.async :as async]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.select.fql :as select.fql]
            [fluree.db.query.exec.select.sparql :as select.sparql]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]
            [fluree.db.util.log :as log :include-macros true]))

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
  "Extracts just the group key from a solution without building grouped-val.
  Used by streaming mode to avoid the dissoc-many overhead per row."
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

(defn- get-streaming-agg
  "Extracts streaming-agg from a selector. Checks record field for AggregateSelector
  and AsSelector, returns nil for other selector types."
  [sel]
  (cond
    (instance? fluree.db.query.exec.select.AggregateSelector sel)
    (:streaming-agg sel)

    (instance? fluree.db.query.exec.select.AsSelector sel)
    (:streaming-agg sel)

    :else
    nil))

(defn combine
  "Returns a channel of solutions from `solution-ch` collected into groups defined
  by the `:group-by` clause specified in the supplied query."
  [{:keys [group-by select having]} solution-ch]
  (let [selectors      (util/sequential select)
        group-vars     (vec group-by)
        group-vars-set (set group-vars)
        ;; Extract streaming-agg from record fields
        streaming-aggs (->> selectors
                            (keep get-streaming-agg)
                            vec)
        ;; Check if this is implicit grouping (global aggregate without GROUP BY)
        implicit?      (and (empty? group-vars)
                            (some select/implicit-grouping? selectors))
        ;; Streaming mode is enabled when:
        ;;  - there is no HAVING clause (HAVING needs grouped collections), and
        ;;  - there are streaming-agg descriptors, and
        ;;  - either:
        ;;    a) explicit GROUP BY with all selectors being group vars or streaming aggs, or
        ;;    b) implicit grouping with all selectors being streaming aggs
        streaming?     (and (nil? having)
                            (seq streaming-aggs)
                            (if implicit?
                              ;; Implicit grouping: all selectors must be streaming aggs
                              (every? (fn [sel]
                                        (or (and (instance? fluree.db.query.exec.select.AggregateSelector sel)
                                                 (some? (:streaming-agg sel)))
                                            (and (instance? fluree.db.query.exec.select.AsSelector sel)
                                                 (some? (:streaming-agg sel)))))
                                      selectors)
                              ;; Explicit GROUP BY: need group vars and matching selectors
                              (and (seq group-vars)
                                   (every? (fn [sel]
                                             (cond
                                               (instance? fluree.db.query.exec.select.VariableSelector sel)
                                               (contains? group-vars-set (:var sel))

                                               (instance? fluree.db.query.exec.select.AggregateSelector sel)
                                               (some? (:streaming-agg sel))

                                               (instance? fluree.db.query.exec.select.AsSelector sel)
                                               (some? (:streaming-agg sel))

                                               :else
                                               false))
                                           selectors))))]
    (log/debug "group/combine streaming?" streaming?
                "implicit?" implicit?
                "group-vars" group-vars
                "streaming-aggs" (count streaming-aggs))
    (if streaming?
      ;; New streaming mode: maintain per-group aggregate state and
      ;; avoid collecting full grouped value collections.
      (let [update-groups
            (fn [groups solution]
              ;; Use extract-group-key to avoid dissoc-many overhead per row
              (let [group-key (extract-group-key group-vars solution)
                    {:keys [group-vars-map agg-states]}
                    (get groups group-key
                         {:group-vars-map (zipmap group-vars group-key)
                          :agg-states     {}})
                    agg-states'
                    (reduce (fn [states {:keys [arg-var result-var descriptor]}]
                              (let [{:keys [init step!]} descriptor
                                    state   (get states result-var (init))
                                    tv      (when arg-var
                                              (some-> solution
                                                      (get arg-var)
                                                      where/mch->typed-val))
                                    new-st  (step! state tv)]
                                (assoc states result-var new-st)))
                            agg-states
                            streaming-aggs)]
                (assoc groups group-key {:group-vars-map group-vars-map
                                         :agg-states     agg-states'})))

            finalize-groups
            (fn [groups]
              (reduce-kv
               (fn [solutions _group-key {:keys [group-vars-map agg-states]}]
                 (let [solution-with-aggs
                       (reduce (fn [sol {:keys [result-var descriptor]}]
                                 (let [state   (get agg-states result-var)
                                       tv      ((:final descriptor) state)
                                       base-mch (where/unmatched-var result-var)
                                       mch     (where/typed-val->mch base-mch tv)]
                                   (assoc sol result-var mch)))
                               group-vars-map
                               streaming-aggs)]
                   (conj solutions solution-with-aggs)))
               []
               groups))]
        (-> (async/transduce (map identity)
                             (completing update-groups finalize-groups)
                             {}
                             solution-ch)
            (async/pipe (async/chan 2 cat))))

      ;; Legacy mode: collect grouped values and wrap them in ::grouping
      ;; matches for collection-based aggregate evaluation.
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
