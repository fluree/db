(ns fluree.db.query.exec.select
  "Format and display solutions consisting of pattern matches found in where
  searches."
  (:refer-clojure :exclude [format])
  (:require [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.eval :as-alias eval]
            [fluree.db.query.exec.select.fql :as select.fql]
            [fluree.db.query.exec.select.json-ld :as select.json-ld]
            [fluree.db.query.exec.select.literal :as literal]
            [fluree.db.query.exec.select.sparql :as select.sparql]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util :refer [catch* try*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.trace :as trace]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol ValueSelector
  :extend-via-metadata true
  (format-value [fmt db iri-cache context compact tracker error-ch solution]
    "Async format a search solution (map of pattern matches) by extracting relevant match."))

(defprotocol ValueAdapter
  (solution-value [fmt error-ch solution]
    "Formats value for subquery select statement as k-v tuple - synchronous."))

(defprotocol SolutionModifier
  (update-solution [this solution]))

(defrecord VariableSelector [var]
  ValueAdapter
  (solution-value
    [_ _ solution]
    [var (get solution var)]))

(defn variable-selector
  "Returns a selector that extracts and formats a value bound to the specified
  `variable` in where search solutions for presentation."
  [variable output]
  (let [selector (->VariableSelector variable)]
    (case output
      :sparql (with-meta selector {`format-value (select.sparql/format-variable-selector-value variable)})
      (with-meta selector {`format-value (select.fql/format-variable-selector-value variable)}))))

(defrecord WildcardSelector []
  ValueAdapter
  (solution-value
    [_ _ solution]
    solution))

(defn wildcard-selector
  "Returns a selector that extracts and formats every bound value bound in the
  where clause."
  [output]
  (let [selector (->WildcardSelector)]
    (case output
      :sparql (with-meta selector {`format-value select.sparql/format-wildcard-selector-value})
      (with-meta selector {`format-value select.fql/format-wildcard-selector-value}))))

(defrecord AggregateSelector [agg-fn]
  ValueSelector
  (format-value
    [_ _ _ _ _ _ error-ch solution]
    (go (try* (:value (agg-fn solution))
              (catch* e
                (log/error! ::aggregate-formatting-error e {:msg "Error applying aggregate selector"
                                                            :solution solution-value
                                                            :agg-fn (meta agg-fn)})
                (log/error e "Error applying aggregate selector")
                (>! error-ch e))))))

(defn aggregate-selector
  "Returns a selector that extracts the grouped values bound to the specified
  variables referenced in the supplied `agg-function` from a where solution,
  formats each item in the group, and processes the formatted group with the
  supplied `agg-function` to generate the final aggregated result for display."
  [agg-function]
  (->AggregateSelector agg-function))

(defrecord AsSelector [as-fn bind-var aggregate?]
  SolutionModifier
  (update-solution
    [_ solution]
    (log/trace "AsSelector update-solution solution:" solution)
    (let [{v :value dt :datatype-iri lang :lang} (as-fn solution)]
      (log/trace "AsSelector update-solution result:" v)
      (assoc solution bind-var (-> (where/unmatched-var bind-var)
                                   (where/match-value v (or dt (datatype/infer-iri v)))
                                   (cond-> lang (where/match-lang v lang))))))
  ValueAdapter
  (solution-value
    [_ _ solution]
    [bind-var (get solution bind-var)]))

(defn as-selector
  [as-fn output bind-var aggregate?]
  (let [selector (->AsSelector as-fn bind-var aggregate?)]
    (case output
      :sparql (with-meta selector {`format-value (select.sparql/format-as-selector-value bind-var)})
      (with-meta selector {`format-value (select.fql/format-as-selector-value bind-var)}))))

(defn get-subject-iri
  [solution subj]
  (if (where/variable? subj)
    (-> solution
        (get subj)
        where/get-iri)
    subj))

(defrecord SubgraphSelector [subj selection depth spec]
  ValueSelector
  (format-value
    [_ ds iri-cache context compact tracker error-ch solution]
    (if-let [iri (get-subject-iri solution subj)]
      (subject/format-subject ds iri context compact spec iri-cache
                              tracker error-ch)
      (go
        (let [match    (get solution subj)
              value    (where/get-value match)
              datatype (where/get-datatype-iri match)
              language (where/get-lang match)]
          (literal/format-literal value datatype language compact spec
                                  iri-cache))))))

(defn subgraph-selector
  "Returns a selector that extracts the subject id bound to the supplied
  `variable` within a where solution and extracts the subgraph containing
  attributes and values associated with that subject specified by `selection`
  from a database value."
  [subj selection depth spec]
  (->SubgraphSelector subj selection depth spec))

(defrecord ConstructSelector [patterns bnodes]
  ValueSelector
  (format-value [_ _ _ _ compact _ _ solution]
    (let [bnodes (swap! bnodes inc)]
      (go (->> (mapv #(where/assign-matched-values % solution) patterns)
               (partition-by first) ; partition by s-match
               (keep (partial select.json-ld/format-node compact bnodes)))))))

(defn construct-selector
  [patterns]
  (->ConstructSelector patterns (atom 0)))

(defn modify
  "Apply any modifying selectors to each solution in `solution-ch`."
  [q solution-ch]
  (let [selectors           (or (:select q)
                                (:select-one q)
                                (:select-distinct q))
        modifying-selectors (filter #(satisfies? SolutionModifier %) (util/sequential selectors))
        mods-xf             (comp
                             (trace/xf ::query-projection-modification {:solution-modifiers modifying-selectors})
                             (map (fn [solution]
                                    (reduce
                                     (fn [sol sel]
                                       (log/trace "Updating solution:" sol)
                                       (update-solution sel sol))
                                     solution modifying-selectors))))
        modify-ch               (chan 1 mods-xf)]
    (async/pipe solution-ch modify-ch)))

(defn format-values
  "Formats the values from the specified where search solution `solution`
  according to the selector or collection of selectors specified by `selectors`"
  [selectors db iri-cache context output-format compact tracker error-ch solution]
  (if (sequential? selectors)
    (go-loop [selectors selectors
              values []]
      (if-let [selector (first selectors)]
        (let [value (<! (format-value selector db iri-cache context compact
                                      tracker error-ch solution))]
          (recur (rest selectors)
                 (conj values value)))

        (if (= output-format :sparql)
          (apply merge values)
          values)))
    (format-value selectors db iri-cache context compact tracker
                  error-ch solution)))

(defn format
  "Formats each solution within the stream of solutions in `solution-ch` according
  to the selectors within the select clause of the supplied parsed query `q`."
  [db q tracker error-ch solution-ch]
  (let [context             (or (:selection-context q)
                                (:context q))
        compact             (json-ld/compact-fn context)
        output-format       (:output (:opts q))
        selectors           (or (:construct q)
                                (:select q)
                                (:select-one q)
                                (:select-distinct q))
        iri-cache           (volatile! {})
        format-xf           (some->> [(trace/xf ::query-format {:selectors selectors})
                                      (when (contains? q :select-distinct) (distinct))
                                      (when (contains? q :construct) cat)
                                      (when (= output-format :sparql) (mapcat select.sparql/disaggregate))]
                                     (remove nil?)
                                     (not-empty)
                                     (apply comp))
        format-ch           (if format-xf
                              (chan 1 format-xf)
                              (chan))]
    (async/pipeline-async 3
                          format-ch
                          (fn [solution ch]
                            (log/trace "select/format solution:" solution)
                            (-> (format-values selectors db iri-cache context output-format compact
                                               tracker error-ch solution)
                                (async/pipe ch)))
                          solution-ch)
    format-ch))

(defn format-subquery-values
  [selectors error-ch solution]
  (reduce
   (fn [acc selector]
     (if-let [soln-val (solution-value selector error-ch solution)]
       (if (map? soln-val)
         (reduced soln-val) ;; wilcard selector returns map of all solutions, can stop.
         (assoc acc (first soln-val) (second soln-val)))
       acc))
   {}
   (util/sequential selectors)))

(defn subquery-format
  "Formats each solution within the stream of solutions in `solution-ch` according
  to the selectors within the select clause of the supplied parsed query `q`."
  [_db q _tracker error-ch solution-ch]
  (let [selectors (or (:select q)
                      (:select-one q)
                      (:select-distinct q))
        format-ch (if (contains? q :select-distinct)
                    (chan 1 (comp
                             (map (partial format-subquery-values selectors error-ch))
                             (distinct)))
                    (chan 1 (map (partial format-subquery-values selectors error-ch))))]
    (async/pipe solution-ch format-ch)
    format-ch))

(defn implicit-grouping?
  [selector]
  (or (instance? AggregateSelector selector)
      (and (instance? AsSelector selector)
           (:aggregate? selector))))

(defn wrap-construct
  [{:keys [orig-context context]} results]
  (let [id-key (json-ld/compact const/iri-id context)]
    (cond-> {"@graph" (->> results
                           (sort-by #(get % id-key))
                           (partition-by #(get % id-key))
                           (mapv select.json-ld/nest-multicardinal-values))}
      orig-context (assoc "@context" orig-context))))

(defn wrap-sparql
  [results]
  {"head" {"vars" (vec (sort (keys (first results))))}
   "results" {"bindings" results}})
