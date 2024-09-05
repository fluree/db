(ns fluree.db.query.exec.select
  "Format and display solutions consisting of pattern matches found in where
  searches."
  (:refer-clojure :exclude [format])
  (:require [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.eval :as-alias eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.json-ld :as json-ld]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.json :as json]
            [fluree.db.json-ld.iri :as iri]))

#?(:clj (set! *warn-on-reflection* true))

(defmulti display
  "Format a where-pattern match for presentation based on the match's datatype.
  Return an async channel that will eventually contain the formatted match."
  (fn [match _compact]
    (where/get-datatype-iri match)))

(defmethod display :default
  [match _]
  (where/get-value match))

(defmethod display const/iri-rdf-json
  [match _compact]
  (-> match where/get-value (json/parse false)))

(defmethod display const/iri-id
  [match compact]
  (some-> match where/get-iri compact))

(defmethod display const/iri-vector
  [match _compact]
  (some-> match where/get-value vec))

(defprotocol ValueSelector
  (format-value [fmt db iri-cache context compact fuel-tracker error-ch solution]
    "Async format a search solution (map of pattern matches) by extracting relevant match."))

(defprotocol ValueAdapter
  (solution-value [fmt error-ch solution]
    "Formats value for subquery select statement as k-v tuple - synchronous."))

(defprotocol SolutionModifier
  (update-solution [this solution]))

(defrecord VariableSelector [var]
  ValueSelector
  (format-value
    [_ _db _iri-cache _context compact _fuel-tracker error-ch solution]
    (log/trace "VariableSelector format-value var:" var "solution:" solution)
    (go (try*
          (-> solution
              (get var)
              (display compact))
          (catch* e
                  (log/error e "Error formatting variable:" var)
                  (>! error-ch e)))))
  ValueAdapter
  (solution-value
    [_ _ solution]
    [var (get solution var)]))

(defn variable-selector
  "Returns a selector that extracts and formats a value bound to the specified
  `variable` in where search solutions for presentation."
  [variable]
  (->VariableSelector variable))

(defrecord WildcardSelector []
  ValueSelector
  (format-value
    [_ _db _iri-cache _context compact _fuel-tracker error-ch solution]
    (go
      (try*
        (loop [[var & vars] (sort (keys solution))
               result {}]
          (if var
            (let [display-var (-> solution (get var) (display compact))]
              (recur vars (assoc result var display-var)))
            result))
        (catch* e
                (log/error e "Error formatting wildcard")
                (>! error-ch e)))))
  ValueAdapter
  (solution-value
    [_ _ solution]
    solution))

(def wildcard-selector
  "Returns a selector that extracts and formats every bound value bound in the
  where clause."
  (->WildcardSelector))

(defrecord AggregateSelector [agg-fn]
  ValueSelector
  (format-value
    [_ _ _ _ _ _ error-ch solution]
    (go (try* (:value (agg-fn solution))
              (catch* e
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
    (let [result (:value (as-fn solution))
          dt     (datatype/infer-iri result)]
      (log/trace "AsSelector update-solution result:" result)
      (assoc solution bind-var (-> bind-var
                                   where/unmatched-var
                                   (where/match-value result dt)))))
  ValueSelector
  (format-value
    [_ _ _ _ _ _ _ solution]
    (log/trace "AsSelector format-value solution:" solution)
    (go (let [match (get solution bind-var)]
          (where/get-value match))))
  ValueAdapter
  (solution-value
    [_ _ solution]
    [bind-var (get solution bind-var)]))

(defn as-selector
  [as-fn bind-var aggregate?]
  (->AsSelector as-fn bind-var aggregate?))

(defrecord SubgraphSelector [subj selection depth spec]
  ValueSelector
  (format-value
    [_ ds iri-cache context compact fuel-tracker error-ch solution]
    (when-let [iri (if (where/variable? subj)
                     (-> solution
                         (get subj)
                         where/get-iri)
                     subj)]
      (subject/format-subject ds iri context compact spec iri-cache
                              fuel-tracker error-ch))))

(defn subgraph-selector
  "Returns a selector that extracts the subject id bound to the supplied
  `variable` within a where solution and extracts the subgraph containing
  attributes and values associated with that subject specified by `selection`
  from a database value."
  [subj selection depth spec]
  (->SubgraphSelector subj selection depth spec))

(defn modify
  "Apply any modifying selectors to each solution in `solution-ch`."
  [q solution-ch]
  (let [selectors           (or (:select q)
                                (:select-one q)
                                (:select-distinct q))
        modifying-selectors (filter #(satisfies? SolutionModifier %) selectors)
        mods-xf             (map (fn [solution]
                                   (reduce
                                     (fn [sol sel]
                                       (log/trace "Updating solution:" sol)
                                       (update-solution sel sol))
                                     solution modifying-selectors)))
        modify-ch               (chan 1 mods-xf)]
    (async/pipe solution-ch modify-ch)))

(defn format-values
  "Formats the values from the specified where search solution `solution`
  according to the selector or collection of selectors specified by `selectors`"
  [selectors db iri-cache context compact fuel-tracker error-ch solution]
  (if (sequential? selectors)
    (go-loop [selectors selectors
              values []]
      (if-let [selector (first selectors)]
        (let [value (<! (format-value selector db iri-cache context compact
                                      fuel-tracker error-ch solution))]
          (recur (rest selectors)
                 (conj values value)))
        values))
    (format-value selectors db iri-cache context compact fuel-tracker
                  error-ch solution)))

(defn format
  "Formats each solution within the stream of solutions in `solution-ch` according
  to the selectors within the select clause of the supplied parsed query `q`."
  [db q fuel-tracker error-ch solution-ch]
  (let [context             (:context q)
        compact             (json-ld/compact-fn context)
        selectors           (or (:select q)
                                (:select-one q)
                                (:select-distinct q))
        iri-cache           (volatile! {})
        format-ch           (if (contains? q :select-distinct)
                              (chan 1 (distinct))
                              (chan))]
    (async/pipeline-async 3
                          format-ch
                          (fn [solution ch]
                            (log/trace "select/format solution:" solution)
                            (-> (format-values selectors db iri-cache context compact
                                               fuel-tracker error-ch solution)
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
  [_db q _fuel-tracker error-ch solution-ch]
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
