(ns fluree.db.query.exec.select
  "Format and display solutions consisting of pattern matches found in where
  searches."
  (:refer-clojure :exclude [format])
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(defmulti display
  "Format a where-pattern match for presentation based on the match's datatype.
  Return an async channel that will eventually contain the formatted match."
  (fn [match db iri-cache compact error-ch]
    (::where/datatype match)))

(defmethod display :default
  [match _ _ _ _]
  (go (::where/val match)))

(defmethod display const/$xsd:anyURI
  [match db iri-cache compact error-ch]
  (go
    (let [v (::where/val match)]
      (if-let [cached (get @iri-cache v)]
        cached
        (try* (let [iri (<? (dbproto/-iri db v compact))]
                (vswap! iri-cache assoc v iri)
                iri)
              (catch* e
                      (log/error e "Error displaying iri:" v)
                      (>! error-ch e)))))))

(defprotocol ValueSelector
  "Format a where search solution (map of pattern matches) by extracting and
  displaying relevant pattern matches."
  (format-value [fmt db iri-cache compact error-ch solution]))

(defrecord VariableSelector [var]
  ValueSelector
  (format-value
    [_ db iri-cache compact error-ch solution]
    (-> solution
        (get var)
        (display db iri-cache compact error-ch))))

(defn variable-selector
  "Returns a selector that extracts and formats a value bound to the specified
  `variable` in where search solutions for presentation."
  [variable]
  (->VariableSelector variable))

(defrecord AggregateSelector [agg-fn]
  ValueSelector
  (format-value
    [_ db iri-cache compact error-ch solution]
    (go (try* (agg-fn solution)
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

(defrecord SubgraphSelector [var selection depth spec]
  ValueSelector
  (format-value
    [_ db iri-cache compact error-ch solution]
    (go
      (let [sid (-> solution
                    (get var)
                    ::where/val)]
        (try*
         (let [flakes (<? (query-range/index-range db :spot = [sid]))]
           ;; TODO: Replace these nils with fuel values when we turn fuel back on
           (<? (json-ld-resp/flakes->res db iri-cache compact nil nil spec 0 flakes)))
         (catch* e
                 (log/error e "Error formatting subgraph for subject:" sid)
                 (>! error-ch e)))))))

(defn subgraph-selector
  "Returns a selector that extracts the subject id bound to the supplied
  `variable` within a where solution and extracts the subgraph containing
  attributes and values associated with that subject specified by `selection`
  from a database value."
  [variable selection depth spec]
  (->SubgraphSelector variable selection depth spec))

(defn format-values
  "Formats the values from the specified where search solution `solution`
  according to the selector or collection of selectors specified by `selectors`"
  [selectors db iri-cache compact error-ch solution]
  (if (sequential? selectors)
    (go-loop [selectors  selectors
              values     []]
      (if-let [selector (first selectors)]
        (let [value (<! (format-value selector db iri-cache compact error-ch solution))]
          (recur (rest selectors)
                 (conj values value)))
        values))
    (format-value selectors db iri-cache compact error-ch solution)))

(defn format
  "Formats each solution within the stream of solutions in `solution-ch` according
  to the selectors within the select clause of the supplied parsed query `q`."
  [db q error-ch solution-ch]
  (let [compact   (->> q :context json-ld/compact-fn)
        selectors (or (:select q)
                      (:select-one q))
        iri-cache (volatile! {})
        format-ch (chan)]
    (async/pipeline-async 1
                          format-ch
                          (fn [solution ch]
                            (-> (format-values selectors db iri-cache compact error-ch solution)
                                (async/pipe ch)))
                          solution-ch)
    format-ch))

(defn implicit-grouping?
  "Returns true if the provide `selector` can only operate on grouped elements."
  [selector]
  (instance? AggregateSelector selector))
