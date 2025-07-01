(ns fluree.db.query.exec.select.sparql
  (:require [clojure.core.async :as async :refer [go >!]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.log :as log]))

(defn var-name
  "Stringify and remove q-mark prefix of var for SPARQL JSON formatting."
  [var]
  (subs (name var) 1))

(defmulti display
  (fn [match _compact]
    (where/get-datatype-iri match)))

(defmethod display :default
  [match _compact]
  (let [v    (where/get-value match)
        dt   (where/get-datatype-iri match)
        lang (where/get-lang match)]
    (cond-> {"value" (str v) "type" "literal"}
      (and v lang)                                                 (assoc "xml:lang" lang)
      (and v (not (#{const/iri-string const/iri-lang-string} dt))) (assoc "datatype" dt))))

(defmethod display "@json"
  [match _compact]
  {"value" (where/get-value match) "type" "literal" "datatype" "@json"})

(defmethod display const/iri-id
  [match _compact]
  (let [iri (where/get-iri match)]
    (if (= \_ (first iri))
      {"type" "bnode" "value" (subs iri 1)}
      {"type" "uri" "value" iri})))

(defmethod display const/iri-vector
  [match _compact]
  {"type" "literal" "value" (some-> match where/get-value vec str) "datatype" const/iri-vector})

(defn disaggregate
  "For SPARQL JSON results, no nesting of data is permitted - the results must be
  tabular. This function unpacks a single result into potentially multiple 'rows' of
  results."
  [result]
  (let [aggregated (filter (fn [[_k v]]
                             (sequential? v))
                           result)]
    (loop [[[agg-var agg-vals] & r] aggregated
           results [result]]
      (if agg-var
        (let [results* (reduce (fn [results* result]
                                 (into results* (map (fn [v] (assoc result agg-var v)) agg-vals)))
                               []
                               results)]
          (recur r results*))
        results))))

(defn format-variable-selector-value
  [var]
  (fn [_ _db _iri-cache _context compact _tracker error-ch solution]
    (go (try* {(var-name var) (-> solution (get var) (display compact))}
              (catch* e
                (log/error e "Error formatting variable:" var)
                (>! error-ch e))))))

(defn format-wildcard-selector-value
  [_ _db _iri-cache _context compact _tracker error-ch solution]
  (go
    (try*
      (loop [[var & vars] (sort (remove nil? (keys solution))) ; implicit grouping can introduce nil keys in solution
             result {}]
        (if var
          (let [output (-> solution (get var) (display compact))]
            (recur vars (assoc result (var-name var) output)))
          result))
      (catch* e
        (log/error e "Error formatting wildcard")
        (>! error-ch e)))))

(defn format-as-selector-value
  [bind-var]
  (fn [_ _ _ _ compact _ _ solution]
    (go (let [output (-> solution (get bind-var) (display compact))]
          {(var-name bind-var) output}))))
