(ns fluree.db.query.exec.select.display
  (:require [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.json :as json]))

(defmulti fql (fn [match compact] (where/get-datatype-iri match)))
(defmethod fql :default
  [match compact]
  (where/get-value match))

(defmethod fql const/iri-rdf-json
  [match compact]
  (-> match where/get-value (json/parse false)))

(defmethod fql const/iri-id
  [match compact]
  (some-> match where/get-iri compact))

(defmethod fql const/iri-vector
  [match compact]
  (some-> match where/get-value vec))

(defn var-name
  "Stringify and remove q-mark prefix of var for SPARQL JSON formatting."
  [var]
  (subs (name var) 1))

(defmulti sparql (fn [match compact] (where/get-datatype-iri match)))
(defmethod sparql :default
  [match compact]
  (let [v  (where/get-value match)
        dt (where/get-datatype-iri match)]
    (cond-> {"value" (str v) "type" "literal"}
      (and v (not= const/iri-string dt)) (assoc "datatype" dt))))

(defmethod sparql const/iri-rdf-json
  [match compact]
  {"value" (where/get-value match) "type" "literal" "datatype" const/iri-rdf-json})

(defmethod sparql const/iri-id
  [match compact]
  (let [iri (where/get-iri match)]
    (if (= \_ (first iri))
      {"type" "bnode" "value" (subs iri 1)}
      {"type" "uri" "value" iri})))

(defmethod sparql const/iri-vector
  [match compact]
  {"type" "literal" "value" (some-> match where/get-value vec str) "datatype" const/iri-vector})

(defn disaggregate
  "For SPARQL JSON results, no nesting of data is permitted - the results must be
  tabular. This function unpacks a single result into potentially multiple 'rows' of
  results."
  [result]
  (let [aggregated (filter (fn [[k v]] (sequential? v)) result)]
    (loop [[[agg-var agg-vals] & r] aggregated
           results [result]]
      (if agg-var
        (let [results* (reduce (fn [results* result]
                                 (into results* (map (fn [v] (assoc result agg-var v)) agg-vals)))
                               []
                               results)]
          (recur r results*))
        results))))
