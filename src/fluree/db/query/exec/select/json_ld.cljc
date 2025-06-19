(ns fluree.db.query.exec.select.json-ld
  (:require [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.where :as where]
            [fluree.db.validation :as v]))

(defn object
  "Returns the formatted object if it is properly bound, or nil. The object formatted
  'multicardinal' style in a vector, regardless of the number of values."
  [bnodes compact p o-match]
  (if (where/unmatched? o-match)
    (let [var (where/get-variable o-match)]
      ;; unbound non-bnode variable is an optional match, return nil
      (when (v/bnode-variable? var)
        [{(compact const/iri-id) (str var bnodes)}]))
    (if-let [iri (where/get-iri o-match)]
      ;; don't wrap @type values
      (if (= p const/iri-type)
        [(compact iri)]
        [{(compact const/iri-id) (compact iri)}])
      (let [v      (where/get-value o-match)
            dt-iri (where/get-datatype-iri o-match)
            lang   (where/get-lang o-match)]
        (if (datatype/inferable-iri? dt-iri)
          [v]
          [(cond-> {(compact const/iri-value) v}
             lang       (assoc (compact const/iri-language) lang)
             (not lang) (assoc (compact const/iri-type) (compact dt-iri)))])))))

(defn predicate
  "Returns the predicate iri if it is properly bound, or nil."
  [bnodes p-match]
  (if (where/unmatched? p-match)
    (let [var (where/get-variable p-match)]
      (when (v/bnode-variable? var)
        (str var bnodes)))
    (let [p (where/get-iri p-match)]
      (if (= p const/iri-rdf-type)
        const/iri-type
        p))))

(defn subject
  "Returns the subject iri if it is properly bound, or nil."
  [bnodes s-match]
  (if (where/unmatched? s-match)
    (let [var (where/get-variable s-match)]
      (when (v/bnode-variable? var)
        (str var bnodes)))
    (where/get-iri s-match)))

(defn format-node
  "Format a collection of subject matches into a json-ld object. If there is not at least
  one valid triple, return an empty object which will be removed."
  [compact bnodes s-matches]
  (let [node (reduce (fn [node [_ p-match o-match]]
                       (if node
                         (if-let [p (predicate bnodes p-match)]
                           (if-let [o (object bnodes compact p o-match)]
                             (assoc node (compact p) o)
                             node)
                           node)
                         ;; no bound subject, no valid triples
                         (reduced nil)))
                     (when-let [s (subject bnodes (ffirst s-matches))]
                       {(compact const/iri-id) (compact s)})
                     s-matches)]
    ;; a valid node needs at least two entries, one for the subject and one for a pred/obj
    (when (> (count node) 1)
      node)))

(defn nest-multicardinal-values
  "Aggregate unique values for the same predicate into a vector."
  [nodes]
  (apply merge-with #(if (and (sequential? %1)
                              (not= %1 %2))
                       (into %1 %2)
                       %2)
         nodes))
