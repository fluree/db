(ns fluree.db.query.exec.select.json-ld
  (:require [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.where :as where]
            [fluree.db.validation :as v]))

(defn json-ld-object
  [compact bnodes p o-match]
  [(if (where/unmatched? o-match)
     (let [var (where/get-variable o-match)]
       ;; unbound non-bnode variable is an optional match.
       (when (v/bnode-variable? var)
         {(compact const/iri-id) (str var bnodes)}))
     (if-let [iri (where/get-iri o-match)]
       ;; don't wrap @type values
       (if (= p const/iri-type)
         (compact iri)
         {(compact const/iri-id) (compact iri)})
       (let [v      (where/get-value o-match)
             dt-iri (where/get-datatype-iri o-match)
             lang   (where/get-lang o-match)]
         (if (datatype/inferable-iri? dt-iri)
           v
           (cond-> {(compact const/iri-value) v}
             lang       (assoc (compact const/iri-language) lang)
             (not lang) (assoc (compact const/iri-type) (compact dt-iri)))))))])

(defn json-ld-predicate
  [p-match]
  (let [p (where/get-iri p-match)]
    (if (= p const/iri-rdf-type)
      const/iri-type
      p)))

(defn json-ld-subject
  [compact bnodes s-match]
  (if (where/get-iri s-match)
    {(compact const/iri-id) (compact (where/get-iri s-match))}
    (let [var (where/get-variable s-match)]
      (if (v/bnode-variable? var)
        {(compact const/iri-id) (str var bnodes)}
        ;; unbound non-bnode variable is an optional match.
        {(compact const/iri-id) nil}))))

(defn json-ld-node
  [compact bnodes s-matches]
  (reduce (fn [node [_ p o]]
            ;; There may be no p or o matches, e. from an :id pattern
            (if-let [pred (json-ld-predicate p)]
              (assoc node (compact pred) (json-ld-object compact bnodes pred o))
              node))
          (json-ld-subject compact bnodes (ffirst s-matches))
          s-matches))

(defn nest-multicardinal-values
  "Aggregate unique values for the same predicate into a vector."
  [nodes]
  (apply merge-with #(if (and (sequential? %1)
                              (not= %1 %2))
                       (into %1 %2)
                       %2)
         nodes))
