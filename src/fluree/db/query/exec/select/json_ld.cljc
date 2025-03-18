(ns fluree.db.query.exec.select.json-ld
  (:require [fluree.db.query.exec.where :as where]
            [fluree.db.validation :as v]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]))

(defn json-ld-object
  [compact bnodes o-match]
  [(if (where/unmatched? o-match)
     (let [var (where/get-variable o-match)]
       ;; unbound non-bnode variable is an optional match.
       (when (v/bnode-variable? var)
         {(compact const/iri-id) (str var bnodes)}))
     (if-let [iri (where/get-iri o-match)]
       {(compact const/iri-id) (compact iri)}
       (let [v      (where/get-value o-match)
             dt-iri (where/get-datatype-iri o-match)
             lang   (where/get-lang o-match)]
         (if (datatype/inferable-iri? dt-iri)
           v
           (cond-> {(compact const/iri-value) v}
             lang       (assoc (compact const/iri-language) lang)
             (not lang) (assoc (compact const/iri-type) (compact dt-iri)))))))])

(defn json-ld-subject
  [compact bnodes s-match]
  (if-let [iri (where/get-iri s-match)]
    {(compact const/iri-id) (compact (where/get-iri s-match))}
    (let [var (where/get-variable s-match)]
      (if (v/bnode-variable? var)
        {(compact const/iri-id) (str var bnodes)}
        ;; unbound non-bnode variable is an optional match.
        {(compact const/iri-id) nil}))))

(defn json-ld-node
  [compact bnodes s-matches]
  (reduce (fn [node [_ p o]]
            (assoc node (compact (where/get-iri p)) (json-ld-object compact bnodes o)))
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
