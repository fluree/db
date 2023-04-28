(ns fluree.db.util.validation
  (:require [malli.core :as m]))

(defn iri?
  [v]
  (or (keyword? v) (string? v)))

(def registry
  (merge
   (m/type-schemas)
   {::iri     [:orn
               [:string :string]
               [:keyword :keyword]]
    ::context :any}))
