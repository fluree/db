(ns fluree.db.util.validation
  (:require [malli.core :as m]))

(defn iri?
  [v]
  (or (keyword? v) (string? v)))

(def registry
  (merge
   (m/type-schemas)
   {::iri     [:or :string :keyword]
    ::context :any}))
