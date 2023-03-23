(ns fluree.db.util.validation
  (:require [malli.core :as m]))

(def value? (complement coll?))

(def registry
  (merge
    (m/base-schemas)
    (m/type-schemas)
    (m/comparator-schemas)
    {::iri                    :string
     ::val                    [:fn value?]
     ::at-context             [:= "@context"]
     ::context-key            :string
     ::context                [:or
                               :string
                               [:map-of ::context-key [:or :string :map]]]
     ::context-containing-map [:map-of ::at-context ::context]}))
