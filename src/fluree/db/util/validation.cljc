(ns fluree.db.util.validation
  (:require [malli.core :as m]))

(def value? (complement coll?))

(def registry
  (merge
    (m/base-schemas)
    (m/type-schemas)
    (m/comparator-schemas)
    (m/predicate-schemas)
    {::iri     :string
     ::val     [:fn value?]
     ::context [:orn
                [:sequence [:sequential [:orn
                                         [:string :string]
                                         [:map map?]]]]
                [:map map?]]}))
