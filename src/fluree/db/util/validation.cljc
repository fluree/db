(ns fluree.db.util.validation
  (:require [malli.core :as m]))

(defn iri?
  [v]
  (or (keyword? v) (string? v)))

(defn decode-json-ld-keyword
  [v]
  (if (string? v)
    (if (= \@ (first v))
      (-> v (subs 1) keyword)
      (keyword v))
    v))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   {::iri             [:or :string :keyword]
    ::json-ld-keyword [:keyword {:decode/json decode-json-ld-keyword
                                 :decode/fql  decode-json-ld-keyword}]
    ::context         :any}))
