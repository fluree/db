(ns fluree.db.util.validation
  (:require [malli.core :as m]
            [malli.transform :as mt]))

(def value? (complement coll?))

(defn decode-iri
  [v]
  (cond
    (qualified-keyword? v) (str (namespace v) ":" (name v))
    (keyword? v) (name v)
    :else v))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/comparator-schemas)
   (m/predicate-schemas)
   {::iri              [:string {:decode/fluree decode-iri}]
    ::val              [:fn value?]
    ::string-key       [:string {:decode/fluree name
                                 :encode/fluree keyword}]
    ::context-map      [:map-of ::iri ::iri]
    ::context          [:orn
                        [:sequence [:sequential [:orn
                                                 [:string :string]
                                                 [:map ::context-map]]]]
                        [:map ::context-map]]
    ::context-key      [:= {:decode/fluree #(if (= % :context)
                                              "@context" %)}
                        "@context"]
    ::did              [:orn
                        [:id :string]
                        [:map [:and
                               [:map-of ::string-key :any]
                               [:map
                                ["id" :string]
                                ["public" :string]
                                ["private" :string]]]]]
    ::connect-defaults [:map
                        [:did {:optional true} ::did]
                        [::m/default [:map-of {:max 1} ::context-key ::context]]]
    ::connect-opts     [:and
                        [:map-of ::string-key :any]
                        [:map
                         ["method" {:decode/fluree name} :string]
                         ["defaults" {:optional true} ::connect-defaults]]]
    ::create-opts      [:maybe
                        [:and
                         [:map-of ::string-key :any]
                         [:map
                          ["defaults" {:optional true}
                           [:map-of {:max 1} ::context-key ::context]]]]]
    ::create-response  [:map-of ::string-key :any]}))

(def fluree-transformer
  (mt/transformer {:name :fluree}))

(def coerce-connect-opts
  (m/coercer ::connect-opts fluree-transformer {:registry registry}))

(def coerce-create-opts
  (m/coercer ::create-opts fluree-transformer {:registry registry}))
