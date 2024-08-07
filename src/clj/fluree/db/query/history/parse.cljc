(ns fluree.db.query.history.parse
  (:require [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.validation :as v]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [malli.core :as m]))

(defn history-query-schema
  "Returns schema for history queries, with any extra key/value pairs `extra-kvs`
  added to the query map.
  This allows eg http-api-gateway to amend the schema with required key/value pairs
  it wants to require, which are not required/supported here in the db library."
  [extra-kvs]
  [:and
   [:map-of ::json-ld-keyword :any]
   [:fn {:error/message "Must supply a value for either \"history\" or \"commit-details\""}
    (fn [{:keys [history commit-details commit data txn]}]
      (or (string? history) (keyword? history) (seq history)
          commit-details
          commit
          data
          txn))]
   (into
    [:map
     [:history {:optional true}
      [:orn {:error/message
             "Value of \"history\" must be a subject, or a vector containing one or more of subject, predicate, object"}
       [:subject {:error/message "Invalid iri"} ::iri]
       [:flake
        [:or {:error/message "Must provide a tuple of one more more iris"}
         [:catn
          [:s ::iri]]
         [:catn
          [:s [:maybe ::iri]]
          [:p ::iri]]
         [:catn
          [:s [:maybe ::iri]]
          [:p ::iri]
          [:o [:not :nil]]]]]]]
     [:commit-details {:optional      true
                       :error/message "Invalid value of \"commit-details\" key"} :boolean]
     [:commit {:optional true
               :error/message "Invalid value of \"commit\" key"} :boolean]
     [:data {:optional true
             :error/message "Invalid value of \"commit\" key"} :boolean]
     [:txn {:optional true
            :error/message "Invalid value of \"txn\" key"} :boolean]
     [:context {:optional true} ::context]
     [:opts {:optional true} [:map-of :keyword :any]]
     [:t
      [:and
       [:map-of {:error/message "Value of \"t\" must be a map"} :keyword :any]
       [:map
        [:from {:optional true}
         [:or {:error/message "Value of \"from\" must be one of: the key latest, an integer > 0, or an iso-8601 datetime value"}
          [:= :latest]
          [:int {:min           0
                 :error/message "Must be a positive value"}]
          [:re datatype/iso8601-datetime-re]]]
        [:to {:optional true}
         [:or {:error/message "Value of \"to\" must be one of: the key latest, an integer > 0, or an iso-8601 datetime value"}
          [:= :latest]
          [:int {:min           0
                 :error/message "Must be a positive value"}]
          [:re datatype/iso8601-datetime-re]]]
        [:at {:optional true}
         [:or {:error/message "Value of \"at\" must be one of: the key latest, an integer > 0, or an iso-8601 datetime value"}
          [:= :latest]
          [:int {:min           0
                 :error/message "Must be a positive value"}]
          [:re datatype/iso8601-datetime-re]]]]
       [:fn {:error/message "Must provide: either \"from\" or \"to\", or the key \"at\" "}
        (fn [{:keys [from to at]}]
          ;; if you have :at, you cannot have :from or :to
          (if at
            (not (or from to))
            (or from to)))]
       [:fn {:error/message "\"from\" value must be less than or equal to \"to\" value"}
        (fn [{:keys [from to]}] (if (and (number? from) (number? to))
                                  (<= from to)
                                  true))]]]]
    extra-kvs)])

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/predicate-schemas)
   (m/comparator-schemas)
   (m/sequence-schemas)
   v/registry
   {::iri             ::v/iri
    ::json-ld-keyword ::v/json-ld-keyword
    ::context         ::v/context
    ::history-query   (history-query-schema [])}))

(def coerce-history-query*
  "Provide a time range :t and either :history or :commit-details, or both.

  :history - either a subject iri or a vector in the pattern [s p o] with either the
  s or the p is required. If the o is supplied it must not be nil.

  :context or \"@context\" - json-ld context to use in expanding the :history iris.

  :commit-details - if true, each result will have a :commit key with the commit map as a value.

  :t  - a map containing either:
  - :at
  - either :from or :to

  accepted values for t maps:
       - positive t-value
       - datetime string
       - :latest keyword"
  (m/coercer ::history-query syntax/fql-transformer {:registry registry}))

(defn coerce-history-query
  [query-map]
  (try*
    (coerce-history-query* query-map)
    (catch* e
      (throw
       (ex-info
        (-> e
            v/explain-error
            (v/format-explained-errors nil))
        {:status 400
         :error  :db/invalid-query})))))

(def explain-error
  (m/explainer ::history-query {:registry registry}))

(def parse-history-query*
  (m/parser ::history-query {:registry registry}))

(defn parse-history-query
  [query-map]
  (-> query-map coerce-history-query parse-history-query*))
