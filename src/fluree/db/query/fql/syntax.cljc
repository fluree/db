(ns fluree.db.query.fql.syntax
  (:require [clojure.string :as str]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.validation :as v]
            [fluree.db.util.docs :as docs]
            [malli.core :as m]
            [malli.error :as me]
            [malli.transform :as mt]))

#?(:clj (set! *warn-on-reflection* true))

(defn wildcard?
  [x]
  (#{"*" :* '*} x))

(defn asc?
  [x]
  (boolean (#{'asc "asc" :asc} x)))

(defn desc?
  [x]
  (boolean (#{'desc "desc" :desc} x)))

(defn one-select-key-present?
  [q]
  (log/trace "one-select-key-present? q:" q)
  (if (map? q)
    (let [skeys (->> q keys
                     (map #{:select :selectOne :select-one :selectDistinct
                            :select-distinct})
                     (remove nil?))]
      (log/trace "one-select-key-present? skeys:" skeys)
      (= 1 (count skeys)))
    true))

(defn query-schema
  "Returns schema for queries, with any extra key/value pairs `extra-kvs`
  added to the query map.
  This allows eg http-api-gateway to amend the schema with required key/value pairs
  it wants to require, which are not required/supported here in the db library."
  [extra-kvs]
  [:and
   [:map-of ::json-ld-keyword :any]
   [:fn
    {:error/fn
     (fn [_ _]
       (str "Query does not have exactly one select clause. "
            "One of 'select', 'selectOne', 'select-one', 'selectDistinct', or 'select-distinct' is required in queries. "
            "See documentation here for more details: "
            docs/error-codes-page "#query-missing-select"))}
    one-select-key-present?]
   (into [:map {:closed true}
          [:from {:optional true} ::from]
          [:from-named {:optional true} ::from-named]
          [:where ::where]
          [:t {:optional true} ::t]
          [:context {:optional true} ::context]
          [:select {:optional true} ::select]
          [:selectOne {:optional true} ::select]
          [:select-one {:optional true} ::select]
          [:selectDistinct {:optional true} ::select]
          [:select-distinct {:optional true} ::select]
          [:orderBy {:optional true} ::order-by]
          [:order-by {:optional true} ::order-by]
          [:groupBy {:optional true} ::group-by]
          [:group-by {:optional true} ::group-by]
          [:filter {:optional true} ::filter]
          [:having {:optional true} ::function]
          [:values {:optional true} ::values]
          [:limit {:optional true} ::limit]
          [:offset {:optional true} ::offset]
          [:maxFuel {:optional true} ::max-fuel]
          [:max-fuel {:optional true} ::max-fuel]
          [:depth {:optional true} ::depth]
          [:opts {:optional true} ::opts]
          [:prettyPrint {:optional true} ::pretty-print]
          [:pretty-print {:optional true} ::pretty-print]]
         extra-kvs)])

(def registry
  (merge
   (m/predicate-schemas)
   (m/class-schemas)
   (m/comparator-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/base-schemas)
   v/registry
   {::limit           pos-int?
    ::offset          nat-int?
    ::max-fuel        pos-int?
    ::depth           nat-int?
    ::pretty-print    boolean?
    ::parse-json      boolean?
    ::context-type    [:enum :string :keyword]
    ::issuer          [:maybe string?]
    ::role            :any
    ::did             :any
    ::opts            [:and
                       [:map-of :keyword :any]
                       [:map
                        [:maxFuel {:optional true} ::max-fuel]
                        [:max-fuel {:optional true} ::max-fuel]
                        [:parseJSON {:optional true} ::parse-json]
                        [:parse-json {:optional true} ::parse-json]
                        [:prettyPrint {:optional true} ::pretty-print]
                        [:pretty-print {:optional true} ::pretty-print]
                        [:contextType {:optional true} ::context-type]
                        [:context-type {:optional true} ::context-type]
                        [:issuer {:optional true} ::issuer]
                        [:role {:optional true} ::role]
                        [:did {:optional true} ::did]]]
    ::function        ::v/function
    ::wildcard        [:fn wildcard?]
    ::var             ::v/var
    ::iri             ::v/iri
    ::subject         ::v/subject
    ::subselect-map   [:map-of {:error/message "must be map from iri to subselection"}
                       ::iri [:ref ::subselection]]
    ::subselection    [:sequential {:error/message
                                    "subselection must be a vector"}
                       [:orn {:error/message "subselection must be a wildcard (\"*\") or subselection map"}
                        [:wildcard ::wildcard]
                        [:predicate ::iri]
                        [:subselect-map [:ref ::subselect-map]]]]
    ::select-map      [:map-of {:max 1
                                :error/message "Only one key/val for select-map"}
                       ::var ::subselection]
    ::selector        [:orn {:error/message "selector must be either a variable, wildcard symbol (`*`), iri, function application, or select map"}
                       [:wildcard ::wildcard]
                       [:var ::var]
                       [:aggregate ::function]
                       [:select-map ::select-map]]
    ::select          [:orn {:error/message "Select must be a valid selector or vector of selectors"}
                       [:selector ::selector]
                       [:collection [:sequential ::selector]]]
    ::direction       [:orn {:error/message "Direction must be ASC or DESC"}
                       [:asc [:fn asc?]]
                       [:desc [:fn desc?]]]
    ::ordering        [:orn {:error/message "Ordering must be a var or two-tuple formatted ['ASC' or 'DESC', var]"}
                       [:scalar ::var]
                       [:vector [:and list?
                                 [:catn
                                  [:direction ::direction]
                                  [:dimension ::var]]]]]
    ::order-by        [:orn {:error/message  "orderBy clause must be variable or two-tuple formatted ['ASC' or 'DESC', var]"}
                       [:clause ::ordering]
                       [:collection [:sequential ::ordering]]]
    ::group-by        [:orn {:error/message "groupBy clause must be a variable or a vector of variables"}
                       [:clause ::var]
                       [:collection [:sequential ::var]]]
    ::triple          ::v/triple
    ::filter          ::v/filter
    ::where           ::v/where
    ::values          ::v/values
    ::t               [:or :int :string]
    ::context         ::v/context
    ::json-ld-keyword ::v/json-ld-keyword
    ::query           (query-schema [])
    ::modification    ::v/modification-txn
    ::from            ::v/from
    ::from-named      ::v/from-named}))

(def triple-validator
  (m/validator ::triple {:registry registry}))

(defn triple?
  [x]
  (triple-validator x))

(def coerce-query*
  (m/coercer ::query (mt/transformer {:name :fql}) {:registry registry}))



(defn humanize-error
  [error]
  (let [explain-data (v/explain-error error)]
    (log/trace "query validation error:"
               (update explain-data :errors
                       (fn [errors] (map #(dissoc % :schema) errors))))
    (-> explain-data
        (v/format-explained-errors nil))))

(defn coerce-query
  [qry]
  (try*
    (coerce-query* qry)
    (catch* e
      (-> e
          humanize-error
          (ex-info {:status 400, :error :db/invalid-query})
          throw))))

(def coerce-where*
  (m/coercer ::where fql-transformer {:registry registry}))

(defn coerce-where
  [where]
  (try*
    (coerce-where* where)
    (catch* e
      (-> e
          humanize-error
          (ex-info {:status 400, :error :db/invalid-query})
          throw))))

(def parse-selector
  (m/parser ::selector {:registry registry}))

(def coerce-modification*
  (m/coercer ::modification (mt/transformer {:name :fql}) {:registry registry}))

(defn coerce-modification
  [mdfn]
  (try*
   (coerce-modification* mdfn)
   (catch* e
     (throw (ex-info "Invalid Ledger Modification"
                     {:status  400
                      :error   :db/invalid-query
                      :reasons (humanize-error e)})))))
