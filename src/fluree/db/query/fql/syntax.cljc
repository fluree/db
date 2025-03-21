(ns fluree.db.query.fql.syntax
  (:require [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.validation :as v]
            [fluree.db.util.docs :as docs]
            [camel-snake-kebab.core :as csk]
            [clojure.edn :as edn]
            [malli.core :as m]
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
                     (map #{:select :select-one :select-distinct :construct})
                     (remove nil?))]
      (log/trace "one-select-key-present? skeys:" skeys)
      (= 1 (count skeys)))
    true))

(def common-query-schema
  [:map {:closed true}
   [:from {:optional true} ::from]
   [:from-named {:optional true} ::from-named]
   [:where {:optional true} ::where]
   [:t {:optional true} ::t]
   [:context {:optional true} ::context]
   [:order-by {:optional true} ::order-by]
   [:group-by {:optional true} ::group-by]
   [:having {:optional true} ::function]
   [:values {:optional true} ::values]
   [:limit {:optional true} ::limit]
   [:offset {:optional true} ::offset]
   [:max-fuel {:optional true} ::max-fuel]
   [:depth {:optional true} ::depth]
   [:opts {:optional true} ::opts]
   [:pretty-print {:optional true} ::pretty-print]])

(defn wrap-query-map-schema
  [schema]
  [:and
   [:map-of ::json-ld-keyword :any]
   [:fn {:error/fn
         (fn [_ _]
           (str "Query does not have exactly one select clause. "
                "One of 'select', 'selectOne', 'select-one', 'selectDistinct', 'select-distinct', or 'construct' is required in queries. "
                "See documentation here for more details: "
                docs/error-codes-page "#query-missing-select"))}
    one-select-key-present?]
   schema])

(defn query-schema
  "Returns schema for queries, with any extra key/value pairs `extra-kvs`
  added to the query map.
  This allows eg http-api-gateway to amend the schema with required key/value pairs
  it wants to require, which are not required/supported here in the db library."
  [extra-kvs]
  (-> common-query-schema
      (into [[:select {:optional true} ::select]
             [:select-one {:optional true} ::select]
             [:select-distinct {:optional true} ::select]
             [:construct {:optional true} ::construct]])
      (into extra-kvs)
      wrap-query-map-schema))

(defn subquery-schema
  [extra-kvs]
  (-> common-query-schema
      (into [[:select {:optional true} ::subquery-select]
             [:select-one {:optional true} ::subquery-select]
             [:select-distinct {:optional true} ::subquery-select]])
      (into extra-kvs)
      wrap-query-map-schema))

(defn string->ordering
  [x]
  (if (string? x)
    (edn/read-string x)
    x))

(def registry
  (merge
   (m/predicate-schemas)
   (m/class-schemas)
   (m/comparator-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/base-schemas)
   v/registry
   {::limit             pos-int?
    ::offset            nat-int?
    ::max-fuel          pos-int?
    ::depth             nat-int?
    ::pretty-print      boolean?
    ::default-allow?    boolean?
    ::parse-json        boolean?
    ::issuer            [:maybe string?]
    ::role              :any
    ::identity          :any
    ::format            [:enum :sparql :fql]
    ::opts              [:map
                         [:max-fuel {:optional true} ::max-fuel]
                         [:identity {:optional true} ::identity]
                         [:policy {:optional true} :any]
                         [:policy-class {:optional true} :any]
                         [:policy-values {:optional true} :any]
                         [:meta {:optional true} :boolean]
                         [:format {:optional true} ::format]
                         [:output {:optional true} [:enum :sparql :fql]]
                         ;; deprecated
                         [:role {:optional true} ::role]
                         [:issuer {:optional true} ::issuer]
                         [:pretty-print {:optional true} ::pretty-print]
                         [:did {:optional true} ::identity]
                         [:default-allow? {:optional true} ::default-allow?]
                         [:parse-json {:optional true} ::parse-json]]
    ::stage-opts        [:map
                         [:meta {:optional true} :boolean]
                         [:max-fuel {:optional true} ::max-fuel]
                         [:identity {:optional true} ::identity]
                         [:format {:optional true} ::format]
                         [:did {:optional true} ::identity]
                         [:context {:optional true} ::context]
                         [:raw-txn {:optional true} :any]
                         [:author {:optional true} ::identity]
                         [:policy-values {:optional true} :any]]
    ::commit-opts       [:map
                         [:identity {:optional true} ::identity]
                         [:context {:optional true} ::context]
                         [:raw-txn {:optional true} :any]
                         [:author {:optional true} ::identity]
                         [:private {:optional true} :string]
                         [:message {:optional true} :string]
                         [:tag {:optional true} :string]
                         [:index-files-ch {:optional true} :any]
                         [:time {:optional true} :string]]
    ::ledger-opts       [:map
                         [:did {:optional true} ::identity]
                         [:identity {:optional true} ::identity]
                         [:branch {:optional true} :string]
                         [:indexing {:optional true}
                          [:map
                           [:reindex-min-bytes {:optional true} nat-int?]
                           [:reindex-max-bytes {:optional true} nat-int?]]]]
    ::txn-opts          [:and ::stage-opts ::commit-opts]
    ::function          ::v/function
    ::as-function       ::v/as-function
    ::wildcard          [:fn wildcard?]
    ::var               ::v/var
    ::iri               ::v/iri
    ::subject           ::v/subject
    ::subselect-map     [:map-of {:error/message "must be map from iri to subselection"}
                         ::iri [:ref ::subselection]]
    ::subselection      [:sequential {:error/message
                                      "subselection must be a vector"}
                         [:orn {:error/message "subselection must be a wildcard (\"*\") or subselection map"}
                          [:wildcard ::wildcard]
                          [:predicate ::iri]
                          [:subselect-map [:ref ::subselect-map]]]]
    ::select-map-key    [:orn {:error/message "select map key must be a variable or iri"}
                         [:var ::var] [:iri ::iri]]
    ::select-map        [:map-of {:max           1
                                  :error/message "Only one key/val for select-map"}
                         ::select-map-key ::subselection]
    ::selector          [:orn {:error/message "selector must be either a variable, iri, function application, or select map"}
                         [:var ::var]
                         [:aggregate ::function]
                         [:select-map ::select-map]]
    ::select            [:orn {:error/message "Select must be a valid selector, a wildcard symbol (`*`), or a vector of selectors"}
                         [:wildcard ::wildcard]
                         [:selector ::selector]
                         [:collection [:sequential ::selector]]]
    ::subquery-selector [:orn {:error/message "selector must be either a variable iri, function application, or select map"}
                         [:var ::var]
                         [:aggregate ::as-function]]
    ::subquery-select   [:orn {:error/message "Select must be a valid selector, a wildcard symbol (`*`), or a vector of selectors. Subqueries do not allow graph crawl syntax (e.g. {?x [*]})."}
                         [:wildcard ::wildcard]
                         [:selector ::subquery-selector]
                         [:collection [:sequential ::subquery-selector]]]
    ::direction         [:orn {:error/message "Direction must be \"asc\" or \"desc\""}
                         [:asc [:fn asc?]]
                         [:desc [:fn desc?]]]
    ::ordering          [:orn {:error/message "Ordering must be a var or a direction function call such as '(asc ?var)' or '(desc ?var)'."
                               :decode/fql  string->ordering
                               :decode/json string->ordering}
                         [:scalar ::var]
                         [:vector [:and list?
                                   [:catn
                                    [:direction ::direction]
                                    [:dimension ::var]]]]]
    ::order-by          [:orn {:error/message "orderBy clause must be variable or two-tuple formatted ['ASC' or 'DESC', var]"}
                         [:clause ::ordering]
                         [:collection [:sequential ::ordering]]]
    ::group-by          [:orn {:error/message "groupBy clause must be a variable or a vector of variables"}
                         [:clause ::var]
                         [:collection [:sequential ::var]]]
    ::construct         ::v/construct
    ::filter            ::v/filter
    ::where             ::v/where
    ::values            ::v/values
    ::t                 [:or :int :string]
    ::context           ::v/context
    ::json-ld-keyword   ::v/json-ld-keyword
    ::query             (query-schema [])
    ::subquery          (subquery-schema [])
    ::modification      ::v/modification-txn
    ::from              ::v/from
    ::from-named        ::v/from-named}))

(def fql-transformer
  (mt/transformer
    {:name     :fql
     :decoders (mt/-json-decoders)}
    (mt/key-transformer {:decode csk/->kebab-case-keyword})))

(def coerce-query*
  (m/coercer ::query fql-transformer {:registry registry}))

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

(def coerce-subquery*
  (m/coercer ::subquery fql-transformer {:registry registry}))

;; TODO - because ::subquery not defined in the f.d.validation registrity it cannot be called from
;; there and parsed as part of the parent query. For now we have a separate coerce-subquery
;; function - once malli is refactored to be part of the same registry this coudl be avoided
(defn coerce-subquery
  [qry]
  (try*
   (coerce-subquery* qry)
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

(def coerce-txn-opts*
  (m/coercer ::txn-opts fql-transformer {:registry registry}))

(defn coerce-txn-opts
  [opts]
  (try*
    (coerce-txn-opts* opts)
    (catch* e
      (-> e
          humanize-error
          (ex-info {:status 400, :error :db/invalid-txn})
          throw))))

(def coerce-ledger-opts*
  (m/coercer ::ledger-opts fql-transformer {:registry registry}))

(defn coerce-ledger-opts
  [opts]
  (try*
    (coerce-ledger-opts* opts)
    (catch* e
            (-> e
                humanize-error
                (ex-info {:status 400, :error :db/invalid-ledger-opts})
                throw))))
