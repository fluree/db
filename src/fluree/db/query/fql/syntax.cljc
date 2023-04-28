(ns fluree.db.query.fql.syntax
  (:require [fluree.db.constants :as const]
            [fluree.db.util.core :as util]
            [fluree.db.util.core :refer [try* catch* pred-ident?]]
            [fluree.db.util.validation :as v]
            [malli.core :as m]
            [malli.error :as me]
            [malli.transform :as mt]))

#?(:clj (set! *warn-on-reflection* true))

(defn fn-string?
  [x]
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))

(defn fn-list?
  [x]
  (and (list? x)
       (-> x first symbol?)))

(defn query-fn?
  [x]
  (or (fn-string? x) (fn-list? x)))

(defn wildcard?
  [x]
  (#{"*" :* '*} x))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(def value? (complement coll?))

(defn sid?
  [x]
  (int? x))

(defn asc?
  [x]
  (boolean (#{'asc "asc" :asc} x)))

(defn desc?
  [x]
  (boolean (#{'desc "desc" :desc} x)))

(defn iri-key?
  [x]
  (= const/iri-id x))

(defn where-op [x]
  (when (map? x)
    (-> x first key)))

(defn string->keyword
  [x]
  (if (string? x)
    (keyword x)
    x))

(defn decode-json
  [v]
  (util/keywordize-keys v))

(def registry
  (merge
    (m/predicate-schemas)
    (m/class-schemas)
    (m/comparator-schemas)
    (m/type-schemas)
    (m/sequence-schemas)
    (m/base-schemas)
    v/registry
    {::limit                pos-int?
     ::offset               nat-int?
     ::maxFuel              number?
     ::max-fuel             ::maxFuel
     ::depth                nat-int?
     ::prettyPrint          boolean?
     ::pretty-print         ::prettyPrint
     ::parseJSON            boolean?
     ::parse-json           ::parseJSON
     ::contextType          [:enum :string :keyword]
     ::context-type         ::contextType
     ::issuer               [:maybe string?]
     ::role                 :any
     ::did                  :any
     ::opts                 [:and
                             [:map-of :keyword :any]
                             [:map
                              [:maxFuel {:optional true} ::maxFuel]
                              [:max-fuel {:optional true} ::maxFuel]
                              [:parseJSON {:optional true} ::parseJSON]
                              [:parse-json {:optional true} ::parse-json]
                              [:prettyPrint {:optional true} ::prettyPrint]
                              [:pretty-print {:optional true} ::pretty-print]
                              [:contextType {:optional true} ::contextType]
                              [:context-type {:optional true} ::contextType]
                              [:issuer {:optional true} ::issuer]
                              [:role {:optional true} ::role]
                              [:did {:optional true} ::did]]]
     ::function             [:orn
                             [:string [:fn fn-string?]]
                             [:list [:fn fn-list?]]]
     ::wildcard             [:fn wildcard?]
     ::var                  [:fn variable?]
     ::val                  [:fn value?]
     ::iri                  ::v/iri
     ::subject              [:orn
                             [:sid [:fn sid?]]
                             [:ident [:fn pred-ident?]]
                             [:iri ::iri]]
     ::subselect-map        [:map-of ::iri [:ref ::subselection]]
     ::subselection         [:sequential [:orn
                                          [:wildcard ::wildcard]
                                          [:predicate ::iri]
                                          [:subselect-map [:ref ::subselect-map]]]]
     ::select-map           [:map-of {:max 1}
                             ::var ::subselection]
     ::selector             [:orn
                             [:var ::var]
                             [:pred ::iri]
                             [:aggregate ::function]
                             [:select-map ::select-map]]
     ::select               [:orn
                             [:selector ::selector]
                             [:collection [:sequential ::selector]]]
     ::selectOne            ::select
     ::select-one           ::selectOne
     ::select-distinct      ::select
     ::selectDistinct       ::select-distinct
     ::direction            [:orn
                             [:asc [:fn asc?]]
                             [:desc [:fn desc?]]]
     ::ordering             [:orn
                             [:scalar ::var]
                             [:vector [:and list?
                                       [:catn
                                        [:direction ::direction]
                                        [:dimension ::var]]]]]
     ::orderBy              [:orn
                             [:clause ::ordering]
                             [:collection [:sequential ::ordering]]]
     ::order-by             ::orderBy
     ::groupBy              [:orn
                             [:clause ::var]
                             [:collection [:sequential ::var]]]
     ::group-by             ::groupBy
     ::where-op             [:enum {:decode {:fql string->keyword}}
                             :filter :optional :union :bind]
     ::where-map            [:and
                             [:map-of {:max 1} ::where-op :any]
                             [:multi {:dispatch where-op}
                              [:filter [:map [:filter [:ref ::filter]]]]
                              [:optional [:map [:optional [:ref ::optional]]]]
                              [:union [:map [:union [:ref ::union]]]]
                              [:bind [:map [:bind [:ref ::bind]]]]]]
     ::iri-key              [:fn iri-key?]
     ::iri-map              [:map-of {:max 1}
                             ::iri-key ::iri]
     ::triple               [:catn
                             [:subject [:orn
                                        [:var ::var]
                                        [:val ::subject]]]
                             [:predicate [:orn
                                          [:var ::var]
                                          [:iri ::iri]]]
                             [:object [:orn
                                       [:var ::var]
                                       [:ident [:fn pred-ident?]]
                                       [:iri-map ::iri-map]
                                       [:val :any]]]]
     ::where-tuple          [:orn
                             [:triple ::triple]
                             [:remote [:sequential {:max 4} :any]]]
     ::where-pattern        [:orn
                             [:map ::where-map]
                             [:tuple ::where-tuple]]
     ::optional             [:orn
                             [:single ::where-pattern]
                             [:collection [:sequential ::where-pattern]]]
     ::filter               [:sequential ::function]
     ::union                [:sequential [:sequential ::where-pattern]]
     ::bind                 [:map-of ::var :any]
     ::where                [:sequential [:orn
                                          [:where-map ::where-map]
                                          [:tuple ::where-tuple]]]
     ::var-collection       [:sequential ::var]
     ::val-collection       [:sequential ::val]
     ::single-var-binding   [:tuple ::var ::val-collection]
     ::value-binding        [:sequential ::val]
     ::multiple-var-binding [:tuple
                             ::var-collection
                             [:sequential ::value-binding]]
     ::values               [:orn
                             [:single ::single-var-binding]
                             [:multiple ::multiple-var-binding]]
     ::t                    [:or :int :string]
     ::context              ::v/context
     ::analytical-query     [:map {:decode/json decode-json}
                             [:where ::where]
                             [:t {:optional true} ::t]
                             [:context {:optional true} ::context]
                             [:select {:optional true} ::select]
                             [:selectOne {:optional true} ::selectOne]
                             [:select-one {:optional true} ::select-one]
                             [:selectDistinct {:optional true} ::selectDistinct]
                             [:select-distinct {:optional true} ::select-distinct]
                             [:orderBy {:optional true} ::orderBy]
                             [:order-by {:optional true} ::order-by]
                             [:groupBy {:optional true} ::groupBy]
                             [:group-by {:optional true} ::group-by]
                             [:filter {:optional true} ::filter]
                             [:having {:optional true} ::function]
                             [:values {:optional true} ::values]
                             [:limit {:optional true} ::limit]
                             [:offset {:optional true} ::offset]
                             [:maxFuel {:optional true} ::maxFuel]
                             [:max-fuel {:optional true} ::max-fuel]
                             [:depth {:optional true} ::depth]
                             [:opts {:optional true} ::opts]
                             [:prettyPrint {:optional true} ::prettyPrint]
                             [:pretty-print {:optional true} ::pretty-print]]
     ::multi-query          [:map-of [:or :string :keyword] ::analytical-query]
     ::query                [:orn
                             [:single ::analytical-query]
                             [:multi ::multi-query]]
     ::delete               [:orn
                             [:single ::triple]
                             [:collection [:sequential ::triple]]]
     ::delete-op            [:map
                             [:context {:optional true} ::context]
                             [:delete ::delete]
                             [:where ::where]
                             [:values {:optional true} ::values]]
     ::insert               [:orn
                             [:single ::triple]
                             [:collection [:sequential ::triple]]]
     ::insert-op            [:map
                             [:context {:optional true} ::context]
                             [:insert ::insert]
                             [:where ::where]
                             [:values {:optional true} ::values]]
     ::modification         [:or ::delete-op ::insert-op]}))

(def triple-validator
  (m/validator ::triple {:registry registry}))

(defn triple?
  [x]
  (triple-validator x))

(def query-validator
  (m/validator ::query {:registry registry}))

(def query-coercer
  (m/coercer ::query (mt/transformer {:name :fql}) {:registry registry}))

(def multi-query?
  (m/validator ::multi-query {:registry registry}))

(defn validate-query
  [qry]
  (if (query-validator qry)
    qry
    (throw (ex-info "Invalid Query"
                    {:status  400
                     :error   :db/invalid-query
                     :reasons (m/explain ::analytical-query qry {:registry registry})}))))

(defn coerce-query
  [qry]
  (try* (query-coercer qry)
        (catch* _e
          (throw (ex-info "Invalid Query"
                          {:status  400
                           :error   :db/invalid-query
                           :reasons (me/humanize (m/explain ::query qry {:registry registry}))})))))

(def modification-validator
  (m/validator ::modification {:registry registry}))

(def modification-coercer
  (m/coercer ::modification (mt/transformer {:name :fql}) {:registry registry}))

(defn coerce-modification
  [mdfn]
  (try* (modification-coercer mdfn)
        (catch* _e
          (throw (ex-info "Invalid Ledger Modification"
                          {:status  400
                           :error   :db/invalid-query
                           :reasons (me/humanize (m/explain ::modification mdfn {:registry registry}))})))))
