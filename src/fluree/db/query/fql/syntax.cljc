(ns fluree.db.query.fql.syntax
  (:require [malli.core :as m]
            [fluree.db.util.core :refer [pred-ident?]]))

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

(defn sid?
  [x]
  (int? x))

(defn asc?
  [x]
  (boolean (#{'asc "asc" :asc} x)))

(defn desc?
  [x]
  (boolean (#{'desc "desc" :desc} x)))

(defn where-op [x]
  (when (map? x)
    (-> x first key)))

(def registry
  (merge
    (m/predicate-schemas)
    (m/class-schemas)
    (m/comparator-schemas)
    (m/type-schemas)
    (m/sequence-schemas)
    (m/base-schemas)
    {::limit pos-int?
     ::offset nat-int?
     ::maxFuel number?
     ::max-fuel ::maxFuel
     ::depth nat-int?
     ::prettyPrint boolean?
     ::pretty-print ::prettyPrint
     ::parseJSON boolean?
     ::parse-json ::parseJSON
     ::contextType [:enum :string :keyword]
     ::context-type ::contextType
     ::opts [:map
             [:maxFuel {:optional true} ::maxFuel]
             [:max-fuel {:optional true} ::maxFuel]
             [:parseJSON {:optional true} ::parseJSON]
             [:parse-json {:optional true} ::parse-json]
             [:prettyPrint {:optional true} ::prettyPrint]
             [:pretty-print {:optional true} ::pretty-print]
             [:contextType {:optional true} ::contextType]
             [:context-type {:optional true} ::contextType]]
     ::function [:orn
                 [:string [:fn fn-string?]]
                 [:list [:fn fn-list?]]]
     ::wildcard [:fn wildcard?]
     ::var [:fn variable?]
     ::iri [:orn
            [:keyword keyword?]
            [:string string?]]
     ::subject [:orn
                [:sid [:fn sid?]]
                [:iri ::iri]
                [:ident [:fn pred-ident?]]]
     ::subselect-map [:map-of ::iri [:ref ::subselection]]
     ::subselection [:sequential [:orn
                                  [:wildcard ::wildcard]
                                  [:predicate ::iri]
                                  [:subselect-map [:ref ::subselect-map]]]]
     ::select-map [:map-of {:max 1}
                   ::var ::subselection]
     ::selector [:orn
                 [:var ::var]
                 [:pred ::iri]
                 [:aggregate ::function]
                 [:select-map ::select-map]]
     ::select [:orn
               [:selector ::selector]
               [:collection [:sequential ::selector]]]
     ::selectOne ::select
     ::select-one ::selectOne
     ::direction [:orn
                  [:asc [:fn asc?]]
                  [:desc [:fn desc?]]]
     ::ordering [:orn
                 [:scalar ::var]
                 [:vector [:catn
                           [:direction ::direction]
                           [:dimension ::var]]]]
     ::orderBy [:orn
                [:clause ::ordering]
                [:collection [:sequential ::ordering]]]
     ::order-by ::orderBy
     ::groupBy [:orn
                [:clause ::var]
                [:collection [:sequential ::var]]]
     ::group-by ::groupBy
     ::where-op [:enum :filter :optional :union :bind]
     ::where-map [:and
                  #_[:map-of {:max 1} ::where-op :any]
                  [:map-of {:max 1} ::where-op :any]
                  [:multi {:dispatch where-op}
                   [:filter [:map [:filter [:ref ::filter]]]]
                   [:optional [:map [:optional [:ref ::optional]]]]
                   [:union [:map [:union [:ref ::union]]]]
                   [:bind [:map [:bind [:ref ::bind]]]]]]
     ::triple [:catn
               [:subject [:orn
                          [:var ::var]
                          [:val ::subject]]]
               [:predicate [:orn
                            [:var ::var]
                            [:iri ::iri]]]
               [:object [:orn
                         [:var ::var]
                         [:ident [:fn pred-ident?]]
                         [:val :any]]]]
     ::where-tuple [:orn
                    [:triple ::triple]
                    [:binding [:sequential {:max 2} :any]]
                    [:remote [:sequential {:max 4} :any]]]
     ::where-pattern [:orn
                      [:where-map ::where-map]
                      [:tuple ::where-tuple]]
     ::optional [:orn
                 [:single ::where-pattern]
                 [:collection [:sequential ::where-pattern]]]
     ::filter [:sequential ::function]
     ::union [:sequential [:sequential ::where-pattern]]
     ::bind [:map-of ::var :any]
     ::where [:sequential [:orn
                           [:where-map ::where-map]
                           [:tuple ::where-tuple]]]
     ::vars [:map-of ::var :any]
     ::t [:or :int :string]
     ::delete ::triple     ::delete-op [:map
                                        [:delete ::delete]
                                        [:where ::where]
                                        [:vars {:optional true} ::vars]]
     ::context [:map-of :any :any]
     ::analytical-query [:map
                         [:where ::where]
                         [:t {:optional true} ::t]
                         [:context {:optional true} ::context]
                         [:select {:optional true} ::select]
                         [:selectOne {:optional true} ::selectOne]
                         [:select-one {:optional true} ::select-one]
                         [:delete {:optional true} ::delete]
                         [:orderBy {:optional true} ::orderBy]
                         [:order-by {:optional true} ::order-by]
                         [:groupBy {:optional true} ::groupBy]
                         [:group-by {:optional true} ::group-by]
                         [:filter {:optional true} ::filter]
                         [:having {:optional true} ::function]
                         [:vars {:optional true} ::vars]
                         [:limit {:optional true} ::limit]
                         [:offset {:optional true} ::offset]
                         [:maxFuel {:optional true} ::maxFuel]
                         [:max-fuel {:optional true} ::max-fuel]
                         [:depth {:optional true} ::depth]
                         [:opts {:optional true} ::opts]
                         [:prettyPrint {:optional true} ::prettyPrint]
                         [:pretty-print {:optional true} ::pretty-print]]
     ::multi-query [:map-of :string ::analytical-query]
     ::query [:orn
              [:single ::analytical-query]
              [:multi ::multi-query]]}))

(def query-validator
  (m/validator ::query {:registry registry}))

(def multi-query?
  (m/validator ::multi-query {:registry registry}))

(defn validate
  [qry]
  (if (query-validator qry)
    qry
    (throw (ex-info "Invalid Query"
                    {:status  400
                     :error   :db/invalid-query
                     :reasons (m/explain ::analytical-query qry {:registry registry})}))))
