(ns fluree.db.query.fql.syntax
  (:require [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.validation :as v]
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

(defn decode-multi-query-opts
  [mq]
  (if (and (map? mq) (contains? mq "opts"))
    (let [opts (get mq "opts")]
      (-> mq (assoc :opts opts) (dissoc "opts")))
    mq))

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
    ::function             ::v/function
    ::wildcard             [:fn wildcard?]
    ::var                  ::v/var
    ::iri                  ::v/iri
    ::subject              ::v/subject
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
    ::triple               ::v/triple
    ::filter               ::v/filter
    ::where                ::v/where
    ::values               ::v/values
    ::t                    [:or :int :string]
    ::context              ::v/context
    ::json-ld-keyword      ::v/json-ld-keyword
    ::analytical-query     [:and
                            [:map-of ::json-ld-keyword :any]
                            [:map
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
                             [:pretty-print {:optional true} ::pretty-print]]]
    ::multi-query          [:map {:decode/json decode-multi-query-opts}
                            [:opts {:optional true} ::opts]
                            [::m/default [:map-of [:or :string :keyword]
                                          ::analytical-query]]]
    ::query                [:orn
                            [:single ::analytical-query]
                            [:multi ::multi-query]]
    ::modification         ::v/modification-txn}))

(def triple-validator
  (m/validator ::triple {:registry registry}))

(defn triple?
  [x]
  (triple-validator x))

(def coerce-query*
  (m/coercer ::query (mt/transformer {:name :fql}) {:registry registry}))

(def multi-query?
  (m/validator ::multi-query {:registry registry}))

(defn coerce-query
  [qry]
  (try*
   (coerce-query* qry)
   (catch* e
     (throw (ex-info "Invalid Query"
                     {:status  400
                      :error   :db/invalid-query
                      :reasons (v/humanize-error e)})))))

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
                      :reasons (v/humanize-error e)})))))
