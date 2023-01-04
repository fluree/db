(ns fluree.db.query.fql.syntax
  (:require [clojure.spec.alpha :as s]
            [fluree.db.util.core :refer [pred-ident?]]))

#?(:clj (set! *warn-on-reflection* true))

(s/def ::limit pos-int?)

(s/def ::offset nat-int?)

(s/def ::maxFuel number?)
(s/def ::max-fuel ::maxFuel)

(s/def ::depth nat-int?)

(s/def ::prettyPrint boolean?)
(s/def ::pretty-print ::prettyPrint)

(s/def ::parseJSON boolean?)
(s/def ::parse-json ::parseJSON)

(s/def ::opts (s/keys :opt-un [::maxFuel ::max-fuel ::parseJSON ::parse-json
                               ::prettyPrint ::pretty-print]))

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

(s/def ::function (s/or :string fn-string?, :list fn-list?))

(s/def ::filter (s/coll-of ::function))

(defn wildcard?
  [x]
  (#{"*" :* '*} x))

(s/def ::wildcard wildcard?)

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(s/def ::var variable?)

(s/def ::iri (s/or :keyword keyword?
                   :string string?))

(defn sid?
  [x]
  (int? x))

(s/def ::subject (s/or :sid   sid?
                       :iri   ::iri
                       :ident pred-ident?))

(s/def ::subselect-map (s/map-of ::iri ::subselection))

(s/def ::subselection (s/coll-of (s/or :wildcard  ::wildcard
                                       :predicate ::iri
                                       :map       ::subselect-map)))

(s/def ::select-map (s/map-of ::var ::subselection
                              :count 1))

(s/def ::selector
  (s/or :var       ::var
        :pred      ::iri
        :aggregate ::function
        :map       ::select-map))

(s/def ::select (s/or :selector   ::selector
                      :collection (s/coll-of ::selector)))

(s/def ::selectOne ::select)
(s/def ::select-one ::selectOne)

(defn asc?
  [x]
  (boolean (#{'asc "asc" :asc} x)))

(defn desc?
  [x]
  (boolean (#{'desc "desc" :desc} x)))

(s/def ::direction (s/or :asc asc?, :desc desc?))

(s/def ::ordering (s/or :scalar ::var
                        :vector (s/cat :direction ::direction
                                       :dimension ::var)))

(s/def ::orderBy (s/or :clause     ::ordering
                       :collection (s/coll-of ::ordering)))
(s/def ::order-by ::orderBy)

(s/def ::groupBy (s/or :clause     ::var
                       :collection (s/coll-of ::var)))
(s/def ::group-by ::groupBy)

(def first-key
  (comp key first))

(s/def ::where-op #{:filter :optional :union :bind})

(defmulti where-map-spec first-key)

(s/def ::where-map (s/and (s/map-of ::where-op any?, :count 1)
                          (s/multi-spec where-map-spec first-key)))

(s/def ::triple (s/cat :subject   (s/or :var variable?, :val ::subject)
                       :predicate (s/or :var variable?, :iri ::iri)
                       :object    (s/or :var   variable?
                                        :ident pred-ident?
                                        :val   any?)))

(s/def ::where-tuple (s/or :triple  ::triple
                           :binding (s/coll-of any?, :count 2)
                           :remote  (s/coll-of any?, :count 4)))

(s/def ::where-pattern (s/or :map   ::where-map
                             :tuple ::where-tuple))

(s/def ::where (s/coll-of ::where-pattern))

(s/def ::optional (s/or :single     ::where-pattern
                        :collection ::where))

(s/def ::union (s/coll-of ::where))

(s/def ::bind (s/map-of ::var any?))

(s/def ::where (s/coll-of (s/or :map   ::where-map
                                :tuple ::where-tuple)))

(defmethod where-map-spec :filter
  [_]
  (s/keys :req-un [::filter]))

(defmethod where-map-spec :optional
  [_]
  (s/keys :req-un [::optional]))

(defmethod where-map-spec :union
  [_]
  (s/keys :req-un [::union]))

(defmethod where-map-spec :bind
  [_]
  (s/keys :req-un [::bind]))

(def never? (constantly false))

(defmethod where-map-spec :minus
  [_]
  ;; negation - SPARQL 1.1, not yet supported
  never?)

(defmethod where-map-spec :default
  [_]
  never?)

(s/def ::vars (s/map-of ::var any?))

(s/def ::from (s/or :subj ::subject
                    :coll (s/coll-of sid?))) ; only sids are supported for
                                             ; specifying multiple subjects

(s/def ::basic-query (s/keys :req-un [::from]))

(s/def ::delete ::triple)

(s/def ::delete-op (s/keys :req-un [::delete ::where]
                           :opt-un [::vars]))

(s/def ::analytical-query
  (s/keys :req-un [::where]
          :opt-un [::select ::selectOne ::select-one ::orderBy ::order-by ::groupBy
                   ::group-by ::filter ::vars ::limit ::offset ::maxFuel ::max-fuel
                   ::depth ::opts ::prettyPrint ::pretty-print]))

(defn validate
  [qry]
  (if (s/valid? ::analytical-query qry)
    qry
    (throw (ex-info "Invalid Query"
                    {:status  400
                     :error   :db/invalid-query
                     :reasons (s/explain-data ::analytical-query qry)}))))
