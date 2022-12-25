(ns fluree.db.query.fql.syntax
  (:require [clojure.spec.alpha :as s]))

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

(s/def ::ref (s/or :keyword keyword?
                   :string string?))

(s/def ::selector
  (s/or :aggregate ::function
        :var       ::var
        :wildcard  ::wildcard
        :pred      ::ref
        :map       (s/map-of (s/or :var      ::var
                                   :wildcard ::wildcard
                                   :ref     ::ref)
                             ::select
                             :count 1)))

(s/def ::select (s/or :selector   ::selector
                      :collection (s/coll-of ::selector)))

(s/def ::selectOne ::select)
(s/def ::select-one ::selectOne)

(s/def ::selectDistinct ::select)
(s/def ::select-distinct ::selectDistinct)

(s/def ::selectReduced ::select)
(s/def ::select-reduced ::selectReduced)

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

(s/def ::where-tuple (s/or :local   (s/coll-of any?, :count 3)
                           :binding (s/coll-of any?, :count 2)
                           :remote  (s/coll-of any?, :count 4)))

(s/def ::where-pattern (s/or :map   ::where-map
                             :tuple ::where-tuple))

(s/def ::where (s/coll-of ::where-pattern))

(s/def ::optional (s/or :single ::where-pattern
                        :coll   ::where))

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

(s/def ::query-map
  (s/keys :opt-un [::select ::selectOne ::select-one ::selectDistinct ::select-distinct
                   ::selectReduced ::select-reduced ::where ::orderBy ::order-by
                   ::groupBy ::group-by ::filter ::vars ::limit ::offset ::maxFuel
                   ::max-fuel ::depth ::opts ::prettyPrint ::pretty-print]))

(defn validate
  [qry]
  (if (s/valid? ::query-map qry)
    qry
    (throw (ex-info "Invalid Query"
                    {:status  400
                     :error   :db/invalid-query
                     :reasons (s/explain-data ::query-map qry)}))))
