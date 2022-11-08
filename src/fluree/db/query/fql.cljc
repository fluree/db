(ns fluree.db.query.fql
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.walk :refer [keywordize-keys]]
            [fluree.db.util.core #?(:clj :refer :cljs :refer-macros) [try* catch*]
             :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [vswap!]]
            [fluree.db.query.analytical-parse :as q-parse]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.query.subject-crawl.core :refer [simple-subject-crawl]]
            [fluree.db.query.compound :as compound]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.constants :as const])
  (:refer-clojure :exclude [var? vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

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

(defn var?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(s/def ::var var?)

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
                                       :field     ::var)))

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

(defmethod where-map-spec :filter
  [_]
  (s/map-of ::where-op ::filter))

(defmethod where-map-spec :optional
  [_]
  (s/map-of ::where-op ::where))

(defmethod where-map-spec :union
  [_]
  (s/map-of ::where-op (s/coll-of ::where, :count 2)))

(defmethod where-map-spec :bind
  [_]
  (s/map-of ::where-op map?))

(def never? (constantly false))

(defmethod where-map-spec :minus
  [_]
  ;; negation - SPARQL 1.1, not yet supported
  never?)

(defmethod where-map-spec :default
  [_]
  never?)

(s/def ::where-map (s/and (s/map-of ::where-op map?, :count 1)
                          (s/multi-spec where-map-spec first-key)))

(s/def ::where-tuple (s/or :binding (s/coll-of any?, :count 2)
                           :local   (s/coll-of any?, :count 3)
                           :remote  (s/coll-of any?, :count 4)))

(s/def ::where (s/coll-of (s/or :map   ::where-map
                                :tuple ::where-tuple)))

(s/def ::query-map
  (s/keys :opt-un [::select ::selectOne ::select-one ::selectDistinct ::select-distinct
                   ::selectReduced ::select-reduced ::where ::orderBy ::order-by
                   ::groupBy ::group-by ::filter ::limit ::offset ::maxFuel ::max-fuel
                   ::depth ::opts ::prettyPrint ::pretty-print]))

(defn update-if-set
  [m k f]
  (if (contains? m k)
    (update m k f)
    m))

(defn normalize
  [qry]
  (update-if-set qry :opts keywordize-keys))

(defn validate
  [qry]
  (if (s/valid? ::query-map qry)
    qry
    (throw (ex-info "Invalid Query"
                    {:status  400
                     :error   :db/invalid-query
                     :reasons (s/explain-data ::query-map qry)}))))

(def read-fn-str #?(:clj  read-string
                    :cljs cljs.reader/read-string))

(defn read-fn-safe
  [fn-str]
  (try*
   (read-fn-str fn-str)
   (catch* e
           (log/error "Failed parsing:" fn-str "with error message: " (ex-message e))
           (throw (ex-info (str "Invalid query function: " fn-str)
                           {:status 400
                            :error :db/invalid-query})))))

(defn with-default
  [x default]
  (or x default))

(defn transform
  [qry]
  (-> qry
      (update :limit with-default util/max-integer)
      (update :offset with-default 0)
      (update :fuel with-default util/max-integer)
      (update :depth with-default 0)
      (update-if-set :orderBy (fn [ob]
                                (if (vector? ob)
                                  ob
                                  [ob])))
      (update-if-set :groupBy (fn [grp]
                                (if (sequential? grp)
                                  grp
                                  [grp])))
      (update-if-set :filter (fn [fns]
                               (map read-fn-safe fns)))))

(defn process-where-item
  [db cache compact-fn fuel-vol fuel where-item spec inVector?]
  (go-try
    (loop [[spec-item & r'] spec
           result-item []]
      (if spec-item
        (let [{:keys [selection in-n iri? o-var? grouped?]} spec-item
              value  (nth where-item in-n)
              value* (cond
                       ;; there is a sub-selection (graph crawl)
                       selection
                       (let [flakes (<? (query-range/index-range db :spot = [value]))]
                         (<? (json-ld-resp/flakes->res db cache compact-fn fuel-vol fuel (:spec spec-item) 0 flakes)))

                       grouped?
                       (if o-var?
                         (mapv first value)
                         value)

                       ;; subject id coming it, we know it is an IRI so resolve here
                       iri?
                       (or (get @cache value)
                           (let [c-iri (<? (db-proto/-iri db value compact-fn))]
                             (vswap! cache assoc value c-iri)
                             c-iri))

                       o-var?
                       (let [[val datatype] value]
                         (if (= const/$xsd:anyURI datatype)
                           (or (get @cache val)
                               (let [c-iri (<? (db-proto/-iri db val compact-fn))]
                                 (vswap! cache assoc val c-iri)
                                 c-iri))
                           val))

                       :else
                       value)]
          (recur r' (conj result-item value*)))
        (if inVector?
          result-item
          (first result-item))))))


(defn process-select-results
  "Processes where results into final shape of specified select statement."
  [db out-ch where-ch error-ch {:keys [select fuel compact-fn group-by] :as _parsed-query}]
  (go-try
    (let [{:keys [spec inVector?]} select
          cache    (volatile! {})
          fuel-vol (volatile! 0)
          {:keys [group-finish-fn]} group-by]
      (loop []
        (let [where-items (async/alt!
                            error-ch ([e]
                                      (throw e))
                            where-ch ([result-chunk]
                                      result-chunk))]
          (if where-items
            (do
              (loop [[where-item & r] (if group-finish-fn   ;; note - this could be added to the chan as a transducer - however as all results are in one big, sorted chunk I don't expect any performance benefit
                                        (map group-finish-fn where-items)
                                        where-items)]
                (if where-item
                  (let [where-result (<? (process-where-item db cache compact-fn fuel-vol fuel where-item spec inVector?))]
                    (async/>! out-ch where-result)
                    (recur r))))
              (recur))
            (async/close! out-ch)))))))


(defn- ad-hoc-query
  "Legacy ad-hoc query processor"
  [db {:keys [fuel] :as parsed-query}]
  (let [out-ch (async/chan)]
    (let [max-fuel fuel
          fuel     (volatile! 0)
          error-ch (async/chan)
          where-ch (compound/where parsed-query error-ch fuel max-fuel db)]
      (process-select-results db out-ch where-ch error-ch parsed-query))
    out-ch))


(defn cache-query
  "Returns already cached query from cache if available, else
  executes and stores query into cache."
  [{:keys [network ledger-id block auth conn] :as db} query-map]
  ;; TODO - if a cache value exists, should max-fuel still be checked and throw if not enough?
  (let [oc        (:object-cache conn)
        query*    (update query-map :opts dissoc :fuel :max-fuel)
        cache-key [:query network ledger-id block auth query*]]
    ;; object cache takes (a) key and (b) fn to retrieve value if null
    (oc cache-key
        (fn [_]
          (let [pc (async/promise-chan)]
            (async/go
              (let [res (async/<! (query db (assoc-in query-map [:opts :cache]
                                                      false)))]
                (async/put! pc res)))
            pc)))))


(defn cache?
  "Returns true if query was requested to run from the cache."
  [{:keys [opts] :as _query-map}]
  #?(:clj (:cache opts) :cljs false))


(defn first-async
  "Returns first result of a sequence returned from an async channel."
  [ch]
  (go-try
    (let [res (<? ch)]
      (first res))))

(defn query
  "Returns core async channel with results or exception"
  [db query-map]
  (log/debug "Running query:" query-map)
  (if (cache? query-map)
    (cache-query db query-map)
    (let [parsed-query (q-parse/parse db (-> query-map normalize validate transform))
          db*          (assoc db :ctx-cache (volatile! {}))] ;; allow caching of some functions when available
      (if (= :simple-subject-crawl (:strategy parsed-query))
        (simple-subject-crawl db* parsed-query)
        (cond-> (async/into [] (ad-hoc-query db* parsed-query))
          (:selectOne? parsed-query) (first-async))))))
