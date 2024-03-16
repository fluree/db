(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.permissions-validate :as validate]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.constants :as const]
            [fluree.db.query.dataset :as dataset]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.json :as json]
            [fluree.db.json-ld.iri :as iri]))

;; handles :select response map for JSON-LD based queries

#?(:clj (set! *warn-on-reflection* true))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (or (get @cache sid)
      (when-let [iri (or (some-> db :schema :pred (get sid) :iri compact-fn)
                         (some-> (iri/decode-sid db sid) compact-fn))]
        (vswap! cache assoc sid {:as iri})
        {:as iri})))

(defn wildcard-spec
  [db cache compact-fn iri]
  (or (get @cache iri)
      (when-let [spec (get-in db [:schema :pred iri])]
        (let [spec* (assoc spec :as (compact-fn (:iri spec)))]
          (vswap! cache assoc iri spec*)
          spec*))))

(defn rdf-type?
  [pid]
  (= const/$rdf:type pid))

(declare flakes->res)
(defn crawl-ref-item
  [db context compact-fn flake-sid sub-select cache depth-i error-ch]
  (go-try
    (let [sub-flakes (<? (query-range/index-range db :spot = [flake-sid]))]
      (<? (flakes->res db cache context compact-fn sub-select depth-i error-ch sub-flakes)))))

(defn add-reverse-specs
  "When @reverse variables are present, crawl for the reverse specs."
  [db cache context compact-fn {:keys [reverse] :as select-spec} depth-i error-ch flakes]
  (go-try
    (let [sid (flake/s (first flakes))]
      (loop [[reverse-item & r] (vals reverse)
             acc {}]
        (if reverse-item
          (let [{:keys [id as spec]} reverse-item
                sub-flakes (<? (query-range/index-range db :opst = [sid id]))
                result     (loop [[ref-sid & r] (map flake/s sub-flakes)
                                  acc-item []]
                             (if ref-sid
                               (let [result (if spec
                                              ;; have a sub-selection
                                              (<! (crawl-ref-item db context compact-fn ref-sid spec cache (inc depth-i) error-ch))
                                              ;; no sub-selection, just return IRI
                                              (:as (cache-sid->iri db cache compact-fn ref-sid)))]
                                 (recur r (conj acc-item result)))
                               (if (= 1 (count acc-item))
                                 (first acc-item)
                                 acc-item)))]
            (recur r (assoc acc as result)))
          acc)))))

(defn includes-id?
  [db sid {:keys [wildcard?] :as select-spec}]
  (go-try
    (when (or wildcard?
              (contains? select-spec const/iri-id))
      (<? (validate/allow-iri? db sid)))))

(defn list-element?
  [flake]
  (-> flake flake/m (contains? :i)))

(defn unwrap-singleton
  ([coll]
   (if (= 1 (count coll))
     (first coll)
     coll))

  ([iri context coll]
   (if (#{:list :set} (-> context (get iri) :container))
     coll
     (unwrap-singleton coll))))

(defn type-value
  [db cache compact-fn type-flakes]
  (->> type-flakes
       (into [] (comp (map flake/o)
                      (map (partial cache-sid->iri db cache compact-fn))
                      (map :as)))
       unwrap-singleton))


(defn display-reference
  [db spec select-spec cache context compact-fn current-depth error-ch oid]
  (go-try
    (let [;; TODO - we generate id-key here every time, this should be done in the :spec once beforehand and used from there
          max-depth (:depth select-spec)
          id-key    (:as (or (wildcard-spec db cache compact-fn const/$id)
                             (cache-sid->iri db cache compact-fn const/$id)))
          o-iri     (compact-fn (iri/decode-sid db oid))
          subselect (:spec spec)]
      (cond
        ;; have a specified sub-selection (graph crawl)
        subselect
        (let [ref-attrs (<! (crawl-ref-item db context compact-fn oid subselect cache (inc current-depth) error-ch))]
          (if (<? (includes-id? db oid subselect))
            (assoc ref-attrs id-key o-iri)
            ref-attrs))

        ;; requested graph crawl depth has not yet been reached
        (< current-depth max-depth)
        (cond-> (<! (crawl-ref-item db context compact-fn oid select-spec cache (inc current-depth) error-ch))
          (<? (validate/allow-iri? db oid)) (assoc id-key o-iri))

        :else
        (when (<? (validate/allow-iri? db oid))
          {id-key o-iri})))))

(defn resolve-reference
  [db cache context compact-fn select-spec current-depth error-ch v]
  (go-try
    (if-let [{:keys [sid spec]} (::reference v)]
      (let [ref (<? (display-reference db spec select-spec cache context
                                       compact-fn current-depth error-ch sid))]
        (not-empty ref))
      v)))

(defn resolve-references
  [db cache context compact-fn select-spec current-depth error-ch attrs]
  (go-try
    (loop [[[prop v] & r] attrs
           resolved-attrs   {}]
      (if prop
        (let [v' (if (sequential? v)
                   (loop [[value & r] v
                          resolved-values   []]
                     (if value
                       (if-let [resolved (<! (resolve-reference db cache context compact-fn select-spec current-depth error-ch value))]
                         (recur r (conj resolved-values resolved))
                         (recur r resolved-values))
                       (not-empty resolved-values)))
                   (<! (resolve-reference db cache context compact-fn select-spec current-depth error-ch v)))]
          (if (some? v')
            (recur r (assoc resolved-attrs prop v'))
            (recur r resolved-attrs)))
        resolved-attrs))))

(defn format-object
  [spec f]
  (let [obj (flake/o f)]
    (cond
      (= const/$xsd:anyURI (flake/dt f))
      {::reference {:sid  obj
                    :spec spec}}

      (= const/$rdf:json (flake/dt f))
      (json/parse obj false)

      :else obj)))

(defn format-property
  [db cache context compact-fn {:keys [wildcard?] :as select-spec} p-flakes]
  (let [ff  (first p-flakes)
        pid (flake/p ff)
        iri (iri/decode-sid db pid)]
    (when-let [spec (or (get select-spec iri)
                        (when wildcard?
                          (or (wildcard-spec db cache compact-fn iri)
                              (cache-sid->iri db cache compact-fn pid))))]
      (let [p-iri (:as spec)
            v     (if (rdf-type? pid)
                    (type-value db cache compact-fn p-flakes)
                    (let [p-flakes* (if (list-element? ff)
                                      (sort-by (comp :i flake/m) p-flakes)
                                      p-flakes)]
                      (->> p-flakes*
                           (mapv (partial format-object spec))
                           (unwrap-singleton p-iri context))))]
        [p-iri v]))))

(defn format-subject-flakes
  [db cache context compact-fn select-spec initial-attrs flakes]
  (into initial-attrs
        (comp (partition-by flake/p)
              (map (partial format-property db cache context
                            compact-fn select-spec))
              (remove nil?))
        flakes))

(defn flakes->res
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache context compact-fn {:keys [reverse] :as select-spec} depth-i error-ch s-flakes]
  (go-try
    (when (not-empty s-flakes)
      (let [sid             (->> s-flakes first flake/s)
            initial-attrs   (if (<? (includes-id? db sid select-spec))
                              (let [iri (compact-fn (iri/decode-sid db sid))]
                                {(compact-fn const/iri-id) iri})
                              {})
            formatted-attrs (format-subject-flakes db cache context compact-fn select-spec initial-attrs s-flakes)
            resolved-attrs  (<? (resolve-references db cache context compact-fn select-spec
                                                    depth-i error-ch formatted-attrs))]
        (if reverse
          (merge resolved-attrs (<? (add-reverse-specs db cache context compact-fn select-spec
                                                       depth-i error-ch s-flakes)))
          resolved-attrs)))))

(defn track-fuel
  [fuel-tracker error-ch]
  (when fuel-tracker
    (fuel/track fuel-tracker error-ch)))

(defn flake-bounds
  [db idx match]
  (let [[start-test start-match end-test end-match]
        (query-range/expand-range-interval idx = match)

        [s1 p1 o1 t1 op1 m1]
        (query-range/match->flake-parts db idx start-match)

        [s2 p2 o2 t2 op2 m2]
        (query-range/match->flake-parts db idx end-match)

        start-flake (query-range/resolve-match-flake start-test s1 p1 o1 t1 op1 m1)
        end-flake   (query-range/resolve-match-flake end-test s2 p2 o2 t2 op2 m2)]
    [start-flake end-flake]))

(defn resolve-subject-properties
  [{:keys [conn t] :as db} iri initial-attrs cache context compact-fn select-spec fuel-tracker error-ch]
  (let [spot-root               (get db :spot)
        spot-novelty            (get-in db [:novelty :spot])
        sid                     (iri/encode-iri db iri)
        [start-flake end-flake] (flake-bounds db :spot [sid])
        range-opts              {:from-t      t
                                 :to-t        t
                                 :start-flake start-flake
                                 :end-flake   end-flake
                                 :flake-xf    (track-fuel fuel-tracker error-ch)}
        flake-slices            (query-range/resolve-flake-slices conn spot-root spot-novelty
                                                                  error-ch range-opts)]
    (async/reduce (partial format-subject-flakes db cache context compact-fn select-spec)
                  initial-attrs flake-slices)))
