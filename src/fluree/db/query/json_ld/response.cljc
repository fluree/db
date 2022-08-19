(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.log :as log :include-macros true]))

;; handles :select response map for JSON-LD based queries

#?(:clj (set! *warn-on-reflection* true))

(declare flakes->res)

(defn wildcard-spec
  [db cache compact-fn pid]
  (or (get @cache pid)
      (let [p-spec (if-let [spec (get-in db [:schema :pred pid])]
                     (assoc spec :as (compact-fn (:iri spec)))
                     {:as pid})]
        (vswap! cache assoc pid p-spec)
        p-spec)))


(defn p-values
  [flakes]
  (if (= 1 (count flakes))
    (-> flakes first flake/o)
    (mapv flake/o flakes)))

(defn iri?
  [pid]
  (= const/$iri pid))

(defn rdf-type?
  [pid]
  (= const/$rdf:type pid))

(defn sid->iri
  [db sid compact-fn]
  (dbproto/-iri db sid compact-fn))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (go-try
    (when-let [iri (<? (sid->iri db sid compact-fn))]
      (vswap! cache assoc sid iri)
      iri)))


(defn iri-only-ref
  "Extracts result information from a ref predicate. If sub-select exists
  and additional graph crawl is performed. If it doesn't exist, simply returns
  {@id <iri>} for each object."
  [db cache compact-fn p-flakes]
  (go-try
    (let [id-key (:as (wildcard-spec db cache compact-fn const/$iri))]
      (loop [[next-flake & r] p-flakes
             acc []]
        (if next-flake
          (let [iri (<? (dbproto/-iri db (flake/o next-flake) compact-fn))]
            (recur r (conj acc {id-key iri})))
          (if (= 1 (count acc))
            (first acc)
            acc))))))

(defn crawl-ref
  "A sub-selection (graph crawl) exists, generate results."
  [db compact-fn p-flakes sub-select cache fuel-vol max-fuel depth-i]
  (go-try
    (loop [[next-flake & r] p-flakes
           acc []]
      (if next-flake
        (let [sub-flakes (<? (query-range/index-range db :spot = [(flake/o next-flake)]))
              res        (<? (flakes->res db cache compact-fn fuel-vol max-fuel sub-select depth-i sub-flakes))]
          (recur r (conj acc res)))
        (if (= 1 (count acc))
          (first acc)
          acc)))))


;; TODO - check for @reverse
(defn flakes->res
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache compact-fn fuel-vol max-fuel {:keys [wildcard? depth] :as select-spec} depth-i flakes]
  (go-try
    (when (not-empty flakes)
      (loop [[p-flakes & r] (partition-by flake/p flakes)
             acc {}]
        (if p-flakes
          (let [p    (-> p-flakes first flake/p)
                spec (or (get select-spec p)
                         (when wildcard?
                           (wildcard-spec db cache compact-fn p)))
                v    (cond
                       (nil? spec)
                       nil

                       ;; flake's .-o value is an IRI string, JSON-LD compact it before returning
                       (iri? p)
                       (-> p-flakes first flake/o compact-fn)

                       ;; flake's .-o value is a rdf:type, resolve subject id to IRI then JSON-LD compact it
                       (rdf-type? p)
                       (loop [[type-id & rest-types] (map flake/o p-flakes)
                              acc []]
                         (if type-id
                           (recur rest-types
                                  (conj acc (or (get @cache type-id)
                                                (<? (cache-sid->iri db cache compact-fn type-id)))))
                           acc))

                       ;; flake's .-o value is a reference to another subject
                       (:ref? spec)
                       (cond
                         ;; have a specified sub-selection (graph crawl)
                         (:spec spec)
                         (<? (crawl-ref db compact-fn p-flakes (:spec spec) cache fuel-vol max-fuel (inc depth-i)))

                         ;; requested graph crawl depth has not yet been reached
                         (< depth-i depth)
                         (<? (crawl-ref db compact-fn p-flakes select-spec cache fuel-vol max-fuel (inc depth-i)))

                         ;; no sub-selection, just return {@id <iri>} for each ref iri
                         :else
                         (<? (iri-only-ref db cache compact-fn p-flakes)))

                       ;; flake's .-o value is a scalar value (e.g. integer, string, etc)
                       :else
                       (p-values p-flakes))]
            (if v
              (recur r (assoc acc (:as spec) v))
              (recur r acc)))
          acc)))))
