(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]))

;; handles :select response map for JSON-LD based queries

#?(:clj (set! *warn-on-reflection* true))

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


(defn extract-refs
  [db id-key compact-fn p-flakes]
  (go-try
    (loop [[next-flake & r] p-flakes
           acc []]
      (if next-flake
        (recur r (conj acc {id-key (<? (dbproto/-iri db (flake/o next-flake) compact-fn))}))
        (if (= 1 (count acc))
          (first acc)
          acc)))))


;; TODO - check for @reverse
(defn flakes->res
  [db cache compact-fn fuel-vol max-fuel {:keys [wildcard?] :as select-spec} flakes]
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

                       (iri? p)
                       (-> p-flakes first flake/o compact-fn)

                       (rdf-type? p)
                       (loop [[type-id & rest-types] (map flake/o p-flakes)
                              acc []]
                         (if type-id
                           (recur rest-types
                                  (conj acc (or (get @cache type-id)
                                                (<? (cache-sid->iri db cache compact-fn type-id)))))
                           acc))

                       (:ref? spec)
                       (let [id-key (:as (wildcard-spec db cache compact-fn const/$iri))]
                         (<? (extract-refs db id-key compact-fn p-flakes)))

                       :else
                       (p-values p-flakes))]
            (if v
              (recur r (assoc acc (:as spec) v))
              (recur r acc)))
          acc)))))
