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

(defn crawl-ref-item
  [db context compact-fn flake-sid sub-select cache fuel-vol max-fuel depth-i]
  (go-try
    (let [sub-flakes (<? (query-range/index-range db :spot = [flake-sid]))]
      (<? (flakes->res db cache context compact-fn fuel-vol max-fuel sub-select depth-i sub-flakes)))))


(defn add-reverse-specs
  "When @reverse variables are present, crawl for the reverse specs."
  [db cache context compact-fn fuel-vol max-fuel {:keys [reverse] :as select-spec} depth-i flakes]
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
                                              (<? (crawl-ref-item db context compact-fn ref-sid spec cache fuel-vol max-fuel (inc depth-i)))
                                              ;; no sub-selection, just return IRI
                                              (or (get @cache ref-sid)
                                                  (<? (cache-sid->iri db cache compact-fn ref-sid))))]
                                 (recur r (conj acc-item result)))
                               (if (= 1 (count acc-item))
                                 (first acc-item)
                                 acc-item)))]
            (recur r (assoc acc as result)))
          acc)))))


(defn flakes->res
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache context compact-fn fuel-vol max-fuel {:keys [wildcard? _id? depth reverse] :as select-spec} depth-i s-flakes]
  (go-try
    (when (not-empty s-flakes)
      (loop [[p-flakes & r] (partition-by flake/p s-flakes)
             acc (if _id?
                   {:_id (flake/s (first s-flakes))}
                   {})]
        (if p-flakes
          (let [ff    (first p-flakes)
                p     (flake/p ff)
                list? (contains? (flake/m ff) :i)
                spec  (or (get select-spec p)
                          (when wildcard?
                            (wildcard-spec db cache compact-fn p)))
                p-iri (:as spec)
                v     (cond
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

                        :else                               ;; display all values
                        (loop [[f & r] (if list?
                                         (sort-by #(:i (flake/m %)) p-flakes)
                                         p-flakes)
                               acc []]
                          (if f
                            (let [res (if (= const/$xsd:anyURI (flake/dt f))
                                        (cond
                                          ;; have a specified sub-selection (graph crawl)
                                          (:spec spec)
                                          (<? (crawl-ref-item db context compact-fn (flake/o f) (:spec spec) cache fuel-vol max-fuel (inc depth-i)))

                                          ;; requested graph crawl depth has not yet been reached
                                          (< depth-i depth)
                                          (<? (crawl-ref-item db context compact-fn (flake/o f) select-spec cache fuel-vol max-fuel (inc depth-i)))

                                          ;; no sub-selection, just return {@id <iri>} for each ref iri
                                          :else
                                          ;; TODO - we generate id-key here every time, this should be done in the :spec once beforehand and used from there
                                          (let [id-key (:as (wildcard-spec db cache compact-fn const/$iri))
                                                c-iri  (<? (dbproto/-iri db (flake/o f) compact-fn))]
                                            {id-key c-iri}))
                                        (flake/o f))]
                              (recur r (conj acc res)))
                            (cond
                              (#{:list :set} (-> context (get p-iri) :container))
                              acc

                              (= 1 (count acc))
                              (first acc)

                              :else
                              acc))))]
            (if v
              (recur r (assoc acc p-iri v))
              (recur r acc)))
          (if reverse
            (merge acc (<? (add-reverse-specs db cache context compact-fn fuel-vol max-fuel select-spec depth-i s-flakes)))
            acc))))))
