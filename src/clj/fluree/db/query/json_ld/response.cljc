(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.json :as json]
            [fluree.db.json-ld.iri :as iri]))

;; handles :select response map for JSON-LD based queries

#?(:clj (set! *warn-on-reflection* true))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (go-try
    (when-let [iri (or (some-> db :schema :pred (get sid) :iri compact-fn)
                       (some-> sid (iri/sid->iri (:namespace-codes db)) compact-fn))]
      (vswap! cache assoc sid {:as iri})
      {:as iri})))

(defn wildcard-spec
  [db cache compact-fn iri]
  (when-let [spec (get-in db [:schema :pred iri])]
    (let [spec* (assoc spec :as (compact-fn (:iri spec)))]
      (vswap! cache assoc iri spec*)
      spec*)))

(defn rdf-type?
  [pid]
  (= const/$rdf:type pid))

(declare flakes->res)
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
                                              (or (:as (get @cache ref-sid))
                                                  (:as (<? (cache-sid->iri db cache compact-fn ref-sid)))))]
                                 (recur r (conj acc-item result)))
                               (if (= 1 (count acc-item))
                                 (first acc-item)
                                 acc-item)))]
            (recur r (assoc acc as result)))
          acc)))))

(defn includes-id?
  [{:keys [wildcard?] :as select-spec}]
  (or wildcard?
      (contains? select-spec const/iri-id)))

(defn flakes->res
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache context compact-fn fuel-vol max-fuel {:keys [wildcard? _id? depth reverse] :as select-spec} depth-i s-flakes]
  (go-try
    (when (not-empty s-flakes)
      (let [sid (->> s-flakes first flake/s)
            iri (-> sid
                    (iri/sid->iri (:namespace-codes db))
                    compact-fn)]
        (loop [[p-flakes & r] (partition-by flake/p s-flakes)
               acc            (cond-> {}
                                _id?
                                (assoc :_id sid)

                                (includes-id? select-spec)
                                (assoc (compact-fn const/iri-id) iri))]
          (if p-flakes
            (let [ff    (first p-flakes)
                  p     (flake/p ff)
                  iri   (iri/sid->iri p (:namespace-codes db))
                  list? (contains? (flake/m ff) :i)
                  spec  (or (get select-spec iri)
                            (when wildcard?
                              (or (get @cache iri)
                                  (wildcard-spec db cache compact-fn iri)
                                  (<? (cache-sid->iri db cache compact-fn p)))))
                  p-iri (:as spec)
                  v     (cond
                          (nil? spec)
                          nil

                          ;; flake's .-o value is a rdf:type, resolve subject id to IRI then JSON-LD compact it
                          (rdf-type? p)
                          (loop [[type-id & rest-types] (map flake/o p-flakes)
                                 acc                    []]
                            (if type-id
                              (recur rest-types
                                     (conj acc (:as (or (get @cache type-id)
                                                        (<? (cache-sid->iri db cache compact-fn type-id))))))
                              (if (= 1 (count acc))
                                (first acc)
                                acc)))

                          :else ;; display all values
                          (loop [[f & r] (if list?
                                           (sort-by #(:i (flake/m %)) p-flakes)
                                           p-flakes)
                                 acc     []]
                            (if f
                              (let [res (cond
                                          (= const/$xsd:anyURI (flake/dt f))
                                          (let [;; TODO - we generate id-key here every time, this should be done in the :spec once beforehand and used from there
                                                id-key   (:as (or (get @cache const/$id)
                                                                  (wildcard-spec db cache compact-fn const/$id)
                                                                  (<? (cache-sid->iri db cache compact-fn const/$id))))
                                                ns-codes (:namespace-codes db)
                                                c-iri    (-> f flake/o (iri/sid->iri ns-codes) compact-fn)
                                                ref-attrs (cond
                                                            ;; have a specified sub-selection (graph crawl)
                                                            (:spec spec)
                                                            (<? (crawl-ref-item db context compact-fn (flake/o f) (:spec spec) cache fuel-vol max-fuel (inc depth-i)))

                                                            ;; requested graph crawl depth has not yet been reached
                                                            (< depth-i depth)
                                                            (<? (crawl-ref-item db context compact-fn (flake/o f) select-spec cache fuel-vol max-fuel (inc depth-i))))]
                                            (assoc ref-attrs id-key c-iri))

                                          (= const/$rdf:json (flake/dt f))
                                          (json/parse (flake/o f) false)

                                          :else
                                          (flake/o f))]
                                (recur r (conj acc res)))
                              (if (and (= 1 (count acc))
                                       (not (#{:list :set} (-> context (get p-iri) :container))))
                                (first acc)
                                acc))))]
              (if (some? v)
                (recur r (assoc acc p-iri v))
                (recur r acc)))
            (if reverse
              (merge acc (<? (add-reverse-specs db cache context compact-fn fuel-vol max-fuel select-spec depth-i s-flakes)))
              acc)))))))
