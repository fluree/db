(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.permissions-validate :as validate]
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

(defn type-value
  [db cache compact-fn type-flakes]
  (let [types (into []
                    (comp (map flake/o)
                          (map (partial cache-sid->iri db cache compact-fn))
                          (map :as))
                    type-flakes)]
    (if (= 1 (count types))
      (first types)
      types)))


(defn display-reference
  [db spec select-spec cache context compact-fn max-depth current-depth max-fuel fuel-vol oid]
  (go-try
    (let [;; TODO - we generate id-key here every time, this should be done in the :spec once beforehand and used from there
          id-key    (:as (or (wildcard-spec db cache compact-fn const/$id)
                             (cache-sid->iri db cache compact-fn const/$id)))
          o-iri     (compact-fn (iri/decode-sid db oid))
          subselect (:spec spec)]
      (cond
        ;; have a specified sub-selection (graph crawl)
        subselect
        (let [ref-attrs (<? (crawl-ref-item db context compact-fn oid subselect cache fuel-vol max-fuel (inc current-depth)))]
          (if (<? (includes-id? db oid subselect))
            (assoc ref-attrs id-key o-iri)
            ref-attrs))

        ;; requested graph crawl depth has not yet been reached
        (< current-depth max-depth)
        (cond-> (<? (crawl-ref-item db context compact-fn oid select-spec cache fuel-vol max-fuel (inc current-depth)))
          (<? (validate/allow-iri? db oid)) (assoc id-key o-iri))

        :else
        (when (<? (validate/allow-iri? db oid))
          {id-key o-iri})))))

(defn flakes->res
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache context compact-fn fuel-vol max-fuel {:keys [wildcard? depth reverse] :as select-spec} depth-i s-flakes]
  (go-try
    (when (not-empty s-flakes)
      (let [sid           (->> s-flakes first flake/s)
            initial-attrs (if (<? (includes-id? db sid select-spec))
                            (let [iri (compact-fn (iri/decode-sid db sid))]
                              {(compact-fn const/iri-id) iri})
                            {})]
        (loop [[p-flakes & r] (partition-by flake/p s-flakes)
               acc            initial-attrs]
          (if p-flakes
            (let [ff    (first p-flakes)
                  pid   (flake/p ff)
                  iri   (iri/decode-sid db pid)
                  spec  (or (get select-spec iri)
                            (when wildcard?
                              (or (wildcard-spec db cache compact-fn iri)
                                  (cache-sid->iri db cache compact-fn pid))))
                  p-iri (:as spec)
                  v     (cond
                          (nil? spec)
                          nil

                          ;; flake's .-o value is a rdf:type, resolve subject id to IRI then JSON-LD compact it
                          (rdf-type? pid)
                          (type-value db cache compact-fn p-flakes)

                          :else ;; display all values
                          (loop [[f & r] (if (list-element? ff)
                                           (sort-by #(:i (flake/m %)) p-flakes)
                                           p-flakes)
                                 acc     []]
                            (if f
                              (let [res (cond
                                          (= const/$xsd:anyURI (flake/dt f))
                                          (let [oid (flake/o f)]
                                            (<? (display-reference db spec select-spec cache
                                                                   context compact-fn depth depth-i
                                                                   max-fuel fuel-vol oid)))

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
