(ns fluree.db.virtual-graph.parse
  (:require #?(:cljs fluree.db.query.exec.select :refer [SubgraphSelector])
            [clojure.core.async :as async :refer [>! go]]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.update :as exec.update]
            [fluree.db.query.exec.where :as exec.where]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as q-parse])
  #?(:clj (:import (fluree.db.query.exec.select SubgraphSelector))))

#?(:clj (set! *warn-on-reflection* true))

(defn- prop-iri
  "Returns property IRI value from triple"
  [triple]
  (-> triple (nth 1) exec.where/get-iri))

(defn- obj-val
  [triple solution]
  (let [o (nth triple 2)]
    (or (exec.where/get-value o)
        (->> (exec.where/get-variable o)
             (get solution)
             (exec.where/get-value)))))

(defn- obj-var
  [triple]
  (-> triple (nth 2) exec.where/get-variable))

(defn- obj-iri
  [triple]
  (-> triple (nth 2) exec.where/get-iri))

(defn match-search-triple
  [solution triple]
  (go
    (let [p-iri (prop-iri triple)]
      (cond
        (= const/iri-index-target p-iri)
        (assoc-in solution [::virtual-graph ::target] (obj-val triple solution))

        (= const/iri-index-property p-iri)
        (assoc-in solution [::virtual-graph ::property] (obj-iri triple))

        (= const/iri-index-limit p-iri)
        (assoc-in solution [::virtual-graph ::limit] (obj-val triple solution))

        (= const/iri-index-sync p-iri)
        (assoc-in solution [::virtual-graph ::sync] (obj-var triple))

        (= const/iri-index-timeout p-iri)
        (assoc-in solution [::virtual-graph ::timeout] (obj-var triple))

        (= const/iri-index-result p-iri)
        (assoc-in solution [::virtual-graph ::result ::id] (obj-var triple))

        (= const/iri-index-score p-iri)
        (assoc-in solution [::virtual-graph ::result ::score] (obj-var triple))

        (= const/iri-index-vector p-iri)
        (assoc-in solution [::virtual-graph ::result ::vector] (obj-var triple))

        :else
        solution))))

(defn clear-search-params
  [solution]
  (dissoc solution ::virtual-graph))

(defn get-search-params
  [solution]
  (::virtual-graph solution))

(defn has-subgraph-selector?
  "Checks if :select of query is a SubgraphSelector"
  [parsed-query]
  (instance? SubgraphSelector (:select parsed-query)))

(defn subgraph-props
  "Returns a list of iris contained in the :select subgraph.
  Ensures one of them is @id."
  [query-parsed]
  (let [subgraph-iris (->> query-parsed
                           :select
                           :spec
                           vals
                           (keep #(when (map? %)
                                    (when-let [iri (:iri %)]
                                      iri))))]
    (if (some #(= "@id" %) subgraph-iris)
      (->> subgraph-iris
           (filter #(not= "@id" %)))
      (throw (ex-info "BM25 index query must not contain @id in the subgraph selector"
                      {:status 400
                       :error  :db/invalid-index})))))

(defn ensure-select-subgraph
  "Downstream we assume all queries are :select, and not :select-one.
  This wil convert a `:select-one` to a `:select`, in addition verify
  that the select is a subgraph selector."
  [parsed-query]
  (let [parsed-query* (if-let [select-one (:select-one parsed-query)]
                        (-> parsed-query
                            (assoc :select select-one)
                            (dissoc :select-one))
                        parsed-query)]
    (if (has-subgraph-selector? parsed-query*)
      parsed-query*
      (throw (ex-info "BM25 index query must be created with a subgraph selector"
                      {:status 400
                       :error  :db/invalid-index})))))

(defn parse-document-query
  "Parses document query, does some validation, and extracts a list of
  property dependencies in the query that all data updates can be
  evaluated against to see if they are relevant to the index.

  Note the property dependencies cannot be turned into encoded IRIs
  (internal format) yet, because the namespaces used in the query may
  not yet exist if this index was created before data."
  [bm25-opts db-vol]
  (let [query          (:query bm25-opts)
        query-parsed   (-> (q-parse/parse-query query)
                           (ensure-select-subgraph))
        ;; TODO - ultimately we want a property dependency chain, so when the properties change we can
        ;; TODO - trace up the chain to the node(s) that depend on them and update the index accordingly
        where-props    (->> query-parsed ;; IRIs of the properties in the query
                            :where
                            (map #(::exec.where/iri (second %))))
        subgraph-props (subgraph-props query-parsed)
        property-deps  (->> (concat where-props subgraph-props)
                            (map #(exec.update/generate-sid! db-vol %))
                            (into #{}))]

    (assoc bm25-opts :query query
           :query-parsed (assoc query-parsed :selection-context {})
           :property-deps property-deps)))

(defn finalize
  [search-af error-ch solution-ch]
  (let [out-ch (async/chan 1 (map clear-search-params))]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (search-af solution error-ch ch))
                          solution-ch)
    out-ch))

(defn limit-results
  [limit results]
  (if limit
    (take limit results)
    results))

(defn process-results
  "Adds virtual-graph results to solution.
  Leverages db/index (iri-codec) for IRI encoding"
  [iri-codec solution parsed-search sparse-vec? search-results]
  (let [result-bindings (::result parsed-search)
        id-var          (::id result-bindings)
        score-var       (::score result-bindings)
        vector-var      (::vector result-bindings)
        db-alias        (first (where/-aliases iri-codec))
        vec-result-dt   (if sparse-vec?
                          const/iri-sparseVector
                          const/iri-vector)]
    (map (fn [result]
           (cond-> solution
             id-var (assoc id-var (-> (where/unmatched-var id-var)
                                      (where/match-iri (iri/decode-sid iri-codec (:id result)))
                                      (where/match-sid db-alias (:id result))))
             score-var (assoc score-var (-> (where/unmatched-var score-var)
                                            (where/match-value (:score result) const/iri-xsd-float)))
             vector-var (assoc vector-var (-> (where/unmatched-var vector-var)
                                              (where/match-value (:vec result) vec-result-dt)))))
         search-results)))
