(ns fluree.db.vector.index-graph
  (:require [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [clojure.core.async :as async]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.vector.scoring :as vector.score]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]))

(def iri-compare (str iri/f-idx-flatrank-ns "compare"))
(def iri-property (str iri/f-idx-flatrank-ns "property"))
(def iri-limit (str iri/f-idx-flatrank-ns "limit"))
(def iri-id (str iri/f-idx-flatrank-ns "id"))
(def iri-score (str iri/f-idx-flatrank-ns "score"))
(def iri-vector (str iri/f-idx-flatrank-ns "vector"))
(def iri-xsd-float "http://www.w3.org/2001/XMLSchema#float")

(def metric-re (re-pattern (str iri/f-idx-flatrank-ns "(.*)")))

(def result-sort (fn [a b] (compare (get a :score) (get b :score))))
(def reverse-result-sort (fn [a b] (compare (get b :score) (get a :score))))

(def metrics
  {:dotproduct {::score-fn vector.score/dotproduct
                ::sort-fn  reverse-result-sort}
   :cosine     {::score-fn vector.score/cosine-similarity
                ::sort-fn  reverse-result-sort}
   :distance   {::score-fn vector.score/euclidian-distance
                ::sort-fn  result-sort}})

(defn- prop-iri
  "Returns property IRI value from triple"
  [triple]
  (-> triple (nth 1) ::where/iri))

(defn- obj-val
  [triple solution]
  (let [o (nth triple 2)]
    (or (where/get-value o)
        (->> (where/get-variable o)
             (get solution)
             (where/get-value)))))

(defn- obj-var
  [triple]
  (-> triple (nth 2) ::where/var))

(defn- obj-iri
  [triple]
  (-> triple (nth 2) ::where/iri))

(defn extract-metric
  "Takes the graph alias as a string and extracts the metric name from the
  end of the IRI"
  [graph-alias]
  (some-> (re-find metric-re graph-alias)
          second
          str/lower-case
          keyword))

(defn parse-search-graph
  [graph-alias solution graph-triples]
  (try*
    (let [metric (extract-metric graph-alias)]
      (reduce
       (fn [acc triple]
         (let [p-iri (prop-iri triple)]
           (cond
             (= iri-compare p-iri)
             (assoc acc ::compare (obj-val triple solution))

             (= iri-property p-iri)
             (assoc acc ::property (obj-iri triple))

             (= iri-limit p-iri)
             (assoc acc ::limit (obj-val triple solution))

             (= iri-id p-iri)
             (assoc-in acc [::result ::id] (obj-var triple))

             (= iri-score p-iri)
             (assoc-in acc [::result ::score] (obj-var triple))

             (= iri-vector p-iri)
             (assoc-in acc [::result ::vector] (obj-var triple))

             :else
             acc)))
       {::metric metric}
       graph-triples))
    (catch* e
            (throw (ex-info (str "Unable to parse search graph provided for index: " graph-alias)
                            {:status 400
                             :error  :db/invalid-query} e)))))

(defn extract-vectors
  [{::keys [property]} db]
  (let [pid (iri/encode-iri db property)]
    ;; For now, pulling all matching values from full index
    ;; once hitting the actual vector index, we'll only need
    ;; to pull matches out of novelty (if that)
    (query-range/index-range db :post = [pid])))

(defn score-vectors
  [{::keys [compare metric]} novelty]
  (try*
    (let [score-fn (get-in metrics [metric ::score-fn])]
      (reduce
       (fn [acc flake]
         (let [vec   (flake/o flake)
               score (score-fn vec compare)]
           (if score
             (conj acc {:id    (flake/s flake)
                        :score score
                        :vec   vec})
             acc)))
       []
       novelty))
    (catch* e
            (throw (ex-info (str "Unable to score vectors in vector index search.")
                            {:status 500
                             :error  :db/unexpected} e)))))

(defn result-candidates
  [{::keys [metric limit]} vectors]
  (let [sort-fn (get-in metrics [metric ::sort-fn])]
    (cond->> (sort sort-fn vectors)
             limit (take limit))))

(defn process-results
  [db solution parsed-search search-results]
  (let [result-bindings (::result parsed-search)
        id-var          (::id result-bindings)
        score-var       (::score result-bindings)
        vector-var      (::vector result-bindings)
        db-alias        (:alias db)]
    (map (fn [result]
           (cond-> solution
                   id-var (assoc id-var (-> (where/unmatched-var id-var)
                                            (where/match-iri (iri/decode-sid db (:id result)))
                                            (where/match-sid db-alias (:id result))))
                   score-var (assoc score-var (-> (where/unmatched-var score-var)
                                                  (where/match-value (:score result) iri-xsd-float)))
                   vector-var (assoc vector-var (-> (where/unmatched-var vector-var)
                                                    (where/match-value (:vec result) const/iri-vector)))))
         search-results)))

(defn search
  [db fuel-tracker solution index-alias search-graph error-ch]
  (let [resp-ch (async/chan)]

    (async/go
      (try*
        (let [parsed-search (parse-search-graph index-alias solution search-graph)]
          (->> (<? (extract-vectors parsed-search db))
               (score-vectors parsed-search)
               (result-candidates parsed-search)
               (process-results db solution parsed-search)
               (async/onto-chan! resp-ch)))
        (catch* e
                (let [e* (if (ex-data e)
                           e
                           (ex-info (str "Unexpected error processing index for results: " index-alias ".")
                                    {:status 500
                                     :error  :db/unexpected} e))]
                  (async/offer! error-ch e*)
                  (async/close! resp-ch)))))

    resp-ch))
