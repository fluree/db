(ns fluree.db.vector.index-graph
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.vector.scoring :as vector.score]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]))

(def iri-search (str iri/f-idx-ns "search"))
(def iri-property (str iri/f-idx-ns "property"))
(def iri-limit (str iri/f-idx-ns "limit"))
(def iri-id (str iri/f-idx-ns "id"))
(def iri-score (str iri/f-idx-ns "score"))
(def iri-result (str iri/f-idx-ns "result"))
(def iri-vector (str iri/f-idx-ns "vector"))
(def iri-xsd-float "http://www.w3.org/2001/XMLSchema#float")

(def flatrank-vg-re (re-pattern "##Flatrank-(.*)"))

(defn result-sort
  [a b]
  (compare (:score a) (:score b)))

(defn reverse-result-sort
  [a b]
  (compare (:score b) (:score a)))

(defn- prop-iri
  "Returns property IRI value from triple"
  [triple]
  (-> triple (nth 1) where/get-iri))

(defn- obj-val
  [triple solution]
  (let [o (nth triple 2)]
    (or (where/get-value o)
        (->> (where/get-variable o)
             (get solution)
             (where/get-value)))))

(defn- obj-var
  [triple]
  (-> triple (nth 2) where/get-variable))

(defn- obj-iri
  [triple]
  (-> triple (nth 2) where/get-iri))

(defn match-search-triple
  [solution triple]
  (let [p-iri (prop-iri triple)]
    (cond
      (= iri-search p-iri)
      (assoc-in solution [::flat-rank ::search] (obj-val triple solution))

      (= iri-property p-iri)
      (assoc-in solution [::flat-rank ::property] (obj-iri triple))

      (= iri-limit p-iri)
      (assoc-in solution [::flat-rank ::limit] (obj-val triple solution))

      (= iri-result p-iri)
      (assoc-in solution [::flat-rank ::result ::id] (obj-var triple))

      (= iri-score p-iri)
      (assoc-in solution [::flat-rank ::result ::score] (obj-var triple))

      (= iri-vector p-iri)
      (assoc-in solution [::flat-rank ::result ::vector] (obj-var triple)))))

(defn get-search-params
  [solution]
  (::flat-rank solution))

(defn clear-search-params
  [solution]
  (dissoc solution ::flat-rank))

(defn format-result
  [f score]
  {:id    (flake/s f)
   :score score
   :vec   (flake/o f)})

(defn rank
  [db search-params score-fn sort-fn error-ch]
  (go
    (try*
      (let [{::keys [property search limit]} search-params

            pid       (iri/encode-iri db property)
            score-opt {:flake-xf (comp (map (partial score-fn search))
                                       (remove nil?))}

            ;; For now, pulling all matching values from full index
            ;; once hitting the actual vector index, we'll only need
            ;; to pull matches out of novelty (if that)
            results (<? (query-range/index-range db :post = [pid] score-opt))
            sorted  (sort sort-fn results)]
        (if limit
          (take limit sorted)
          sorted))
      (catch* e
              (log/error e "Error ranking vectors")
              (>! error-ch e)))))

(defn process-results
  [db solution parsed-search search-results]
  (let [result-bindings (::result parsed-search)
        id-var          (::id result-bindings)
        score-var       (::score result-bindings)
        vector-var      (::vector result-bindings)
        db-alias        (:alias db)]
    (map (fn [result]
           (cond-> solution
             id-var     (assoc id-var (-> (where/unmatched-var id-var)
                                          (where/match-iri (iri/decode-sid db (:id result)))
                                          (where/match-sid db-alias (:id result))))
             score-var  (assoc score-var (-> (where/unmatched-var score-var)
                                             (where/match-value (:score result) iri-xsd-float)))
             vector-var (assoc vector-var (-> (where/unmatched-var vector-var)
                                              (where/match-value (:vec result) const/iri-vector)))))
         search-results)))

(defn dot-product-score
  [v f]
  (when-let [score (vector.score/dotproduct (flake/o f) v)]
    (format-result f score)))

(defn dot-product-rank
  [db solution error-ch out-ch]
  (go
    (let [search-params (get-search-params solution)]
      (->> (<! (rank db search-params dot-product-score reverse-result-sort error-ch))
           (process-results db solution search-params)
           (async/onto-chan! out-ch)))))

(defrecord DotProductGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (go (match-search-triple solution triple)))

  (-finalize [_ error-ch solution-ch]
    (let [out-ch (async/chan 1 (map clear-search-params))]
      (async/pipeline-async 2
                            out-ch
                            (fn [solution ch]
                              (dot-product-rank db solution error-ch ch))
                            solution-ch)
      out-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn dot-product-graph
  [db]
  (->DotProductGraph db))

(defn cosine-score
  [v f]
  (when-let [score (vector.score/cosine-similarity (flake/o f) v)]
    (format-result f score)))

(defn cosine-rank
  [db solution error-ch out-ch]
  (go
    (let [search-params (get-search-params solution)]
      (->> (<! (rank db search-params cosine-score reverse-result-sort error-ch))
           (process-results db solution search-params)
           (async/onto-chan! out-ch)))))

(defrecord CosineGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (go (match-search-triple solution triple)))

  (-finalize [_ error-ch solution-ch]
    (let [out-ch (async/chan 1 (map clear-search-params))]
      (async/pipeline-async 2
                            out-ch
                            (fn [solution ch]
                              (cosine-rank db solution error-ch ch))
                            solution-ch)
      out-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn cosine-graph
  [db]
  (->CosineGraph db))

(defn euclidean-score
  [v f]
  (when-let [score (vector.score/euclidian-distance (flake/o f) v)]
    (format-result f score)))

(defn euclidean-rank
  [db solution error-ch out-ch]
  (go
    (let [search-params (get-search-params solution)]
      (->> (<! (rank db search-params euclidean-score result-sort error-ch))
           (process-results db solution search-params)
           (async/onto-chan! out-ch)))))

(defrecord EuclideanGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (go (match-search-triple solution triple)))

  (-finalize [_ error-ch solution-ch]
    (let [out-ch (async/chan 1 (map clear-search-params))]
      (async/pipeline-async 2
                            out-ch
                            (fn [solution ch]
                              (euclidean-rank db solution error-ch ch))
                            solution-ch)
      out-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn euclidean-graph
  [db]
  (->EuclideanGraph db))

(defn extract-metric
  "Takes the graph alias as a string and extracts the metric name from the
  end of the IRI"
  [graph-alias]
  (some-> (re-find flatrank-vg-re graph-alias)
          second
          str/lower-case
          keyword))

(defn index-graph
  [db graph-alias]
  (let [metric (extract-metric graph-alias)]
    (cond
      (= metric :cosine)
      (cosine-graph db)

      (= metric :dotproduct)
      (dot-product-graph db)

      (= metric :distance)
      (euclidean-graph db))))
