(ns fluree.db.vector.flat-rank
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.core.async :as async :refer [>! go]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.vector.scoring :refer [dot-product cosine-similarity euclidian-distance]]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]))

(def iri-search (str iri/f-idx-ns "target"))
(def iri-property (str iri/f-idx-ns "property"))
(def iri-limit (str iri/f-idx-ns "limit"))
(def iri-id (str iri/f-idx-ns "id"))
(def iri-score (str iri/f-idx-ns "score"))
(def iri-result (str iri/f-idx-ns "result"))
(def iri-vector (str iri/f-idx-ns "vector"))
(def iri-xsd-float "http://www.w3.org/2001/XMLSchema#float")

(def flatrank-vg-re (re-pattern "##FlatRank-(.*)"))

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
  (go
    (let [p-iri (prop-iri triple)]
      (cond
        (= iri-search p-iri)
        (assoc-in solution [::flat-rank ::target] (obj-val triple solution))

        (= iri-property p-iri)
        (assoc-in solution [::flat-rank ::property] (obj-iri triple))

        (= iri-limit p-iri)
        (assoc-in solution [::flat-rank ::limit] (obj-val triple solution))

        (= iri-result p-iri)
        (assoc-in solution [::flat-rank ::result ::id] (obj-var triple))

        (= iri-score p-iri)
        (assoc-in solution [::flat-rank ::result ::score] (obj-var triple))

        (= iri-vector p-iri)
        (assoc-in solution [::flat-rank ::result ::vector] (obj-var triple))))))

(defn get-search-params
  [solution]
  (::flat-rank solution))

(defn clear-search-params
  [solution]
  (dissoc solution ::flat-rank))

(defn finalize
  [search-af error-ch solution-ch]
  (let [out-ch (async/chan 1 (map clear-search-params))]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (search-af solution error-ch ch))
                          solution-ch)
    out-ch))

(defn format-result
  [f score]
  {:id    (flake/s f)
   :score score
   :vec   (flake/o f)})

(defn score-flake
  [score-fn v f]
  (when-let [score (score-fn (flake/o f) v)]
    (format-result f score)))

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

(defn limit-results
  [limit results]
  (if limit
    (take limit results)
    results))

(defn search
  [db score-fn sort-fn solution error-ch out-ch]
  (go
    (try*
      (let [{::keys [property target limit] :as search-params}
            (get-search-params solution)

            pid       (iri/encode-iri db property)
            score-opt {:flake-xf (comp (map (partial score-flake score-fn target))
                                       (remove nil?))}
            ;; For now, pulling all matching values from full index once hitting
            ;; the actual vector index, we'll only need to pull matches out of
            ;; novelty (if that)
            vectors   (<? (query-range/index-range db :post = [pid] score-opt))]
        (->> vectors
             (sort sort-fn)
             (limit-results limit)
             (process-results db solution search-params)
             (async/onto-chan! out-ch)))
      (catch* e
        (log/error e "Error ranking vectors")
        (>! error-ch e)))))

(defrecord DotProductFlatRankGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (match-search-triple solution triple))

  (-finalize [_ _ error-ch solution-ch]
    (finalize (partial search db dot-product reverse-result-sort) error-ch solution-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn dot-product-flat-rank-graph
  [db]
  (->DotProductFlatRankGraph db))

(defrecord CosineFlatRankGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (match-search-triple solution triple))

  (-finalize [_ _ error-ch solution-ch]
    (finalize (partial search db cosine-similarity reverse-result-sort) error-ch solution-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn cosine-flat-rank-graph
  [db]
  (->CosineFlatRankGraph db))

(defrecord EuclideanFlatRankGraph [db]
  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (match-search-triple solution triple))

  (-finalize [_ _ error-ch solution-ch]
    (finalize (partial search db euclidian-distance result-sort) error-ch solution-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [_ alias']
    (where/-activate-alias db alias'))

  (-aliases [_]
    (where/-aliases db)))

(defn euclidean-flat-rank-graph
  [db]
  (->EuclideanFlatRankGraph db))

(defn extract-metric
  "Takes the graph alias as a string and extracts the metric name from the
  end of the IRI"
  [graph-alias]
  (some-> (re-find flatrank-vg-re graph-alias)
          second
          ->kebab-case-keyword))

(defn index-graph
  [db graph-alias]
  (let [metric (extract-metric graph-alias)]
    (cond
      (= metric :cosine)
      (cosine-flat-rank-graph db)

      (= metric :dot-product)
      (dot-product-flat-rank-graph db)

      (= metric :distance)
      (euclidean-flat-rank-graph db))))
