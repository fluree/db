(ns fluree.db.virtual-graph.bm25.search
  (:require [fluree.db.virtual-graph.bm25.update :as update]
            [fluree.db.util.log :as log]))

;; TODO - VG - this can be moved to unboxed math for some slight improvement
(defn calc-term-score
  [k1 b avg-doc-len doc-len term-idf term-f]
  (* term-idf
     (/ (* term-f (+ k1 1))
        (+ term-f (* k1 (+ (- 1 b) (* b (/ doc-len
                                           avg-doc-len))))))))

;; TODO - VG - this can be moved to unboxed math for some slight improvement
(defn calc-idf
  [item-count n-instances]
  (Math/log
   (+ 1 (/ (+ (- item-count n-instances) 0.5)
           (+ n-instances 0.5)))))

(defn seek-term
  "Returns nil once doc-vec is exhausted"
  [term doc-vec]
  (loop [doc-vec* doc-vec]
    ;; term-tuple is a two-tuple of [term-idx term-frequency]
    (when-let [term-tuple (first doc-vec*)]
      (let [term-idx (first term-tuple)]
        (cond
          ;; matching term, return rest of doc-vec and term frequency
          (= term term-idx)
          [(rest doc-vec) (second term-tuple)]

          ;; have passed matching term, return vec to that point
          ;; no term-freq as no match
          (> term-idx term)
          [doc-vec*]

          :else
          (recur (rest doc-vec*)))))))

;; TODO - VG - this can be pre-calculated
(defn get-doc-length
  "Return document length for doc-id of corpus"
  [vector]
  (->> vector
       (map second)
       (reduce +)))

(defn calc-doc-score
  [k1 b avg-length query-terms doc-vec]
  (let [doc-len (get-doc-length doc-vec)]
    (loop [[{:keys [idx idf]} & r] query-terms
           doc-vec doc-vec
           score   0.0]
      (if idx
        (let [[doc-vec* term-f] (seek-term idx doc-vec)
              score* (if term-f
                       (+ score
                          (calc-term-score k1 b avg-length doc-len idf term-f))
                       score)]
          (if doc-vec*
            (recur r doc-vec* score*)
            score*))
        score))))

(defn parse-query
  "Based on query text, parses text to remove stopword and
   runs through stemmer, then returns a sequence of maps that include:
  - :idx - sparse vector index number for term
  - :items - @ids of items that contain that term
  - :idf - inverse document frequency of term

  Only returns terms that are in the index."
  [query terms item-count stemmer stopwords]
  (let [q-terms (->> (update/parse-sentence query stemmer stopwords)
                     (distinct))]
    (->> q-terms
         (reduce
          (fn [acc term]
            (if-let [term-match (get terms term)] ;; won't match term if not in index
              (conj acc (assoc term-match :idf (calc-idf item-count (count (:items term-match)))))
              acc))
          [])
         ;; important that we sort by :idx value, as we seek terms in order as we parse through sparse vectors downstream
         (sort-by :idx))))
