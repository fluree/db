(ns fluree.db.virtual-graph.bm25.storage)

#?(:clj (set! *warn-on-reflection* true))

(defn compare-term-indexes
  [terms x y]
  (compare (get-in terms [x :idx])
           (get-in terms [y :idx])))

(defn serialize-terms
  [terms]
  (->> terms
       (into (sorted-map-by (partial compare-term-indexes terms)))
       keys))
