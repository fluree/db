(ns fluree.db.track.solutions)

(defn init
  "Map of `<pattern>->[in-count out-count]`, where `pattern` is a where-clause pattern,
  `in-count` is the number of solutions the pattern took as input and `out-count` is the
  number of solutions the pattern produced."
  []
  (atom {}))

(defn pattern-in!
  [explain pattern]
  (swap! explain update pattern
         (fn [[in out]]
           (if (nil? in)
             [1 0] ; initialize pattern counters
             [(inc in) out]))))

(defn pattern-out!
  [explain pattern]
  (swap! explain update pattern (fn [[in out]] [in (inc out)])))

(defn tally
  [explain]
  (update-keys @explain (fn [pattern] (:orig (meta pattern)))))
