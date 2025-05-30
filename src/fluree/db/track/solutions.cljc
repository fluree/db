(ns fluree.db.track.solutions)

(defn init
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
  (deref explain))
