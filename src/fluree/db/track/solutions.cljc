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
         (fn [{:keys [in out]}]
           (if (nil? in)
             {:in 1 :out 0} ; initialize pattern counters
             {:in (inc in) :out out}))))

(defn pattern-out!
  [explain pattern]
  (swap! explain update pattern (fn [{:keys [in out]}] {:in in :out (inc out)})))

(defn tally
  [explain]
  (update-keys @explain (fn [pattern] (:orig (meta pattern)))))
