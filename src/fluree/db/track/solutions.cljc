(ns fluree.db.track.solutions)

(defn init
  "Map of `<pattern>->{:in <count> :out <count>}`, where `pattern` is a where-clause pattern,
  `:in` is the number of solutions the pattern took as input and `:out` is the number of
  solutions the pattern produced. `:patterns` is the order the patterns were evaluated."
  []
  (atom {:patterns []}))

(defn pattern-in!
  "Increment :in counter for pattern."
  [tracker pattern]
  (swap! tracker
         (fn [explain]
           (cond-> (update explain pattern (fnil #(update % :in inc) {:in 0 :out 0})) ; increment :in counter
             ;; if pattern isn't tracked yet, add it to :patterns sequence
             (not (get explain pattern)) (update :patterns conj pattern)))))

(defn pattern-out!
  "Increment :out counter for pattern."
  [tracker pattern]
  (swap! tracker update-in [pattern :out] inc))

(defn tally
  [explain]
  (update-keys @explain (fn [pattern] (:orig (meta pattern)))))
