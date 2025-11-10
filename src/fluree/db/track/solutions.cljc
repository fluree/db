(ns fluree.db.track.solutions)

(defn init
  "Map of `<pattern>->{:in <count> :out <count>}`, where `pattern` is a where-clause pattern,
  `:in` is the number of solutions the pattern took as input and `:out` is the number of
  solutions the pattern produced. `:patterns` is the order the patterns were evaluated."
  []
  (atom {:patterns []}))

(def initial-stats
  {:in        0
   :out       0
   :binds-in #{}
   :binds-out #{}})

(defn update-in-stats
  [{:keys [binds-in] :as stats} solution]
  (-> stats
      (update :in inc)
      (assoc :binds-in (reduce (fn [binds-in* var]
                                 ;; maintain insert order as metadata
                                 (conj binds-in* (with-meta var {:ord (count binds-in*)})))
                               binds-in
                               (keys solution)))))

(defn pattern-in!
  "Increment :in counter for pattern."
  [tracker pattern solution]
  (swap! tracker
         (fn [explain]
           (cond-> (update explain pattern (fnil update-in-stats initial-stats) solution)
             ;; if pattern isn't tracked yet, add it to :patterns sequence
             (not (get explain pattern)) (update :patterns conj pattern)))))

(defn update-out-stats
  [{:keys [binds-in] :as stats} solution]
  (-> stats
      (update :out inc)
      (assoc :binds-out (reduce (fn [binds-out* var]
                                  ;; maintain insert order as metadata
                                  (conj binds-out* (with-meta var {:ord (count binds-out*)})))
                                binds-in
                                (keys solution)))))

(defn pattern-out!
  "Increment :out counter for pattern."
  [tracker pattern solution]
  (swap! tracker (fn [explain] (update explain pattern update-out-stats solution))))

(defn tally
  [tracker]
  (let [{:keys [patterns] :as explain} @tracker]
    (def explain explain)
    (reduce (fn [explanation pattern]
              (conj explanation (-> (get explain pattern)
                                    (assoc :pattern (pr-str pattern))
                                    (update :binds-in #(vec (sort-by (comp :ord meta) %)))
                                    (update :binds-out #(vec (sort-by (comp :ord meta) %))))))
            []
            patterns)))

(comment
  explain


  )
