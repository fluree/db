(ns fluree.db.track.solutions
  (:require [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as-alias where]
            [fluree.json-ld :as json-ld]))

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

(defn multi-pattern-node?
  "When a 'node' pattern has more than two entries and gets parsed to multiple triple patterns."
  [orig]
  (and (map? orig) ; triple pattern
       (> (count orig) 2))) ; more than two entries

(defn display-triple-pattern
  "Users can express a series of triple patterns in a single 'node' pattern.

  e.g. where clause:
  [{:id ?s :ex/foo ?foo :ex/bar ?bar}]
  maps to:
  [ [{::where/var ?s} {::where/iri \"ex:foo\"} {::where/var ?foo}]
    [{::where/var ?s} {::where/iri \"ex:bar\"} {::where/var ?bar}] ]

  By using the supplied context we can map the expanded subject id and predicate iri
  back to the user's corresponding syntax.

  [ [{:id ?s :ex/foo ?foo} <counters>]
    [{:id ?s :ex/bar ?bar} <counters>] ]"
  [pattern context orig]
  (let [[_ p _] pattern
        id-key (reduce-kv (fn [_ k _] (when (= (json-ld/expand-iri k context) const/iri-id)
                                        (reduced k))) nil orig)
        orig-p (or (::where/var p)
                   (-> (::where/iri p)
                       (json-ld/compact context)))]
    (select-keys orig [id-key orig-p])))

(defn display-pattern
  "Replace parsed patterns with the user's original syntax."
  [pattern]
  (let [{:keys [orig context]} (meta pattern)]
    (if (multi-pattern-node? orig)
      (display-triple-pattern pattern context orig)
      orig)))

(defn tally
  "Format the explanation as a vector of [<pattern> <counters>] tuples in execution order."
  [tracker]
  (let [{:keys [patterns] :as explain} @tracker]
    (reduce (fn [explanation pattern]
              (conj explanation [(display-pattern pattern) (get explain pattern)]))
            []
            patterns)))
