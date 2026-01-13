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

(defn multi-triple-node-pattern?
  "When a 'node' pattern has more than two entries and gets parsed to multiple triple patterns."
  [orig]
  (and (map? orig) ; node pattern
       (> (count orig) 2))) ; more than two entries => more than one triple pattern

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
        id-key  (reduce-kv (fn [_ k _] (when (= (json-ld/expand-iri k context) const/iri-id)
                                         (reduced k))) nil orig)

        p-sym-var  (::where/var p)
        p-str-var  (str p-sym-var)
        orig-p-iri (some-> (::where/iri p)
                           (json-ld/compact context))]
    ;; p-sym-var, p-str-var, orig-p-iri are mutually exclusive keys, only one will be present
    ;; we cannot know whether the original var is a symbol or a str, so we just try both
    (select-keys orig [id-key p-sym-var p-str-var orig-p-iri])))

(defn display-pattern
  "Replace parsed patterns with the user's original syntax."
  [pattern]
  (let [{:keys [orig context]} (meta pattern)]
    (if (multi-triple-node-pattern? orig)
      (display-triple-pattern pattern context orig)
      orig)))

(defn tally
  "Format the explanation as a vector of [<pattern> <counters>] tuples in execution order."
  [tracker]
  (let [{:keys [patterns] :as explain} @tracker]
    (reduce (fn [explanation pattern]
              (conj explanation [(display-pattern pattern) (-> (get explain pattern)
                                                               ;; display vars in order of insertion for readability
                                                               (update :binds-in #(vec (sort-by (comp :ord meta) %)))
                                                               (update :binds-out #(vec (sort-by (comp :ord meta) %))))]))
            []
            patterns)))
