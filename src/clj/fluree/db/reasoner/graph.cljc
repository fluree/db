(ns fluree.db.reasoner.graph
  (:require [clojure.set :as set :refer [difference union intersection]]))

#?(:clj (set! *warn-on-reflection* true))


(defn rule-deps
  "Supports rules-deps, but analyzes a single rule"
  [rule-id rules]
  (let [rule-deps (-> (get rules rule-id) :deps)]
    (reduce-kv
      (fn [acc rule-id' {:keys [gens]}]
        (if (seq (intersection rule-deps gens))
          (update-in acc [rule-id :rule-deps] conj rule-id')
          acc))
      rules rules)))

(defn add-rule-dependencies
  "For a map of rules in the appropriate format,
  returns a map of rules with a :rule-deps key added which indicates
  which rules are dependent on other rules"
  [rules]
  (reduce
    (fn [acc rule-id]
      (rule-deps rule-id acc))
    rules (keys rules)))

(defn remove-non-dependent-rules
  [rules]
  (filter #(seq (get (val %) :rule-deps)) rules))


(defn keep-last-run-matches
  [result-summary rules]
  (filter
    (fn [[_ {:keys [deps]}]]
      (some result-summary deps))
    rules))


(defn task-queue
  "Creates an ordered queue based on the number of dependencies each rule has on other rules.

  Rules must first be run through 'add-rule-dependencies' to add a :rule-deps key to each rule.

  result-summary is a function where each rule's :deps key is passed in, and if new data
   was generated that matches the deps, then the rule should be re-run."
  ([rules]
   (->> rules
        (sort-by #(count (get (val %) :rule-deps)))
        (map key)))
  ([rules result-summary]
   (->> rules
        remove-non-dependent-rules ;; any rule that has run and has no dependencies can be removed
        (keep-last-run-matches result-summary)
        (task-queue rules)))) ;; sort based on dependencies

