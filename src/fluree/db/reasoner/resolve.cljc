(ns fluree.db.reasoner.resolve
  "Functions for finding reasoning rules that should be enforced"
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as exec-where]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn result-summary
  "Generates a result summary appropriate for 'schedule' function.

  Input is the original rule set, and a list/set of newly generated
  flakes from the last rules run."
  [_rules _post-flakes]
  :TODO)

(defn extract-pattern
  [triple-pattern]
  (mapv
   (fn [individual]
     (if (contains? individual ::exec-where/var)
       nil
       (::exec-where/iri individual)))
   triple-pattern))

(defn extract-patterns
  [where-patterns]
  (reduce
   (fn [acc pattern]
     (case (first pattern)
       :class (conj acc (extract-pattern (second pattern)))
       :union (reduce
               (fn [acc* union-pattern]
                 (into acc*
                       (extract-patterns (::exec-where/patterns union-pattern))))
               acc
               (second pattern))
       :optional (into acc (extract-patterns (::exec-where/patterns pattern)))
       :bind acc ;; bind can only use patterns/vars already established, nothing to add
        ;; else
       (conj acc (extract-pattern pattern))))
   #{}
   where-patterns))

(defn flake-tests
  "Generates 'test' flakes to test if any patterns in this rule match
  newly generate flakes (based on post index sorting)"
  [patterns]
  (mapv
   (fn [[s p o]]
     (flake/create s p o nil nil nil nil))
   patterns))

(defn rule-graph
  "Puts rule in a specific graph format"
  [rule]
  (let [context         (get rule "@context")
        where           (get rule "where")
        insert          (get rule "insert")
        rule-parsed     (parse/parse-stage-txn {:context context
                                                :where   where
                                                :insert  insert})
        where-patterns  (extract-patterns (::exec-where/patterns (:where rule-parsed)))
        insert-patterns (extract-patterns (:insert rule-parsed))]
    {:deps        where-patterns
     :gens        insert-patterns
     :flake-tests (flake-tests where-patterns)
     :rule        rule
     :rule-parsed rule-parsed
     :rule-deps   #{}}))

(defn rules->graph
  "Turns rules into a map of rule @id keys and metadata about
  the rule as a value map, which includes which patterns the rule
  relies on (:deps), what patterns the rule generates (:gens),
  empty :rule-deps which will be filled in later by add-rule-dependencies,
  and a list of 'test' flakes (:flake-tests) which will be used to
  test inferred flakes against the post index
  to determine if any of the patterns used by this rule had a match."
  [rules]
  (reduce
   (fn [acc [rule-id rule]]
     (assoc acc rule-id (rule-graph rule)))
   {}
   rules))

(defn extract-owl2rl-from-db
  [db]
  (async/go
    (let [all-rules (async/<! (fql/query db nil
                                         {:select {"?s" ["*"]}
                                          :where  [["union"
                                                    {"@id"   "?s",
                                                     "@type" const/iri-owl:Class}
                                                    {"@id"   "?s",
                                                     "@type" const/iri-owl:ObjectProperty}
                                                    {"@id"                "?s",
                                                     const/iri-owl:sameAs nil}]]
                                          :depth  6}))]
      (if (util/exception? all-rules)
        (do
          (log/error "Error extracting owl2rl from db:" (ex-message all-rules))
          all-rules)
        ;; blank nodes can be part of OWL logic (nested), but we won't assign class or
        ;; property names to blank nodes
        (remove iri/blank-node? all-rules)))))

(defn rules-from-db
  "Returns core async channel with rules query result"
  [db method]
  (case method
    :datalog (fql/query db nil
                        {:select ["?s" "?rule"]
                         :where  {"@id"          "?s",
                                  const/iri-rule "?rule"}})
    :owl2rl (extract-owl2rl-from-db db)))
