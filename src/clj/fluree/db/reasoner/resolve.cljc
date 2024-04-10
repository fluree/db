(ns fluree.db.reasoner.resolve
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.exec.where :as exec-where]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.fql :as fql]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

;; namespace to find all rules that should be enforced


(defn result-summary
  "Generates a result summary appropriate for 'schedule' function.

  Input is the original rule set, and a list/set of newly generated
  flakes from the last rules run."
  [rules post-flakes]
  :TODO)

(defn extract-pattern*
  [patterns]
  (reduce
    (fn [acc triple-pattern]
      (conj acc
            (mapv
              (fn [individual]
                (if (contains? individual ::exec-where/var)
                  nil
                  (::exec-where/iri individual)))
              triple-pattern)))
    #{}
    patterns))

(defn extract-pattern
  [rule-statement context]
  (let [patterns (-> (parse/parse-where-clause rule-statement nil context)
                     ::exec-where/patterns)]
    (extract-pattern* patterns)))

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
  (let [context         (some-> (get rule "@context")
                                json-ld/parse-context)
        where           (get rule "where")
        insert          (get rule "insert")
        rule-parsed (q-parse/parse-txn  {const/iri-where  [{:value where}]
                                         const/iri-insert [{:value insert}]} context)
        where-patterns  (extract-pattern* (::exec-where/patterns (:where rule-parsed)))
        insert-patterns (extract-pattern* (:insert rule-parsed))]
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
  [db rules]
  (reduce
    (fn [acc [rule-id rule]]
      (assoc acc rule-id (rule-graph rule)))
    {}
    rules))

(defn find-rules
  "Returns core async channel with rules query result"
  [db]
  (fql/query db nil
             {:select ["?s" "?rule"]
              :where  {"@id"                           "?s",
                       "http://flur.ee/ns/ledger#rule" "?rule"}}))
