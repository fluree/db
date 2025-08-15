(ns fluree.db.explain-test
  (:require  [clojure.test :refer [deftest is testing]]
             [fluree.db.api :as fluree]))

(defn gen-subj
  [i]
  {"@id" (str "ex:" i)
   "ex:foo" "foo"
   "ex:i" i})

(deftest explain-test
  (let [conn @(fluree/connect-memory)
        db0  @(fluree/create conn "test-explain")
        db1  @(fluree/update db0 {"@context" {"ex" "http://example.com/"}
                                  "insert"
                                  (mapv (fn [i]
                                          (cond-> (gen-subj i)
                                            (odd? i) (assoc "ex:ref1"
                                                            (cond-> (gen-subj (+ 1000 i))
                                                              (#{0} (mod i 3))
                                                              (assoc "ex:ref2" (gen-subj (+ 2000 i))
                                                                     "ex:ref3" {"@id" (str "ex:" i)})))
                                            (even? i) (assoc
                                                       "ex:bar" "bar"
                                                       "ex:num" [i (inc i) (dec i)])))
                                        (range 1 100 3))})]
    (testing "query with triple and non-triple patterns"
      (is (= '[[{"@id" "?s", "ex:ref1" "?ref1"}
                {:in 48, :out 0, :binds-in [?s ?num], :binds-out []}]
               [{"@id" "?s", "ex:num" "?num"}
                {:in 1, :out 48, :binds-in [], :binds-out [?s ?num]}]
               [[:optional {"@id" "?s", "ex:ref1" "?ref1"}]
                {:in 48, :out 48, :binds-in [?s ?num], :binds-out [?s ?num ?ref1]}]
               [[:union {"@id" "?s", "ex:foo" "?str"} {"@id" "?s", "ex:bar" "?str"}]
                {:in        48,
                 :out       96,
                 :binds-in  [?s ?num ?ref1],
                 :binds-out [?s ?num ?ref1 ?str]}]
               [{"@id" "?s", "ex:bar" "?str"}
                {:in        48,
                 :out       48,
                 :binds-in  [?s ?num ?ref1],
                 :binds-out [?s ?num ?ref1 ?str]}]
               [{"@id" "?s", "ex:foo" "?str"}
                {:in        48,
                 :out       48,
                 :binds-in  [?s ?num ?ref1],
                 :binds-out [?s ?num ?ref1 ?str]}]
               [[:filter "(> 50 ?num)"]
                {:in        96,
                 :out       48,
                 :binds-in  [?s ?num ?ref1 ?str],
                 :binds-out [?s ?num ?ref1 ?str]}]]
             (->> @(fluree/explain db1 {"@context" {"ex" "http://example.com/"}
                                        "where"    [{"@id" "?s" "ex:num" "?num"}
                                                    ["optional"
                                                     {"@id" "?s" "ex:ref1" "?ref1"}]
                                                    ["union"
                                                     {"@id" "?s" "ex:foo" "?str"}
                                                     {"@id" "?s" "ex:bar" "?str"}]
                                                    ["filter" "(> 50 ?num)"]]
                                        "select"   ["?s" "?ref1" "?str"]})
                  :explain
                  ;; execution order of the union clauses is nondeterministic, make them deterministic
                  (sort-by identity
                           (fn [[pattern-a stats-a] [pattern-b stats-b]]
                             (cond
                               ;; ex:bar always first
                               (= [{"@id" "?s", "ex:bar" "?str"}
                                   {"@id" "?s", "ex:foo" "?str"}]
                                  [pattern-a pattern-b])
                               -1
                               ;; ex:bar always first
                               (= [{"@id" "?s", "ex:foo" "?str"}
                                   {"@id" "?s", "ex:bar" "?str"}]
                                  [pattern-a pattern-b])
                               1
                               ;; order by number of :binds-out vars
                               :else
                               (compare (-> stats-a :binds-out count)
                                        (-> stats-b :binds-out count)))))))))
    (testing "result mapping equivalent where patterns"
      (testing "multi-triple node pattern"
        (testing "with string context"
          (is (= '[[{"@id" "?s", "ex:bar" "?bar"}
                    {:in 1, :out 16, :binds-in [], :binds-out [?s ?bar]}]
                   [{"@id" "?s", "ex:num" "?num"}
                    {:in 16, :out 48, :binds-in [?s ?bar], :binds-out [?s ?bar ?num]}]
                   [{"@id" "?s", "?p" "?o"}
                    {:in        48,
                     :out       288,
                     :binds-in  [?s ?bar ?num],
                     :binds-out [?s ?bar ?num ?p ?o]}]]
                 (:explain @(fluree/explain db1 {"@context" {"ex" "http://example.com/"}
                                                 ;; single node pattern
                                                 "where"    [{"@id"    "?s"
                                                              "ex:bar" "?bar"
                                                              "ex:num" "?num"
                                                              "?p"     "?o"}]
                                                 "select"   ["?s" "?bar" "?num"]})))))
        (testing "with keyword context"
          (is (= '[[{:id "?s", :ex/bar "?bar"}
                    {:in 1, :out 16, :binds-in [], :binds-out [?s ?bar]}]
                   [{:id "?s", :ex/num "?num"}
                    {:in 16, :out 48, :binds-in [?s ?bar], :binds-out [?s ?bar ?num]}]
                   [{:id "?s", ?p ?o}
                    {:in 48, :out 288, :binds-in [?s ?bar ?num], :binds-out [?s ?bar ?num ?p ?o]}]]
                 (:explain @(fluree/explain db1 {:context {:id "@id" :ex "http://example.com/"}
                                                 ;; single node pattern
                                                 :where   [{:id     "?s"
                                                            :ex/bar "?bar"
                                                            :ex/num "?num"
                                                            '?p     '?o}]
                                                 :select  ["?s" "?bar" "?num"]}))))))
      (testing "single-triple per node pattern"
        (is (= '[[{"@id" "?s", "ex:bar" "?bar"}
                  {:in 1, :out 16, :binds-in [], :binds-out [?s ?bar]}]
                 [{"@id" "?s", "ex:num" "?num"}
                  {:in 16, :out 48, :binds-in [?s ?bar], :binds-out [?s ?bar ?num]}]]
               (:explain @(fluree/explain db1 {"@context" {"ex" "http://example.com/"}
                                               ;; two node patterns
                                               "where"    [{"@id" "?s" "ex:bar" "?bar"}
                                                           {"@id" "?s" "ex:num" "?num"}]
                                               "select"   ["?s" "?bar" "?num"]}))))))))
