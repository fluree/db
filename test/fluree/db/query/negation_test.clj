(ns fluree.db.query.negation-test
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.api :as fluree]
             [fluree.db.test-utils :as test-utils]))

(deftest negation
  (testing "queries with negation"
    (testing "on an existing ledger containing data about people"
      (let [conn    @(fluree/connect {:method :memory})
            ledger  @(fluree/create conn "negation-test")
            context ["https://flur.ee"
                     test-utils/default-str-context
                     {"ex" "http://example.com/"}]
            db0     (fluree/db ledger)
            db1     @(fluree/stage db0 {"@context" context
                                        "insert"   [{"@id"           "ex:alice"
                                                     "@type"         "ex:Person"
                                                     "ex:nickname"   "Ali"
                                                     "ex:givenName"  "Alice"
                                                     "ex:familyName" "Smith"}
                                                    {"@id"           "ex:bob"
                                                     "ex:givenName"  "Bob"
                                                     "ex:familyName" "Jones"}
                                                    {"@id"           "ex:carol"
                                                     "ex:givenName"  "Carol"
                                                     "ex:familyName" "Smith"}]})]
        (testing "checking for existence of a pattern"
          (testing "when the pattern is present in the data"
            (is (= [["ex:alice"]]
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?person"]
                                       "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                                   ["exists" [{"@id" "?person" "ex:givenName" "?name"}]]]}))
                "returns the subject with that pattern"))
          (testing "when the pattern is absent in the data"
            (is (= []
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?person"]
                                       "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                                   ["exists" [{"@id" "?person" "ex:name" "?name"}]]]}))
                "returns no subjects")))


        (testing "checking for the non-existence of a pattern"
          (testing "when the pattern does not exist in the data"
            (is (= [["ex:bob"] ["ex:carol"]]
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?person"]
                                       "where"    [{"@id" "?person" "ex:givenName" "?gname"}
                                                   ["not-exists" [{"@id" "?person" "ex:nickname" "?name"}]]]}))
                "returns only subjects who do not have a nickname"))
          (testing "when the pattern does exist in the data"
            (is (= []
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?person"]
                                       "where"    [{"@id" "?person" "ex:givenName" "?gname"}
                                                   ["not-exists" [{"@id" "?person" "ex:familyName" "?fname"}]]]}))
                "returns no subjects, as every subject has an ex:familyName"))
          (testing "when pattern has all variables"
            (is (= []
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?s" "?p" "?o"]
                                       "where"    [{"@id" "?s" "?p" "?o"}
                                                   ["not-exists" [{"@id" "?x" "?y" "?z"}]]]}))
                "two patterns match the same data, everything is filtered out"))
          (testing "when pattern has all literals"
            (is (= []
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?s" "?p" "?o"]
                                       "where"    [{"@id" "?s" "?p" "?o"}
                                                   ["not-exists" [{"@id" "ex:alice" "type" "ex:Person"}]]]}))
                "[ex:alice type ex:Person] does exist, so all [?s ?p ?o] are filtered out")))
        (testing "checking for the removal of solutions that match a pattern"
          (testing "when the pattern produces a solution that matches existing solutions"
            (is (= ["ex:alice" "ex:carol"]
                   @(fluree/query db1 {"@context"       context
                                       "selectDistinct" "?s"
                                       "where"          [{"@id" "?s" "?p" "?o"}
                                                         ["minus" [{"@id" "?s" "ex:givenName" "Bob"}]]]}))
                "ex:bob is removed from the solution set"))
          (testing "when a pattern of all variables has no common bindings with the existing solutions"
            (is (= [["ex:alice" "type" "ex:Person"]
                    ["ex:alice" "ex:familyName" "Smith"]
                    ["ex:alice" "ex:givenName" "Alice"]
                    ["ex:alice" "ex:nickname" "Ali"]
                    ["ex:bob" "ex:familyName" "Jones"]
                    ["ex:bob" "ex:givenName" "Bob"]
                    ["ex:carol" "ex:familyName" "Smith"]
                    ["ex:carol" "ex:givenName" "Carol"]]
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?s" "?p" "?o"]
                                       "where"    [{"@id" "?s" "?p" "?o"}
                                                   ["minus" [{"@id" "?x" "?y" "?z"}]]]}))
                "nothing is removed from the solution set because there are no variables in common"))
          (testing "when the pattern of all literals has no common bindings with the existing solutions"
            (is (= [["ex:alice" "type" "ex:Person"]
                    ["ex:alice" "ex:familyName" "Smith"]
                    ["ex:alice" "ex:givenName" "Alice"]
                    ["ex:alice" "ex:nickname" "Ali"]
                    ["ex:bob" "ex:familyName" "Jones"]
                    ["ex:bob" "ex:givenName" "Bob"]
                    ["ex:carol" "ex:familyName" "Smith"]
                    ["ex:carol" "ex:givenName" "Carol"]]
                   @(fluree/query db1 {"@context" context
                                       "select"   ["?s" "?p" "?o"]
                                       "where"    [{"@id" "?s" "?p" "?o"}
                                                   ["minus" [{"@id" "ex:alice" "ex:familyName" "Smith"}]]]}))
                "no match of bindings so nothing is removed")))

        (testing "checking the difference between negation methods in inner filters"
          (testing "for an existing ledger with various data"
            (let [db1 @(fluree/stage db0 {"insert" [{"@id" "ex:a"
                                                     "ex:p" 1
                                                     "ex:q" [1 2]}
                                                    {"@id" "ex:b"
                                                     "ex:p" 3.0
                                                     "ex:q" [4.0 5.0]}]})]
              (testing "where the filter is inside a not-exists pattern"
                (is (= [["ex:b" 3.0M]]
                       @(fluree/query db1 {"where" [{"@id" "?x" "ex:p" "?p"}
                                                    ["not-exists" [{"@id" "?x" "ex:q" "?q"}
                                                                   ["filter" "(= ?p ?q)"]]]]
                                           "select" ["?x" "?p"]}))
                    "existing bindings are available for filtering"))
              (testing "where the filter is inside a minus pattern and one result is filtered out"
                (is (= [["ex:a" 1]
                        ["ex:b" 3.0M]]
                       @(fluree/query db1 {"where" [{"@id" "?x" "ex:p" "?p"}
                                                    ["minus" [{"@id" "?x" "ex:q" "?q"}
                                                              ["filter" "(= ?p ?q)"]]]]
                                           "select" ["?x" "?p"]}))
                    "existing bindings are not available for filtering and no results are filtered out")))))))))
