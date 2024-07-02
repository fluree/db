(ns fluree.db.query.negation-test
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.json-ld.api :as fluree]
             [fluree.db.test-utils :as test-utils]))

(deftest negation
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
    (testing "exists"
      (is (= [["ex:alice"]]
             @(fluree/query db1 {"@context" context
                                 "select"   ["?person"]
                                 "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                             ["exists" [{"@id" "?person" "ex:givenName" "?name"}]]]}))
          "returns ex:Person who has an ex:givenName")
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?person"]
                                 "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                             ["exists" [{"@id" "?person" "ex:name" "?name"}]]]}))
          "returns ex:Person who has an ex:name (none)"))


    (testing "not-exists"
      (is (= [["ex:alice"]]
             @(fluree/query db1 {"@context" context
                                 "select"   ["?person"]
                                 "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                             ["not-exists" [{"@id" "?person" "ex:name" "?name"}]]]}))
          "returns ex:Person who does not have an ex:name")
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?person"]
                                 "where"    [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                             ["not-exists" [{"@id" "?person" "ex:givenName" "?name"}]]]}))
          "returns ex:Person who does not have an ex:givenName (none)")
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?s" "?p" "?o"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["not-exists" [{"@id" "?x" "?y" "?z"}]]]}))
          "two patterns match the same data, everything is filtered out")
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?s" "?p" "?o"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["not-exists" [{"@id" "ex:alice" "type" "ex:Person"}]]]}))
          "[ex:alice type ex:Person] does exist, so all [?s ?p ?o] are filtered out"))
    (testing "minus"
      (is (= ["ex:alice" "ex:carol"]
             @(fluree/query db1 {"@context"       context
                                 "selectDistinct" "?s"
                                 "where"          [{"@id" "?s" "?p" "?o"}
                                                   ["minus" [{"@id" "?s" "ex:givenName" "Bob"}]]]}))
          "ex:bob is removed from the solution set")
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
          "nothing is removed from the solution set because there are no variables in common")
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
          "no match of bindings so nothing is removed"))

    (testing "inner filters"
      (let [db1 @(fluree/stage db0 {"insert" [{"@id" "ex:a"
                                               "ex:p" 1
                                               "ex:q" [1 2]}
                                              {"@id" "ex:b"
                                               "ex:p" 3.0
                                               "ex:q" [4.0 5.0]}]})]
        (is (= [["ex:b" 3.0M]]
               @(fluree/query db1 {"where" [{"@id" "?x" "ex:p" "?p"}
                                            ["not-exists" [{"@id" "?x" "ex:q" "?q"}
                                                           ["filter" "(= ?p ?q)"]]]]
                                   "select" ["?x" "?p"]})))
        (is (= [["ex:a" 1]
                ["ex:b" 3.0M]]
               @(fluree/query db1 {"where" [{"@id" "?x" "ex:p" "?p"}
                                            ["minus" [{"@id" "?x" "ex:q" "?q"}
                                                      ["filter" "(= ?p ?q)"]]]]
                                   "select" ["?x" "?p"]})))))))
