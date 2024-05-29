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
                                             ["not-exists" [{"@id" "?x" "?y" "?z"}]]]})))
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?s" "?p" "?o"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["not-exists" [{"@id" "ex:alice" "type" "ex:Person"}]]]}))))
    (testing "minus"
      (is (= ["ex:alice" "ex:carol"]
             @(fluree/query db1 {"@context"       context
                                 "selectDistinct" "?s"
                                 "where"          [{"@id" "?s" "?p" "?o"}
                                                   ["minus" [{"@id" "?s" "ex:givenName" "Bob"}]]]})))
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
                                             ["minus" [{"@id" "?x" "?y" "?z"}]]]})))
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select"   ["?s" "?p" "?o"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["minus" [{"@id" "ex:alice" "ex:familyName" "Smith"}]]]})))
      (is (= [["ex:alice" "type" "ex:Person"]
              ["ex:alice" "ex:givenName" "Alice"]
              ["ex:alice" "ex:nickname" "Ali"]
              ["ex:bob" "ex:familyName" "Jones"]
              ["ex:bob" "ex:givenName" "Bob"]
              ["ex:carol" "ex:familyName" "Smith"]
              ["ex:carol" "ex:givenName" "Carol"]]
             @(fluree/query db1 {"@context" context
                                 "select"   ["?s" "?p" "?o"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["minus" [{"@id" "ex:alice" "ex:familyName" "Smith"}]]]}))))))
