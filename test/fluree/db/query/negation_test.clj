(ns fluree.db.query.negation-test
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.json-ld.api :as fluree]
             [fluree.db.test-utils :as test-utils]))

(deftest minus
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "negation-test")
        context ["https://flur.ee"
                 test-utils/default-str-context
                 {"ex" "http://example.com/"}]
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert" [{"@id" "ex:alice"
                                               "@type" "ex:Person"
                                               "ex:nickname" "Ali"
                                               "ex:givenName" "Alice"
                                               "ex:familyName" "Smith"}
                                              {"@id" "ex:bob"
                                               "ex:givenName" "Bob"
                                               "ex:familyName" "Jones"}
                                              {"@id" "ex:carol"
                                               "ex:givenName" "Carol"
                                               "ex:familyName" "Smith"}]})]
    (testing "not-exists"
      (is (= [["ex:alice"]]
             @(fluree/query db1 {"@context" context
                                 "select" ["?person"]
                                 "where" [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                          ["not-exists" [{"@id" "?person" "ex:name" "?name"}]]]})))
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select" ["?person"]
                                 "where" [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                          ["not-exists" [{"@id" "?person" "ex:givenName" "?name"}]]]}))))
    (testing "exists"
      (is (= [["ex:alice"]]
             @(fluree/query db1 {"@context" context
                                 "select" ["?person"]
                                 "where" [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                          ["exists" [{"@id" "?person" "ex:givenName" "?name"}]]]})))
      (is (= []
             @(fluree/query db1 {"@context" context
                                 "select" ["?person"]
                                 "where" [{"@id" "?person" "@type" {"@id" "ex:Person"}}
                                          ["exists" [{"@id" "?person" "ex:name" "?name"}]]]}))))))
