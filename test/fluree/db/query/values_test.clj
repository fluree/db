(ns fluree.db.query.values-test
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.api :as fluree]
             [fluree.db.test-utils :as test-utils]))

(deftest values
  (let [conn    @(fluree/connect-memory)

        context ["https://flur.ee"
                 test-utils/default-str-context
                 {"ex" "http://example.com/"}]
        db1     @(fluree/create-with-txn conn
                                         {"@context" context
                                          "ledger" "values-test"
                                          "insert" (into test-utils/people-strings
                                                         [{"@id" "ex:nikola"
                                                           "schema:name" "Nikola"
                                                           "ex:greeting" [{"@value" "Здраво" "@language" "sb"}
                                                                          {"@value" "Hello" "@language" "en"}]
                                                           "ex:birthday" {"@value" "2000-01-01"
                                                                          "@type" "xsd:datetime"}
                                                           "ex:cool" true}])})]
    (testing "top-level clause"
      (testing "no where clause"
        (testing "multiple vars"
          (is (= [["foo1" "bar1"] ["foo2" "bar2"] ["foo3" "bar3"]]
                 @(fluree/query db1 {"select" ["?foo" "?bar"]
                                     "values" [["?foo" "?bar"]
                                               [["foo1" "bar1"]
                                                ["foo2" "bar2"]
                                                ["foo3" "bar3"]]]}))
              "syntactic form is parsed correctly"))
        (testing "single var"
          (is (= [["foo1"] ["foo2"] ["foo3"]]
                 @(fluree/query db1 {"select" ["?foo"]
                                     "values" ["?foo" ["foo1" "foo2" "foo3"]]}))
              "syntactic form is parsed correctly")))
      (testing "iri values"
        (is (= [["Brian" "brian@example.org"]
                ["Cam" "cam@example.org"]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?email"]
                                   "values" ["?s" [{"@type" "@id" "@value" "ex:brian"}
                                                   {"@type" "@id" "@value" "ex:cam"}]]
                                   "where" [{"@id" "?s" "schema:name" "?name"}
                                            {"@id" "?s" "schema:email" "?email"}]}))
            "id-maps can be used to distinguish iris")
        (is (= [["Brian" "brian@example.org"]
                ["Cam" "cam@example.org"]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?email"]
                                   "values" ["?s" [{"@value" "ex:brian"
                                                    "@type" "@id"}
                                                   {"@value" "ex:cam"
                                                    "@type" "@id"}]]
                                   "where" [{"@id" "?s" "schema:name" "?name"}
                                            {"@id" "?s" "schema:email" "?email"}]}))
            "id-maps can be used to distinguish iris")
        (testing "equivalent syntactic forms"
          (is (= [["ex:cam"] ["ex:liam"]]
                 @(fluree/query db1 {"@context" context
                                     "where" [{"@id" "?s" "ex:friend" {"@id" "ex:alice"}}]
                                     "select" ["?s"]}))
              "iri literal")
          (is (= [["ex:cam"] ["ex:liam"]]
                 @(fluree/query db1 {"@context" context
                                     "values" ["?friend" [{"@value" "ex:alice" "@type" "@id"}]]
                                     "where" [{"@id" "?s" "ex:friend" "?friend"}]
                                     "select" ["?s"]}))
              "variable")
          (is (= [["ex:cam"] ["ex:liam"]]
                 @(fluree/query db1 {"@context" context
                                     "values" ["?friend" [{"@value" "ex:alice" "@type" "@id"}]]
                                     "where" [{"@id" "?s" "ex:friend" {"@id" "?friend"}}]
                                     "select" ["?s"]}))
              "variable in id-map")))
      (testing "matching id only"
        (is (= ["ex:brian"]
               @(fluree/query db1 {"@context" context
                                   "values" ["?s" [{"@type" "@id" "@value" "ex:brian"}]]
                                   "where" [{"@id" "?s"}]
                                   "select" "?s"})))))
    (testing "where pattern"
      (testing "single var"
        (is (= [["Brian" "brian@example.org"]
                ["Cam" "cam@example.org"]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?email"]
                                   "where" [{"@id" "?s" "schema:name" "?name"}
                                            {"@id" "?s" "schema:email" "?email"}
                                            ["values"
                                             ["?s" [{"@type" "id" "@value" "ex:cam"}
                                                    {"@type" "id" "@value" "ex:brian"}]]]]}))
            "syntactic form is parsed correctly"))
      (testing "multiple vars"
        (is (= [["Brian" "brian@example.org"]
                ["Cam" "cam@example.org"]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?email"]
                                   "where" [{"@id" "?s" "schema:name" "?name"}
                                            {"@id" "?s" "schema:email" "?email"}
                                            ["values"
                                             [["?s"] [[{"@type" "id" "@value" "ex:cam"}]
                                                      [{"@type" "@id" "@value" "ex:brian"}]]]]]}))
            "syntactic form is parsed correctly"))
      (testing "nested under optional clause"
        (is (= [["Nikola" nil true]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?age" "?cool"]
                                   "where" [["optional"
                                             [{"@id" "?s"
                                               "schema:name" "?name"
                                               "ex:cool" "?cool"}
                                              ["values"
                                               ["?s" [{"@type" "id" "@value" "ex:nikola"}]]]]]]}))
            "syntactic form is parsed correctly"))
      (testing "federated"
        (let [db3 @(fluree/create-with-txn conn
                                           {"@context" context
                                            "ledger" "other-ledger"
                                            "insert" [{"@id" "ex:khris"
                                                       "schema:name" "Khris"}]})]
          (is (= [["Khris"] ["Nikola"]]
                 @(fluree/query-connection conn
                                           {"@context" context
                                            :from ["values-test" "other-ledger"]
                                            "select" ["?name"]
                                            "where" [{"@id" "?s" "schema:name" "?name"}
                                                     ["values"
                                                      ["?s" [{"@type" "id" "@value" "ex:nikola"}
                                                             {"@type" "id" "@value" "ex:khris"}]]]]
                                            ;; federated queries async/merge solutions from different
                                            ;; graphs nondeterministically
                                            "orderBy" "?name"}))
              "constrains across multiple ledgers")))
      (testing "match meta"
        (is (= ["ex:nikola"]
               @(fluree/query db1 {"@context" context
                                   "select" "?s"
                                   "where" [{"@id" "?s" "ex:greeting" "?greet"}
                                            ["values"
                                             ["?greet" [{"@value" "Здраво" "@language" "sb"}]]]]}))
            "language tag"))
      (testing "with empty solution"
        (is (= [["ex:liam" "Liam"] ["ex:cam" "Cam"]]
               @(fluree/query db1 {"@context" context
                                   "where" [["values" ["?name" ["Liam" "Cam"]]]
                                            {"@id" "?s" "schema:name" "?name"}]
                                   "select" ["?s" "?name"]})))))))
