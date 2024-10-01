(ns fluree.db.query.subquery-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

(deftest ^:integration subquery-basics
  (testing "Basic subquery"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]

      (testing "binding an IRI in the select"
        (is (= [["Alice" 50] ["Brian" 50] ["Cam" 34] ["Liam" 13]]
               @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                              "ex"     "http://example.org/ns/"}
                                  "select"   ["?name" "?age"]
                                  "where"    [{"@id"         "?s"
                                               "schema:name" "?name"}
                                              ["query" {"select" ["?s" "?age"]
                                                        "where"  {"@id"        "?s"
                                                                  "schema:age" "?age"}}]]
                                  "orderBy"  "?name"}))))

      (testing "with unrelated vars in subquery expand to all parent vals"
        (is (= [[13 5] [13 7] [13 9] [13 10] [13 11] [13 42] [13 42] [13 76] [34 5] [34 7] [34 9]
                [34 10] [34 11] [34 42] [34 42] [34 76] [50 5] [50 5] [50 7] [50 7] [50 9] [50 9]
                [50 10] [50 10] [50 11] [50 11] [50 42] [50 42] [50 42] [50 42] [50 76] [50 76]]
               @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                              "ex"     "http://example.org/ns/"}
                                  "select"   ["?age" "?favNums"]
                                  "where"    [{"schema:age" "?age"}
                                              ["query" {"select" ["?favNums"]
                                                        "where"  {"ex:favNums" "?favNums"}}]]
                                  "orderBy"  ["?age" "?favNums"]})))

        (testing "and shorten results with subquery 'limit'"
          (is (= [[13 5] [13 7] [34 5] [34 7] [50 5] [50 5] [50 7] [50 7]]
                 @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                                "ex"     "http://example.org/ns/"}
                                    "select"   ["?age" "?favNums"]
                                    "where"    [{"schema:age" "?age"}
                                                ["query" {"select" ["?favNums"]
                                                          "where"  {"ex:favNums" "?favNums"}
                                                          "limit"  2}]]
                                    "orderBy"  ["?age" "?favNums"]})))

          (testing "and obeys selectDistinct in subquery"
            (is (= [[13 5] [13 7] [13 9] [13 10] [13 11] [13 42] [13 42] [13 76]
                    [34 5] [34 7] [34 9] [34 10] [34 11] [34 42] [34 42] [34 76]
                    [50 5] [50 7] [50 9] [50 10] [50 11] [50 42] [50 42] [50 76]]
                   @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                                  "ex"     "http://example.org/ns/"}
                                      "select"   ["?age" "?favNums"]
                                      "where"    [{"ex:favNums" "?favNums"}
                                                  ["query" {"selectDistinct" ["?age"]
                                                            "where"          {"schema:age" "?age"}}]]
                                      "orderBy"  ["?age" "?favNums"]})))))))))

(deftest ^:integration subquery-aggregate-as
  (testing "Subquery with an 'as' aggregate in select clause"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]

      (testing " calculate average in subquery and use it in parent query as filter"
        (let [qry {:context  [test-utils/default-context
                              {:ex "http://example.org/ns/"}]
                   :select   '[?iri ?favNums]
                   :where    ['{:id         ?iri
                                :ex/favNums ?favNums}
                              [:filter "(> ?favNums ?avgFavNum)"]
                              [:query {:where  '{:ex/favNums ?favN}
                                       :select '[(as (avg ?favN) ?avgFavNum)]}]]
                   :order-by '[?iri ?favNums]}]
          (is (= [[:ex/alice 42] [:ex/alice 76] [:ex/liam 42]]
                 @(fluree/query db qry))))))))

(deftest ^:integration multiple-subqueries
  (testing "More than one subquery"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]

      (testing "in parallel gets all values"
        (is (= [[13 5] [13 7] [13 9] [13 10] [13 11] [13 42] [13 42] [13 76] [34 5] [34 7] [34 9]
                [34 10] [34 11] [34 42] [34 42] [34 76] [50 5] [50 5] [50 7] [50 7] [50 9] [50 9]
                [50 10] [50 10] [50 11] [50 11] [50 42] [50 42] [50 42] [50 42] [50 76] [50 76]]
               @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                              "ex"     "http://example.org/ns/"}
                                  "select"   ["?age" "?favNums"]
                                  "where"    [["query" {"select" ["?age"]
                                                        "where"  {"schema:age" "?age"}}]
                                              ["query" {"select" ["?favNums"]
                                                        "where"  {"ex:favNums" "?favNums"}}]]
                                  "orderBy"  ["?age" "?favNums"]}))))

      (testing "with nested subqueries"
        (is (= [["Alice" "alice@example.org" 13] ["Alice" "alice@example.org" 34] ["Alice" "alice@example.org" 50]
                ["Alice" "brian@example.org" 13] ["Alice" "brian@example.org" 34] ["Alice" "brian@example.org" 50]
                ["Alice" "cam@example.org" 13] ["Alice" "cam@example.org" 34] ["Alice" "cam@example.org" 50]
                ["Alice" "liam@example.org" 13] ["Alice" "liam@example.org" 34] ["Alice" "liam@example.org" 50]
                ["Brian" "alice@example.org" 13] ["Brian" "alice@example.org" 34] ["Brian" "alice@example.org" 50]
                ["Brian" "brian@example.org" 13] ["Brian" "brian@example.org" 34] ["Brian" "brian@example.org" 50]
                ["Brian" "cam@example.org" 13] ["Brian" "cam@example.org" 34] ["Brian" "cam@example.org" 50]
                ["Brian" "liam@example.org" 13] ["Brian" "liam@example.org" 34] ["Brian" "liam@example.org" 50]
                ["Cam" "alice@example.org" 13] ["Cam" "alice@example.org" 34] ["Cam" "alice@example.org" 50]
                ["Cam" "brian@example.org" 13] ["Cam" "brian@example.org" 34] ["Cam" "brian@example.org" 50]
                ["Cam" "cam@example.org" 13] ["Cam" "cam@example.org" 34] ["Cam" "cam@example.org" 50]
                ["Cam" "liam@example.org" 13] ["Cam" "liam@example.org" 34] ["Cam" "liam@example.org" 50]
                ["Liam" "alice@example.org" 13] ["Liam" "alice@example.org" 34] ["Liam" "alice@example.org" 50]
                ["Liam" "brian@example.org" 13] ["Liam" "brian@example.org" 34] ["Liam" "brian@example.org" 50]
                ["Liam" "cam@example.org" 13] ["Liam" "cam@example.org" 34] ["Liam" "cam@example.org" 50]
                ["Liam" "liam@example.org" 13] ["Liam" "liam@example.org" 34] ["Liam" "liam@example.org" 50]]
               @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                              "ex"     "http://example.org/ns/"}
                                  "select"   ["?name" "?email" "?age"]
                                  "where"    [{"schema:name" "?name"}
                                              ["query" {"selectDistinct" ["?age" "?email"]
                                                        "where"          [{"schema:age" "?age"}
                                                                          ["query" {"select" ["?email"]
                                                                                    "where"  {"schema:email" "?email"}}]]}]]
                                  "orderBy"  ["?name" "?email" "?age"]})))))))

(deftest ^:integration subquery-unions
  (testing "Subquery within a union statement"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]

      (testing "calculate average in subquery and use it in parent query as filter"
        (let [qry {:context  [test-utils/default-context
                              {:ex "http://example.org/ns/"}]
                   :select   '[?person ?avgFavNum]
                   :where    [[:union
                               [:query {:where  '{:id :ex/alice :ex/favNums ?favN}
                                        :select '[(as (str "Alice") ?person) (as (avg ?favN) ?avgFavNum)]}]
                               [:query {:where  '{:id :ex/cam :ex/favNums ?favN}
                                        :select '[(as (str "Cam") ?person) (as (avg ?favN) ?avgFavNum)]}]]]
                   :order-by '[?iri ?favNums]}]
          (is (= [["Alice" 42.33333333333333] ["Cam" 7.5]]
                 @(fluree/query db qry))))))))
