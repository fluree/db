(ns fluree.db.query.subquery-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
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
                                              ["subquery" {"select" ["?s" "?age"]
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
                                              ["subquery" {"select" ["?favNums"]
                                                           "where"  {"ex:favNums" "?favNums"}}]]
                                  "orderBy"  ["?age" "?favNums"]})))

        (testing "and shorten results with subquery 'limit'"
          (is (= [[13 5] [13 7] [34 5] [34 7] [50 5] [50 5] [50 7] [50 7]]
                 @(fluree/query db {"@context" {"schema" "http://schema.org/"
                                                "ex"     "http://example.org/ns/"}
                                    "select"   ["?age" "?favNums"]
                                    "where"    [{"schema:age" "?age"}
                                                ["subquery" {"select" ["?favNums"]
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
                                                  ["subquery" {"selectDistinct" ["?age"]
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
                              [:subquery {:where  '{:ex/favNums ?favN}
                                          :select '[(as (avg ?favN) ?avgFavNum)]}]]
                   :order-by '[?iri ?favNums]}]
          (is (= [[:ex/alice 42] [:ex/alice 76] [:ex/liam 42]]
                 @(fluree/query db qry))))))))

