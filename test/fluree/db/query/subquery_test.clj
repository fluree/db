(ns fluree.db.query.subquery-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

(deftest ^:integration subquery-basics
  (testing "simple subquery"

    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)
          qry    {"@context" {"schema" "http://schema.org/"
                              "ex"     "http://example.org/ns/"}
                  "select"   ["?name" "?age"]
                  "where"    [{"@id"         "?s"
                               "schema:name" "?name"}
                              ["subquery" {"select" ["?s" "?favNums"]
                                           "where"  {"@id"        "?s"
                                                     "schema:age" "?age"}}]]
                  "orderBy"  "?name"}]

      (is (= [["Alice" 50] ["Brian" 50] ["Cam" 34] ["Liam" 13]]
             @(fluree/query db qry))))))

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

