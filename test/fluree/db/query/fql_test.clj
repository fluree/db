(ns fluree.db.query.fql-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration grouping-test
  (testing "grouped queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with a single grouped-by field"
        (let [qry     '{:context  {:ex "http://example.org/ns/"}
                        :select   [?name ?email ?age ?favNums]
                        :where    [[?s :schema/name ?name]
                                   [?s :schema/email ?email]
                                   [?s :schema/age ?age]
                                   [?s :ex/favNums ?favNums]]
                        :group-by ?name
                        :order-by ?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice"
                   ["alice@example.org" "alice@example.org" "alice@example.org"]
                   [50 50 50]
                   [9 42 76]]
                  ["Brian" ["brian@example.org"] [50] [7]]
                  ["Cam" ["cam@example.org" "cam@example.org"] [34 34] [5 10]]
                  ["Liam" ["liam@example.org" "liam@example.org"] [13 13] [11 42]]]
                 subject)
              "returns grouped results")))

      (testing "with multiple grouped-by fields"
        (let [qry     '{:context  {:ex "http://example.org/ns/"}
                        :select   [?name ?email ?age ?favNums]
                        :where    [[?s :schema/name ?name]
                                   [?s :schema/email ?email]
                                   [?s :schema/age ?age]
                                   [?s :ex/favNums ?favNums]]
                        :group-by [?name ?email ?age]
                        :order-by ?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" "alice@example.org" 50 [9 42 76]]
                  ["Brian" "brian@example.org" 50 [7]]
                  ["Cam" "cam@example.org" 34 [5 10]]
                  ["Liam" "liam@example.org" 13 [11 42]]]
                 subject)
              "returns grouped results"))

        (testing "with having clauses"
          (is (= [["Liam" [11 42]] ["Cam" [5 10]] ["Alice" [9 42 76]]]
                 @(fluree/query db '{:context  {:ex "http://example.org/ns/"}
                                     :select   [?name ?favNums]
                                     :where    [[?s :schema/name ?name]
                                                [?s :ex/favNums ?favNums]]
                                     :group-by ?name
                                     :having   (>= (count ?favNums) 2)}))
              "filters results according to the supplied having function code")

          (is (= [["Liam" [11 42]] ["Alice" [9 42 76]]]
                 @(fluree/query db '{:context  {:ex "http://example.org/ns/"}
                                     :select   [?name ?favNums]
                                     :where    [[?s :schema/name ?name]
                                                [?s :ex/favNums ?favNums]]
                                     :group-by ?name
                                     :having   (>= (avg ?favNums) 10)}))
              "filters results according to the supplied having function code"))))))

(deftest ^:integration select-distinct-test
  (testing "Distinct queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)
          q      '{:context         {:ex "http://example.org/ns/"}
                   :select-distinct [?name ?email]
                   :where           [[?s :schema/name ?name]
                                     [?s :schema/email ?email]
                                     [?s :ex/favNums ?favNum]]
                   :order-by        ?favNum}]
      (is (= [["Cam" "cam@example.org"]
              ["Brian" "brian@example.org"]
              ["Alice" "alice@example.org"]
              ["Liam" "liam@example.org"]]
             @(fluree/query db q))
          "return results without repeated entries"))))

(deftest ^:integration values-test
  (testing "Queries with pre-specified values"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "binding a single variable"
        (testing "with a single value"
          (let [q '{:context {:ex "http://example.org/ns/"}
                    :select  [?name ?age]
                    :where   [[?s :schema/email ?email]
                              [?s :schema/name ?name]
                              [?s :schema/age ?age]]
                    :values  [?email ["alice@example.org"]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the bound value")))
        (testing "with multiple values"
          (let [q '{:context {:ex "http://example.org/ns/"}
                    :select  [?name ?age]
                    :where   [[?s :schema/email ?email]
                              [?s :schema/name ?name]
                              [?s :schema/age ?age]]
                    :values  [?email ["alice@example.org" "cam@example.org"]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values"))))
      (testing "binding multiple variables"
        (testing "with multiple values"
          (let [q '{:context {:ex "http://example.org/ns/"}
                    :select  [?name ?age]
                    :where   [[?s :schema/email ?email]
                              [?s :ex/favNums ?favNum]
                              [?s :schema/name ?name]
                              [?s :schema/age ?age]]
                    :values  [[?email ?favNum] [["alice@example.org" 42]
                                                ["cam@example.org" 10]]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values")))
        (testing "with some values not present"
          (let [q '{:context {:ex "http://example.org/ns/"}
                    :select  [?name ?age]
                    :where   [[?s :schema/email ?email]
                              [?s :ex/favNums ?favNum]
                              [?s :schema/name ?name]
                              [?s :schema/age ?age]]
                    :values  [[?email ?favNum] [["alice@example.org" 42]
                                                ["cam@example.org" 37]]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the existing bound values")))))))

(deftest ^:integration bind-query-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "with 2 separate fn binds"
      (let [q   '{:context  {:ex "http://example.org/ns/"}
                  :select   [?firstLetterOfName ?name ?decadesOld]
                  :where    [[?s :schema/age ?age]
                             {:bind {?decadesOld (quot ?age 10)}}
                             [?s :schema/name ?name]
                             {:bind {?firstLetterOfName (subStr ?name 0 1)}}]
                  :order-by ?firstLetterOfName}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" 5]
                ["B" "Brian" 5]
                ["C" "Cam" 3]
                ["L" "Liam" 1]]
               res))))

    (testing "with 2 fn binds in one bind map"
      (let [q   '{:context  {:ex "http://example.org/ns/"}
                  :select   [?firstLetterOfName ?name ?canVote]
                  :where    [[?s :schema/age ?age]
                             [?s :schema/name ?name]
                             {:bind {?firstLetterOfName (subStr ?name 0 1)
                                     ?canVote           (>= ?age 18)}}]
                  :order-by ?name}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" true]
                ["B" "Brian" true]
                ["C" "Cam" true]
                ["L" "Liam" false]]
               res))))

    (testing "with invalid aggregate fn"
      (let [q '{:context  {:ex "http://example.org/ns/"}
                :select   [?sumFavNums ?name ?canVote]
                :where    [[?s :schema/age ?age]
                           [?s :ex/favNums ?favNums]
                           [?s :schema/name ?name]
                           {:bind {?sumFavNums (sum ?favNums)
                                   ?canVote    (>= ?age 18)}}]
                :order-by ?name}]
        (is (re-matches
              #"Aggregate function sum is only valid for grouped values"
              (ex-message @(fluree/query db q))))))))

(deftest ^:integration iri-test
  (let [conn   (test-utils/create-conn)
        movies (test-utils/load-movies conn)
        db     (fluree/db movies)]
    (testing "iri queries"
      (let [test-subject @(fluree/query db '{:select [?s]
                                             :where [[?s :id :wiki/Q836821]]})]
        (is (= [[:wiki/Q836821]]
               test-subject)
            "Returns the subject with that iri")))))

(deftest ^:integration multi-query-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "multi queries"
      (let [q       '{"alice" {:select {?s [:*]}
                               :where  [[?s :schema/email "alice@example.org"]]}
                      "brian" {:select {?s [:*]}
                               :where  [[?s :schema/email "brian@example.org"]]}}
            subject @(fluree/multi-query db q)]
        (is (= {"alice"
                [{:id                             "http://example.org/ns/alice",
                  :rdf/type                       ["http://example.org/ns/User"],
                  :schema/name                    "Alice",
                  :schema/email                   "alice@example.org",
                  :schema/age                     50,
                  "http://example.org/ns/favNums" [9 42 76]}],
                "brian"
                [{:id                             "http://example.org/ns/brian",
                  :rdf/type                       ["http://example.org/ns/User"],
                  :schema/name                    "Brian",
                  :schema/email                   "brian@example.org",
                  :schema/age                     50,
                  "http://example.org/ns/favNums" 7}]}
               subject)
            "returns all results in a map keyed by alias.")))))
