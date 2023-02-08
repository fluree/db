(ns fluree.db.query.fql-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration grouping-test
  (testing "grouped queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with multiple grouped fields"
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
                  ["Cam" ["cam@example.org" "cam@example.org"] [34 34] [5 10]]]
                 subject)
              "returns grouped results")))

      (testing "with having clauses"
        (is (= [["Cam" [5 10]] ["Alice" [9 42 76]]]
               @(fluree/query db '{:context  {:ex "http://example.org/ns/"}
                                   :select   [?name ?favNums]
                                   :where    [[?s :schema/name ?name]
                                              [?s :ex/favNums ?favNums]]
                                   :group-by ?name
                                   :having   (>= (count ?favNums) 2)}))
            "filters results according to the supplied having function code")

        (is (= [["Cam" [5 10]] ["Alice" [9 42 76]] ["Brian" [7]]]
               @(fluree/query db '{:context  {:ex "http://example.org/ns/"}
                                   :select   [?name ?favNums]
                                   :where    [[?s :schema/name ?name]
                                              [?s :ex/favNums ?favNums]]
                                   :group-by ?name
                                   :having   (>= (avg ?favNums) 2)}))
            "filters results according to the supplied having function code")))))

(deftest ^:integration multi-query-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "multi queries"
      (let [q '{"alice" {:select {?s [:*]}
                         :where  [[?s :schema/email "alice@example.org"]]}
                "brian" {:select {?s [:*]}
                         :where  [[?s :schema/email "brian@example.org"]]}}
            subject @(fluree/multi-query db q)]
        (is (= {"alice"
                [{:id "http://example.org/ns/alice",
                  :rdf/type ["http://example.org/ns/User"],
                  :schema/name "Alice",
                  :schema/email "alice@example.org",
                  :schema/age 50,
                  "http://example.org/ns/favNums" [9 42 76]}],
                "brian"
                [{:id "http://example.org/ns/brian",
                  :rdf/type ["http://example.org/ns/User"],
                  :schema/name "Brian",
                  :schema/email "brian@example.org",
                  :schema/age 50,
                  "http://example.org/ns/favNums" 7}]}
               subject)
            "returns all results in a map keyed by alias.")))))
