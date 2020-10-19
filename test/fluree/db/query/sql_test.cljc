(ns fluree.db.query.sql-test
  (:requre [clojure.test :refer :all]
           [fluree.db.query.sql :refer [parse]]))

(deftest sql-query-parser-test
  (testing "parse"
    (testing "on a simple query"
      (testing "with equality predicate"
        (let [query   "SELECT name, email FROM person WHERE age = 18"
              subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "person/age" 18]
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause"))))))
