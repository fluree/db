(ns fluree.db.query.sql-test
  (:require [clojure.test :refer :all]
            [fluree.db.query.sql :refer [parse]]))

(deftest sql-query-parser-test
  (testing "parse"
    (testing "on a simple query"
      (testing "with a select quantifier"
        (testing "DISTINCT"
          (let [query   "SELECT DISTINCT email FROM person WHERE age = 18"
                subject (parse query)]
            (is (contains? subject :selectDistinct)
                "uses the correct select key")))

        (testing "ALL"
          (let [query   "SELECT ALL email FROM person WHERE age = 18"
                subject (parse query)]
            (is (contains? subject :select)
                "uses the correct select key"))))

      (testing "without a select quantifier"
        (let [query   "SELECT email FROM person WHERE age = 18"
              subject (parse query)]
          (is (contains? subject :select)
              "uses the correct select key")))

      (testing "with a wildcard select list"
        (testing "with restrictions"
          (let [query   "SELECT * FROM person WHERE age = 18"
                subject (parse query)]
            (is (= (:select subject)
                   {"?person" ["*"]})
                "correctly constructs the select clause")

            (is (= (:where subject)
                   [["?person" "person/age" 18]])
                "correctly constructs the where clause")))

        (testing "without any restrictions"
          (let [query   "SELECT * FROM person"
                subject (parse query)]
            (is (= (:select subject)
                   {"?person" ["*"]})
                "correctly constructs the select clause")

            (is (= (:where subject)
                   [["?person" "rdf:type" "person"]])
                "does not include a where clause"))))

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
              "correctly constructs the where clause")))

      (testing "with 'greater than' predicate"
        (let [query   "SELECT name, email FROM person WHERE age > 18"
              subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "person/age" "?age"]
                  {:filter ["(> ?age 18)"]}
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))

      (testing "with 'less than' predicate"
        (let [query   "SELECT name, email FROM person WHERE age < 18"
              subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "person/age" "?age"]
                  {:filter ["(< ?age 18)"]}
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))

      (testing "with a null predicate"
        (testing "negated"
          (let [query   "SELECT name, email FROM person WHERE email IS NOT NULL"
                subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "person/email" "?email"]
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))

        (testing "not negated"
          (let [query   "SELECT name, email FROM person WHERE email IS NULL"
                subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "rdf:type" "person"]
                  {:optional [["?person" "person/email" "?email"]]}
                  {:filter ["(nil? ?email)"]}
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))))

    (testing "on a complex query"
      (testing "with AND"
        (let [query   "SELECT name, email FROM person WHERE age = 18 AND team = \"red\" AND foo = \"bar\""
              subject (parse query)]
          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [["?person" "person/age" 18]
                  ["?person" "person/team" "\"red\""]
                  ["?person" "person/foo" "\"bar\""]
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))

      (testing "with OR"
        (let [query   "SELECT name, email FROM person WHERE age > 18 OR team = \"red\""
              subject (parse query)]

          (is (= (:select subject)
                 ["?name" "?email"])
              "correctly constructs the select clause")

          (is (= (:where subject)
                 [{:union
                   [[["?person" "person/age" "?age"]
                     {:filter ["(> ?age 18)"]}]
                    [["?person" "person/team" "\"red\""]]]}
                  ["?person" "person/name" "?name"]
                  ["?person" "person/email" "?email"]])
              "correctly constructs the where clause")))

      (testing "with BETWEEN"
        (testing "without NOT"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35"
                subject (parse query)]

            (is (= (:select subject)
                   ["?email"])
                "correctly constructs the select clause")

            (is (= (:where subject)
                   [["?person" "person/age" "?age"]
                    {:filter ["(>= ?age 18)" "(<= ?age 35)"]}
                    ["?person" "person/email" "?email"]])
                "correctly constructs the where clause")))

        (testing "with NOT"
          (let [query   "SELECT email FROM person WHERE age NOT BETWEEN 18 AND 35"
                subject (parse query)]

            (is (= (:select subject)
                   ["?email"])
                "correctly constructs the select clause")

            (is (= (:where subject)
                   [["?person" "person/age" "?age"]
                    {:union [{:filter ["(< ?age 18)"]}
                             {:filter ["(> ?age 35)"]}]}
                    ["?person" "person/email" "?email"]])
                "correctly constructs the where clause"))))

      (testing "with IN"
        (testing "without NOT"
          (let [query   "SELECT email FROM person WHERE age IN (18, 19, 20, 21)"
                subject (parse query)]

            (is (= (:select subject)
                   ["?email"])
                "correctly constructs the select clause")

            (is (= (:where subject)
                   [["?person" "person/age" "?age"]
                    {:filter "(or (= ?age 18) (= ?age 19) (= ?age 20) (= ?age 21))"}
                    ["?person" "person/email" "?email"]])
                "correctly constructs the where clause")))
        (testing "with NOT"
          (testing "without NOT"
            (let [query   "SELECT email FROM person WHERE age NOT IN (18, 19, 20, 21)"
                  subject (parse query)]

              (is (= (:select subject)
                     ["?email"])
                  "correctly constructs the select clause")

              (is (= (:where subject)
                     [["?person" "person/age" "?age"]
                      {:filter "(and (not= ?age 18) (not= ?age 19) (not= ?age 20) (not= ?age 21))"}
                      ["?person" "person/email" "?email"]])
                  "correctly constructs the where clause"))))))

    (testing "with query options"
      (testing "ordering"
        (testing "without explicit direction"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 ORDER BY age"
                subject (parse query)]
            (is (= (-> subject :opts :orderBy)
                   "person/age")
                "correctly constructs the orderBy clause")))

        (testing "with explicit direction"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 ORDER BY age DESC"
                subject (parse query)]
            (is (= (-> subject :opts :orderBy)
                   ["DESC" "person/age"])
                "correctly constructs the orderBy clause")))))))
