(ns fluree.db.query.sql-test
  (:require
   #?@(:clj  [[clojure.test :refer :all]]
       :cljs [[cljs.test :refer-macros [deftest is testing]]])
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
            (is (= {"?person" ["*"]}
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/age" 18]]
                   (:where subject))
                "correctly constructs the where clause")))

        (testing "without any restrictions"
          (let [query   "SELECT * FROM person"
                subject (parse query)]
            (is (= {"?person" ["*"]}
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "type" "person"]]
                   (:where subject))
                "correctly constructs the where clause"))))

      (testing "with a comma-separated select list"
        (testing "with spaces"
          (let [query   "SELECT name, email FROM person"
                subject (parse query)]
            (is (= ["?personName" "?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")))

        (testing "without spaces"
          (let [query   "SELECT name,email FROM person"
                subject (parse query)]
            (is (= ["?personName" "?personEmail"]
                   (:select subject))
                "correctly constructs the select clause"))))

      (testing "with subject _id placeholder"
        (testing "in the select list"
          (let [query   "SELECT $, name FROM person WHERE age = 18"
                subject (parse query)]

            (is (= ["?person" "?personName"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/age" 18]
                    ["?person" "person/name" "?personName"]]
                   (:where subject))))))

      (testing "with qualified fields"
        (let [query   "SELECT person.name FROM person WHERE person.age = 18"
              subject (parse query)]
          (is (= ["?personName"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" 18]
                  ["?person" "person/name" "?personName"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with equality predicate"
        (let [query   "SELECT name, email FROM person WHERE age = 18"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" 18]
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with 'greater than' predicate"
        (let [query   "SELECT name, email FROM person WHERE age > 18"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" "?personAge"]
                  {:filter ["(> ?personAge 18)"]}
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with 'less than' predicate"
        (let [query   "SELECT name, email FROM person WHERE age < 18"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" "?personAge"]
                  {:filter ["(< ?personAge 18)"]}
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with 'greater than or equal' predicate"
        (let [query   "SELECT name, email FROM person WHERE age >= 18"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" "?personAge"]
                  {:filter ["(>= ?personAge 18)"]}
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with 'less than or equal' predicate"
        (let [query   "SELECT name, email FROM person WHERE age <= 18"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" "?personAge"]
                  {:filter ["(<= ?personAge 18)"]}
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with a null predicate"
        (testing "negated"
          (let [query   "SELECT name, email FROM person WHERE email IS NOT NULL"
                subject (parse query)]

            (is (= ["?personName" "?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/email" "?personEmail"]
                    ["?person" "person/name" "?personName"]
                    ["?person" "person/email" "?personEmail"]]
                   (:where subject))
                "correctly constructs the where clause")))

        (testing "not negated"
          (let [query   "SELECT name, email FROM person WHERE email IS NULL"
                subject (parse query)]

            (is (= ["?personName" "?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "type" "person"]
                    {:optional [["?person" "person/email" "?personEmail"]]}
                    {:filter ["(nil? ?personEmail)"]}
                    ["?person" "person/name" "?personName"]
                    ["?person" "person/email" "?personEmail"]]
                   (:where subject))
                "correctly constructs the where clause"))))

      (testing "with aggregate functions"
        (testing "count"
          (testing "explicit fields"
            (let [query   "SELECT COUNT(middleName) FROM person"
                  subject (parse query)]

              (is (= ["(count ?personMiddleName)"]
                     (:select subject))
                  "correctly constructs the select clause")

              (is (= [["?person" "type" "person"]
                      ["?person" "person/middleName" "?personMiddleName"]]
                     (:where subject))
                  "correctly constructs the where clause"))

            (testing "with distinct"
              (let [query   "SELECT COUNT(DISTINCT middleName) FROM person"
                    subject (parse query)]

                (is (= ["(count (distinct ?personMiddleName))"]
                       (:select subject))
                    "correctly constructs the select clause")

                (is (= [["?person" "type" "person"]
                        ["?person" "person/middleName" "?personMiddleName"]]
                       (:where subject))
                    "correctly constructs the where clause")))))))

    (testing "on a complex query"
      (testing "with AND"
        (let [query   "SELECT name, email FROM person WHERE age = 18 AND team = 'red' AND foo = 'bar'"
              subject (parse query)]
          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [["?person" "person/age" 18]
                  ["?person" "person/team" "red"]
                  ["?person" "person/foo" "bar"]
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with OR"
        (let [query   "SELECT name, email FROM person WHERE age > 18 OR team = 'red'"
              subject (parse query)]

          (is (= ["?personName" "?personEmail"]
                 (:select subject))
              "correctly constructs the select clause")

          (is (= [{:union
                   [[["?person" "person/age" "?personAge"]
                     {:filter ["(> ?personAge 18)"]}]
                    [["?person" "person/team" "red"]]]}
                  ["?person" "person/name" "?personName"]
                  ["?person" "person/email" "?personEmail"]]
                 (:where subject))
              "correctly constructs the where clause")))

      (testing "with BETWEEN"
        (testing "without NOT"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35"
                subject (parse query)]

            (is (= ["?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/age" "?personAge"]
                    {:filter ["(>= ?personAge 18)" "(<= ?personAge 35)"]}
                    ["?person" "person/email" "?personEmail"]]
                   (:where subject))
                "correctly constructs the where clause")))

        (testing "with NOT"
          (let [query   "SELECT email FROM person WHERE age NOT BETWEEN 18 AND 35"
                subject (parse query)]

            (is (= ["?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/age" "?personAge"]
                    {:union [{:filter ["(< ?personAge 18)"]}
                             {:filter ["(> ?personAge 35)"]}]}
                    ["?person" "person/email" "?personEmail"]]
                   (:where subject))
                "correctly constructs the where clause"))))

      (testing "with IN"
        (testing "without NOT"
          (let [query   "SELECT email FROM person WHERE age IN (18, 19, 20, 21)"
                subject (parse query)]

            (is (= ["?personEmail"]
                   (:select subject))
                "correctly constructs the select clause")

            (is (= [["?person" "person/age" "?personAge"]
                    {:filter "(or (= ?personAge 18) (= ?personAge 19) (= ?personAge 20) (= ?personAge 21))"}
                    ["?person" "person/email" "?personEmail"]]
                   (:where subject))
                "correctly constructs the where clause")))
        (testing "with NOT"
          (testing "without NOT"
            (let [query   "SELECT email FROM person WHERE age NOT IN (18, 19, 20, 21)"
                  subject (parse query)]

              (is (= ["?personEmail"]
                     (:select subject))
                  "correctly constructs the select clause")

              (is (= [["?person" "person/age" "?personAge"]
                      {:filter "(and (not= ?personAge 18) (not= ?personAge 19) (not= ?personAge 20) (not= ?personAge 21))"}
                      ["?person" "person/email" "?personEmail"]]
                     (:where subject))
                  "correctly constructs the where clause"))))))

    (testing "with query options"
      (testing "ordering"
        (testing "without explicit direction"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 ORDER BY age"
                subject (parse query)]
            (is (= "person/age"
                   (-> subject :opts :orderBy))
                "correctly constructs the orderBy clause")))

        (testing "with explicit direction"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 ORDER BY age DESC"
                subject (parse query)]
            (is (= ["DESC" "person/age"]
                   (-> subject :opts :orderBy))
                "correctly constructs the orderBy clause"))))

      (testing "grouping"
        (testing "with multiple fields"
          (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 GROUP BY age, email"
                subject (parse query)]
            (is (= ["?personAge" "?personEmail"]
                   (-> subject :opts :groupBy))
                "correctly constructs the groupBy clause"))))

      (testing "limiting"
        (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 LIMIT 10"
              subject (parse query)]
          (is (= 10
                 (-> subject :opts :limit))
              "correctly constructs the limit clause")))

      (testing "offsetting"
        (let [query   "SELECT email FROM person WHERE age BETWEEN 18 AND 35 LIMIT 10 OFFSET 5"
              subject (parse query)]
          (is (= 5
                 (-> subject :opts :offset))
              "correctly constructs the offset clause"))))

    (testing "with multiple collections"
      (let [query   "SELECT person.name, job.title FROM person JOIN job ON person.job = job.$ WHERE person.age = 18"
            subject (parse query)]
        (is (= ["?personName" "?jobTitle"]
               (:select subject))
            "correctly constructs the select clause")

        (is (= [["?person" "person/job" "?job"]
                ["?person" "person/age" 18]
                ["?person" "person/name" "?personName"]
                ["?job" "job/title" "?jobTitle"]]
               (:where subject))
            "correctly constructs the where clause")))))
