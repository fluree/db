(ns fluree.db.query.aggregate-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]))

(deftest ^:integration aggregates-test
  (testing "aggregate queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with explicit grouping"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '[?name (count ?favNums)]
                       :where    '{:schema/name ?name
                                   :ex/favNums  ?favNums}
                       :group-by '?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" 3] ["Brian" 1] ["Cam" 2] ["Liam" 2]]
                 subject)
              "aggregates bindings within each group")))
      (testing "with singular function selector"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '(count ?favNums)
                       :where    '{:schema/name ?name
                                   :ex/favNums  ?favNums}
                       :group-by '?name}
              subject @(fluree/query db qry)]
          (is (= [3 1 2 2] subject)
              "aggregates bindings within each group")))
      (testing "with implicit grouping"
        (let [qry     {:context test-utils/default-context
                       :select  '[(count ?name)]
                       :where   '{:schema/name ?name}}
              subject @(fluree/query db qry)]
          (is (= [[4]] subject)
              "aggregates bindings for all results")))
      (testing "with min and implicit grouping"
        (let [qry     {:context [test-utils/default-context
                                 {:ex "http://example.org/ns/"}]
                       :select  '[(min ?nums)]
                       :where   '{:ex/favNums ?nums}}
              subject @(fluree/query db qry)]
          (is (= [[5]] subject)
              "aggregates bindings for all results")))
      (testing "with implicit grouping and comparable data types"
        (let [qry     {:context test-utils/default-context
                       :select  ['(max ?birthDate)]
                       :where   '{:schema/birthDate ?birthDate}}
              subject @(fluree/query db qry)]
          (is (= [[(java.time.LocalDate/parse "2011-09-26")]] subject)
              "aggregates bindings for all results")))
      (testing "with ordering"
        (let [qry {:context  [test-utils/default-context
                              {:ex "http://example.org/ns/"}]
                   :where    '{:schema/name ?name
                               :ex/favNums  ?favNums}
                   :group-by '?name
                   :order-by '?count
                   :select   '[?name (as (count ?favNums) ?count)]}]
          (is (= [["Brian" 1] ["Cam" 2] ["Liam" 2] ["Alice" 3]]
                 @(fluree/query db qry))))))))
