(ns fluree.db.query.aggregate-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration aggregates-test
  (testing "aggregate queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)
          qry    '{:context {:ex "http://example.org/ns/"}
                   :select  [?name (count ?favNums)]
                   :where   [[?s :schema/name ?name]
                              [?s :ex/favNums ?favNums]]}]
      (testing "with explicit grouping"
        (let [qry    '{:context {:ex "http://example.org/ns/"}
                       :select  [?name (count ?favNums)]
                       :where   [[?s :schema/name ?name]
                                 [?s :ex/favNums ?favNums]]
                       group-by ?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" 3] ["Brian" 1] ["Cam" 2]]
                 subject)
              "aggregates bindings within each group")))
      (testing "with implicit grouping"
        (let [qry '{:context {:ex "http://example.org/ns/"}
                    :select  [(count ?name)]
                    :where   [[?s :schema/name ?name]]}
              subject @(fluree/query db qry)]
          (is (= [[3]] subject)
              "aggregates bindings for all results"))))))
