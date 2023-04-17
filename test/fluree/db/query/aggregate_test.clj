(ns fluree.db.query.aggregate-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration aggregates-test
  (testing "aggregate queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with explicit grouping"
        (let [qry     '{"select"   [?name (count ?favNums)]
                        "where"    [[?s "schema:name" ?name]
                                    [?s "ex:favNums" ?favNums]]
                        "group-by" ?name}
              subject @(fluree/query db qry)]
          (is (= [["Liam" 2] ["Cam" 2] ["Alice" 3] ["Brian" 1]]
                 subject)
              "aggregates bindings within each group")))
      (testing "with implicit grouping"
        (let [qry     '{"select" [(count ?name)]
                        "where"  [[?s "schema:name" ?name]]}
              subject @(fluree/query db qry)]
          (is (= [[4]] subject)
              "aggregates bindings for all results"))))))
