(ns fluree.db.query.aggregate-test
  (:require [clojure.test :refer :all]
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
      (testing "with grouping"
        (let [grouped-qry (assoc qry :group-by '?name)
              subject     @(fluree/query db grouped-qry)]
          (is (= [["Alice" 3] ["Brian" 1] ["Cam" 2]]
                 subject)
              "aggregates bindings within each group"))))))
