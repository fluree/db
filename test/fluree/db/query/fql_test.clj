(ns fluree.db.query.fql-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration grouping-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "multiple grouped fields"
      (let [qry     '{:context  {:ex "http://example.org/ns/"}
                      :select   [?name ?email ?age ?favNums]
                      :where    [[?s :schema/name ?name]
                                 [?s :schema/email ?email]
                                 [?s :schema/age ?age]
                                 [?s :ex/favNums ?favNums]]
                      :group-by ?name}
            subject @(fluree/query db qry)]
        (is (= [["Alice"
                 ["alice@example.org" "alice@example.org" "alice@example.org"]
                 [50 50 50]
                 [9 42 76]]
                ["Brian" ["brian@example.org"] [50] [7]]
                ["Cam" ["cam@example.org" "cam@example.org"] [34 34] [5 10]]]
               subject)
            "returns grouped results")))))
