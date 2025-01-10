(ns fluree.db.query.property-path-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.api :as fluree]))

(deftest one-or-more
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "one-or-more")
        db0 (fluree/db ledger)
        db1 @(fluree/stage db0 {"insert"
                                [{"@id" "ex:a"
                                  "ex:knows" {"@id" "ex:b"
                                              "ex:knows" [{"@id" "ex:c"}
                                                          {"@id" "ex:d"
                                                           "ex:knows" {"@id" "ex:e"
                                                                       "ex:knows" {"@id" "ex:f"}}}]}}]})]
    (testing "single variable"
      (testing "non-transitive"
        (is (= ["ex:b"]
               @(fluree/query db1 {"where" [{"@id" "ex:a" "ex:knows" "?who"}]
                                   "select" "?who"}))))
      (testing "one+"
        (is (= ["ex:b" "ex:c" "ex:d" "ex:e" "ex:f"]
               @(fluree/query db1 {"where" [{"@id" "ex:a" "<ex:knows+>" "?who"}]
                                   "select" "?who"})))))))
