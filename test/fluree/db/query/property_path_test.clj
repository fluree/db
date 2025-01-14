(ns fluree.db.query.property-path-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.api :as fluree]))

(deftest transitive-paths
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "property/path")
        db0    (fluree/db ledger)]
    (testing "object variable"
      (let [db1 @(fluree/stage db0 {"insert"
                                    [{"@id" "ex:a"
                                      "ex:knows" {"@id" "ex:b"
                                                  "ex:knows" [{"@id" "ex:c"}
                                                              {"@id" "ex:d"
                                                               "ex:knows" {"@id" "ex:e"}}]}}]})]
        (testing "non-transitive"
          (is (= ["ex:b"]
                 @(fluree/query db1 {"where" [{"@id" "ex:a" "ex:knows" "?who"}]
                                     "select" "?who"}))))
        (testing "one+"
          (testing "without cycle"
            (is (= ["ex:b" "ex:c" "ex:d" "ex:e"]
                   @(fluree/query db1 {"where" [{"@id" "ex:a" "<ex:knows+>" "?who"}]
                                       "select" "?who"}))))
          (testing "with cycle"
            (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:e" "ex:knows" {"@id" "ex:a"}}})]
              (is (= ["ex:b" "ex:c" "ex:d" "ex:e" "ex:a"]
                     @(fluree/query db2 {"where" [{"@id" "ex:a" "<ex:knows+>" "?who"}]
                                         "select" "?who"}))))))))
    (testing "two variables"
      (let [db1 @(fluree/stage db0
                               {"insert"
                                [{"@id" "ex:1"
                                  "ex:knows" {"@id" "ex:2"
                                              "ex:knows" {"@id" "ex:3"}}}]})]
        (testing "non-transitive"
          (is (= [["ex:1" "ex:2"]
                  ["ex:2" "ex:3"]]
                 @(fluree/query db1 {"where" [{"@id" "?s" "ex:knows" "?o"}]
                                     "select" ["?s" "?o"]}))))
        (testing "one+"
          (testing "without cycle"
            (is (= [["ex:1" "ex:2"]
                    ["ex:2" "ex:3"]
                    ["ex:1" "ex:3"]]
                   @(fluree/query db1 {"where" [{"@id" "?x" "<ex:knows+>" "?y"}]
                                       "select" ["?x" "?y"]}))))
          (testing "with cycle"
            (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:3" "ex:knows" {"@id" "ex:1"}}})]
              (is (= [["ex:1" "ex:2"]
                      ["ex:3" "ex:2"]
                      ["ex:2" "ex:2"]
                      ["ex:2" "ex:1"]
                      ["ex:2" "ex:3"]
                      ["ex:3" "ex:3"]
                      ["ex:3" "ex:1"]
                      ["ex:1" "ex:3"]
                      ["ex:1" "ex:1"]]
                     @(fluree/query db2 {"where" [{"@id" "?x" "<ex:knows+>" "?y"}]
                                         "select" ["?x" "?y"]}))))))))
    #_(testing "zero+"
        (is (= [["ex:1" "ex:2"]
                ["ex:2" "ex:3"]
                ["ex:1" "ex:3"]
                ["ex:1" "ex:1"]
                ["ex:2" "ex:2"]
                ["ex:3" "ex:3"]]
               @(fluree/query db1 {"where" [{"@id" "?x" "ex:knows" "?person"}]
                                   "select" ["?x" "?person"]}))))))

(deftest transitive-paths2
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "property/path")
        db0    (fluree/db ledger)
        db1     @(fluree/stage db0
                               {"insert"
                                [{"@id" "ex:a"
                                  "ex:y" [{"@id" "ex:b"
                                           "ex:y" {"@id" "ex:c"
                                                   "ex:y" {"@id" "ex:d"
                                                           "ex:y" {"@id" "ex:e"
                                                                   "ex:y" {"@id" "ex:f"}}}}}
                                          {"@id" "ex:g"
                                           "ex:y" [{"@id" "ex:h"
                                                    "ex:y" {"@id" "ex:i"}}
                                                   {"@id" "ex:j"
                                                    "ex:y" {"@id" "ex:k"}}]}]}]})]
    (testing "one+"
      (is (= [["ex:a" "ex:i"]
              ["ex:c" "ex:e"]
              ["ex:h" "ex:i"]
              ["ex:e" "ex:f"]
              ["ex:b" "ex:e"]
              ["ex:g" "ex:i"]
              ["ex:b" "ex:c"]
              ["ex:a" "ex:d"]
              ["ex:j" "ex:k"]
              ["ex:a" "ex:f"]
              ["ex:c" "ex:f"]
              ["ex:a" "ex:e"]
              ["ex:a" "ex:h"]
              ["ex:g" "ex:k"]
              ["ex:a" "ex:k"]
              ["ex:d" "ex:f"]
              ["ex:d" "ex:e"]
              ["ex:a" "ex:j"]
              ["ex:b" "ex:d"]
              ["ex:c" "ex:d"]
              ["ex:g" "ex:h"]
              ["ex:a" "ex:b"]
              ["ex:a" "ex:g"]
              ["ex:g" "ex:j"]
              ["ex:b" "ex:f"]
              ["ex:a" "ex:c"]]
             @(fluree/query db1 {"where" [{"@id" "?a" "<ex:y+>" "?b"}]
                                 "select" ["?a" "?b"]}))))))
