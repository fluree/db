(ns fluree.db.query.property-path-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.api :as fluree]))

(deftest transitive-paths
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "property/path")
        db0    (fluree/db ledger)]
    (testing "one+"
      (testing "no variables"
        (let [db1 @(fluree/stage db0 {"insert"
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
          (testing "non-transitive"
            (is (= []
                   @(fluree/query db1 {"where" [{"@id" "ex:a" "ex:y" {"@id" "ex:f"}}]
                                       "select" {"ex:a" ["*"]}}))))
          (testing "transitive"
            (let [result @(fluree/query db1 {"where" [{"@id" "ex:a" "<ex:y+>" {"@id" "ex:f"}}]
                                             "select" {"ex:a" ["*"]}})]
              (is (= {:status 400, :error :db/unsupported-transitive-path}
                     (ex-data result)))
              (is (= "Unsupported transitive path."
                     (ex-message result)))))))
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
          (testing "transitive"
            (testing "without cycle"
              (is (= ["ex:b" "ex:c" "ex:d" "ex:e"]
                     @(fluree/query db1 {"where" [{"@id" "ex:a" "<<ex:knows>+>" "?who"}]
                                         "select" "?who"}))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:e" "ex:knows" {"@id" "ex:a"}}})]
                (is (= ["ex:b" "ex:c" "ex:d" "ex:e" "ex:a"]
                       @(fluree/query db2 {"where" [{"@id" "ex:a" "<ex:knows+>" "?who"}]
                                           "select" "?who"}))))))))
      (testing "subject variable"
        (let [db1 @(fluree/stage db0 {"insert"
                                      [{"@id" "ex:a"
                                        "ex:knows" {"@id" "ex:b"
                                                    "ex:knows" [{"@id" "ex:c"}
                                                                {"@id" "ex:d"
                                                                 "ex:knows" {"@id" "ex:e"}}]}}]})]
          (testing "non-transitive"
            (is (= ["ex:d"]
                   @(fluree/query db1 {"where" [{"@id" "?who" "ex:knows" {"@id" "ex:e"}}]
                                       "select" "?who"}))))
          (testing "transitive"
            (testing "without cycle"
              (is (= ["ex:d" "ex:b" "ex:a"]
                     @(fluree/query db1 {"where" [{"@id" "?who" "<ex:knows+>" {"@id" "ex:e"}}]
                                         "select" "?who"}))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:e" "ex:knows" {"@id" "ex:a"}}})]
                (is (= ["ex:d" "ex:b" "ex:a" "ex:e"]
                       @(fluree/query db2 {"where" [{"@id" "?who" "<ex:knows+>" {"@id" "ex:e"}}]
                                           "select" "?who"}))))))))
      (testing "subject and object variable"
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
          (testing "transitive"
            (testing "without cycle"
              (is (= [["ex:1" "ex:2"]
                      ["ex:2" "ex:3"]
                      ["ex:1" "ex:3"]]
                     @(fluree/query db1 {"where" [{"@id" "?x" "<ex:knows+>" "?y"}]
                                         "select" ["?x" "?y"]}))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:3" "ex:knows" {"@id" "ex:1"}}})]
                (is (= [["ex:3" "ex:2"]
                        ["ex:1" "ex:2"]
                        ["ex:2" "ex:3"]
                        ["ex:1" "ex:3"]
                        ["ex:2" "ex:2"]
                        ["ex:3" "ex:3"]
                        ["ex:3" "ex:1"]
                        ["ex:2" "ex:1"]
                        ["ex:1" "ex:1"]]
                       @(fluree/query db2 {"where" [{"@id" "?x" "<ex:knows+>" "?y"}]
                                           "select" ["?x" "?y"]})))))))))
    (testing "zero+"
      (testing "no variables"
        (let [db1 @(fluree/stage db0 {"insert"
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
          (testing "non-transitive"
            (is (= []
                   @(fluree/query db1 {"where" [{"@id" "ex:a" "ex:y" {"@id" "ex:f"}}]
                                       "select" {"ex:a" ["*"]}}))))
          (testing "transitive"
            (let [result @(fluree/query db1 {"where" [{"@id" "ex:a" "<ex:y*>" {"@id" "ex:f"}}]
                                             "select" {"ex:a" ["*"]}})]
              (is (= {:status 400, :error :db/unsupported-transitive-path}
                     (ex-data result)))
              (is (= "Unsupported transitive path."
                     (ex-message result)))))))
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
          (testing "transitive"
            (testing "without cycle"
              (is (= ["ex:a" "ex:b" "ex:c" "ex:d" "ex:e"]
                     @(fluree/query db1 {"where" [{"@id" "ex:a" "<ex:knows*>" "?who"}]
                                         "select" "?who"}))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:e" "ex:knows" {"@id" "ex:a"}}})]
                (is (= ["ex:a" "ex:b" "ex:c" "ex:d" "ex:e"]
                       @(fluree/query db2 {"where" [{"@id" "ex:a" "<ex:knows*>" "?who"}]
                                           "select" "?who"}))))))))
      (testing "subject variable"
        (let [db1 @(fluree/stage db0 {"insert"
                                      [{"@id" "ex:a"
                                        "ex:knows" {"@id" "ex:b"
                                                    "ex:knows" [{"@id" "ex:c"}
                                                                {"@id" "ex:d"
                                                                 "ex:knows" {"@id" "ex:e"}}]}}]})]
          (testing "non-transitive"
            (is (= ["ex:d"]
                   @(fluree/query db1 {"where" [{"@id" "?who" "ex:knows" {"@id" "ex:e"}}]
                                       "select" "?who"}))))
          (testing "transitive"
            (testing "without cycle"
              (is (= ["ex:e" "ex:d" "ex:b" "ex:a"]
                     @(fluree/query db1 {"where" [{"@id" "?who" "<ex:knows*>" {"@id" "ex:e"}}]
                                         "select" "?who"}))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:e" "ex:knows" {"@id" "ex:a"}}})]
                (is (= ["ex:e" "ex:d" "ex:b" "ex:a"]
                       @(fluree/query db2 {"where" [{"@id" "?who" "<ex:knows*>" {"@id" "ex:e"}}]
                                           "select" "?who"}))))))))
      (testing "subject and object variable"
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
          (testing "transitive"
            (testing "without cycle"
              (is (= [["ex:1" "ex:1"]
                      ["ex:1" "ex:2"]
                      ["ex:1" "ex:3"]
                      ["ex:2" "ex:2"]
                      ["ex:2" "ex:3"]
                      ["ex:3" "ex:3"]]
                     (sort @(fluree/query db1 {"where" [{"@id" "?x" "<ex:knows*>" "?y"}]
                                               "select" ["?x" "?y"]})))))
            (testing "disjoint subgraphs"
              (let [db2 @(fluree/stage db1
                                       {"insert"
                                        [{"@id" "ex:4"
                                          "ex:knows" {"@id" "ex:5"
                                                      "ex:knows" {"@id" "ex:6"}}}]})]
                (is (= #{["ex:1" "ex:1"]
                         ["ex:1" "ex:2"]
                         ["ex:1" "ex:3"]
                         ["ex:2" "ex:2"]
                         ["ex:2" "ex:3"]
                         ["ex:3" "ex:3"]

                         ["ex:4" "ex:4"]
                         ["ex:4" "ex:5"]
                         ["ex:4" "ex:6"]
                         ["ex:5" "ex:5"]
                         ["ex:5" "ex:6"]
                         ["ex:6" "ex:6"]}
                       (set @(fluree/query db2 {"where" [{"@id" "?x" "<ex:knows*>" "?y"}]
                                                "select" ["?x" "?y"]}))))))
            (testing "with cycle"
              (let [db2 @(fluree/stage db1 {"insert" {"@id" "ex:3" "ex:knows" {"@id" "ex:1"}}})]
                (is (= [["ex:1" "ex:1"]
                        ["ex:1" "ex:2"]
                        ["ex:1" "ex:3"]

                        ["ex:2" "ex:1"]
                        ["ex:2" "ex:2"]
                        ["ex:2" "ex:3"]

                        ["ex:3" "ex:1"]
                        ["ex:3" "ex:2"]
                        ["ex:3" "ex:3"]]
                       (sort @(fluree/query db2 {"where" [{"@id" "?x" "<ex:knows*>" "?y"}]
                                                 "select" ["?x" "?y"]}))))))))))))
