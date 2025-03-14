(ns fluree.db.transact.retraction-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]))

(deftest ^:integration retracting-data
  (testing "Retractions of individual properties and entire subjects."
    (let [conn           (test-utils/create-conn)
          ledger         @(fluree/create conn "tx/retract")
          db             @(fluree/stage
                            (fluree/db ledger)
                            {"insert"
                             {:context [test-utils/default-context
                                        {:ex "http://example.org/ns/"}]
                              :graph   [{:id          :ex/alice,
                                         :type        :ex/User,
                                         :schema/name "Alice"
                                         :schema/age  42}
                                        {:id          :ex/bob,
                                         :type        :ex/User,
                                         :schema/name "Bob"
                                         :schema/age  22}
                                        {:id          :ex/jane,
                                         :type        :ex/User,
                                         :schema/name "Jane"
                                         :schema/age  30}]}})
          ;; retract Alice's age attribute
          db-age-retract @(fluree/stage
                            db
                            {"delete"
                             {:context    [test-utils/default-context
                                           {:ex "http://example.org/ns/"}]
                              :id         :ex/alice,
                              :schema/age 42}})]
      (is (= [{:id           :ex/alice,
               :type     :ex/User,
               :schema/name  "Alice"}]
             @(fluree/query db-age-retract
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}],
                             :select {:ex/alice [:*]}}))
          "Alice should no longer have an age property")))
  (testing "retracting ordered lists"
    (let [conn             (test-utils/create-conn)
          ledger           @(fluree/create conn "tx/retract")
          context          [test-utils/default-str-context
                            {"ex"        "http://example.org/ns/"
                             "ex:items2" {"@container" "@list"}}]
          q1               {:context context
                            :select  {"ex:list-test" ["*"]}}
          db               @(fluree/stage
                             (fluree/db ledger)
                             {"@context" context
                              "insert"
                              [{"id"        "ex:list-test"
                                "ex:items1" {"@list" ["zero" "one" "two"
                                                      "three"]}
                                "ex:items2" ["four" "five" "six" "seven"]}]})
          before-retract   @(fluree/query db q1)
          db-after-retract @(fluree/stage
                             db
                             {"@context" context
                              "delete"   {"id"        "ex:list-test"
                                          "ex:items1" "?items1"
                                          "ex:items2" "?items2"}
                              "where"    {"id"        "ex:list-test"
                                          "ex:items1" "?items1"
                                          "ex:items2" "?items2"}})
          after-retract    @(fluree/query db-after-retract q1)]
      (is (= [{"id"        "ex:list-test"
               "ex:items1" ["zero" "one" "two" "three"]
               "ex:items2" ["four" "five" "six" "seven"]}]
             before-retract))
      (is (= [{"id" "ex:list-test"}] after-retract)))))
