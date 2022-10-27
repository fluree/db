(ns fluree.db.transact.retraction-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-fixtures :as test]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest using-pre-defined-types-as-classes
  (testing "Class not used as class initially can still be used as one."
    (let [conn   test/memory-conn
          ledger @(fluree/create conn "tx/retract")
          db1    @(fluree/stage
                    ledger
                    {:context      {:ex "http://example.org/ns/"}
                     :id           :ex/alice,
                     :type         :ex/User,
                     :schema/name  "Alice"
                     :ex/last      "Blah"
                     :schema/email "alice@example.org"
                     :schema/age   42
                     :ex/favNums   [42, 76, 9]
                     :ex/scores    [102 92.5 90]})
          db2    @(fluree/stage
                    db1
                    {:context    {:ex "http://example.org/ns/"}
                     :id         :ex/alice,
                     :schema/age nil})]
      (is (= @(fluree/query db2 {:context {:ex "http://example.org/ns/"}
                                 :select  [:*]
                                 :from    :ex/alice})
             [{:id           :ex/alice,
               :rdf/type     [:ex/User],
               :ex/favNums   [9 42 76],
               :ex/scores    [90 92.5 102],
               :ex/last      "Blah",
               :schema/email "alice@example.org",
               :schema/name  "Alice"}])))))
