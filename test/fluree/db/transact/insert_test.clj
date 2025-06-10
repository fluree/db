(ns fluree.db.transact.insert-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration insert-data
  (testing "Inserting data into a ledger"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/insert")
          db     @(fluree/insert
                   (fluree/db ledger)
                   {"@context" [test-utils/default-str-context
                                {"ex" "http://example.org/ns/"}]
                    "@graph"   [{"id"         "ex:alice",
                                 "type"        "ex:User",
                                 "schema:name" "Alice"
                                 "schema:age"  42}
                                {"id"         "ex:bob",
                                 "type"        "ex:User",
                                 "schema:name" "Bob"
                                 "schema:age"  22}]})]
      (is (= ["Alice" "Bob"]
             @(fluree/query db
                            {"@context" [test-utils/default-str-context
                                         {"ex" "http://example.org/ns/"}]
                             "select" "?name"
                             "where"  {"schema:name" "?name"}})))
      "Inserted data should be retrievable."))

  (testing "Inserting data into a ledger using EDN keywords"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/insert-edn-keywords")
          db     @(fluree/insert
                   (fluree/db ledger)
                   {:context [test-utils/default-context
                              {:ex "http://example.org/ns/"}]
                    :graph   [{:id          :ex/alice,
                               :type        :ex/User,
                               :schema/name "Alice"
                               :schema/age  42}
                              {:id          :ex/bob,
                               :type        :ex/User,
                               :schema/name "Bob"
                               :schema/age  22}]})]
      (is (= ["Alice" "Bob"]
             @(fluree/query db
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select '?name
                             :where  {:schema/name '?name}})))
      "Inserted data should be retrievable.")))
