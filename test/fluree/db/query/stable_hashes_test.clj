(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util :as util]))

(deftest stable-hashes-test
  (with-redefs [util/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "stable-commit-id")
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db0    @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    [{:id           :ex/alice
                      :type         :ex/User
                      :schema/name  "Alice"
                      :schema/email "alice@flur.ee"
                      :schema/age   42}
                     {:id          :ex/bob,
                      :type        :ex/User,
                      :schema/name "Bob"
                      :schema/age  22}
                     {:id           :ex/jane,
                      :type         :ex/User,
                      :schema/name  "Jane"
                      :schema/email "jane@flur.ee"
                      :schema/age   30}]})
          db1    @(fluree/commit! ledger db0)]
      (testing "stable commit id"
        (is (= "fluree:commit:sha256:bbdainpfs7v2pg2yj76uzpybdfdldvvt5idlbasisuhwlrxbiqhii"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://bdainpfs7v2pg2yj76uzpybdfdldvvt5idlbasisuhwlrxbiqhii"
               (get-in db1 [:commit :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:btqomzs3uzs7dspzbs5ht4e7af7qrahnvomx4s4id7apr5jm7dxn"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://tqomzs3uzs7dspzbs5ht4e7af7qrahnvomx4s4id7apr5jm7dxn"
               (get-in db1 [:commit :data :address])))))))
