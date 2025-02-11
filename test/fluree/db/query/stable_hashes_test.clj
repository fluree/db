(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest stable-hashes-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "stable-commit-id")
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db0    @(fluree/stage
                   (fluree/db ledger)
                   {"@context" ["https://ns.flur.ee" context]
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
        (is (= "fluree:commit:sha256:bsclmgntysvjv4kfq223egs537wrcaj3jotgnd4tlrgravfq4xkd"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://6b3bf5de06b84c05858b474a62e90d49208196bd1f7031cf553131d64ce49726"
               (get-in db1 [:commit :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bvktsmao5ivreittrb4scd3hkc4qkefhqg42va3npk64dbmss4qt"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://8845433666a9ff813ed629b2083ca337bfb15bb9969ef2ab6a6ee660014963e9"
               (get-in db1 [:commit :data :address])))))))
