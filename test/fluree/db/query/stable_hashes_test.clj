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
        (is (= "fluree:commit:sha256:bbnabt4dx7g2cetpxnpmj7yw3dwwztdo6r4pusndprm2cedpvjixm"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://4be8331e870ec61478ca8c4f0fb834f392bcd8cc341be5c081ba9bf7c70ef11c"
               (get-in db1 [:commit :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bb7yu65w6lgl7xpoic663lnws6lxeu36wyubphbyszpye5iirko7z"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://04be3eee73fc4a553f3b481bfb8867e337a8b2d76a2aea347a3b9c10c8582c6f"
               (get-in db1 [:commit :data :address])))))))
