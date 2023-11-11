(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest stable-hashes-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "stable-commit-id"
                                 {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db0    @(fluree/stage2
                    (fluree/db ledger)
                   {"@context" "https://ns.flur.ee"
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
        (is (= "fluree:commit:sha256:b7mixbkldxge5oausbhahcnglopqbf6vmw4fbrh6y7753wqgbics"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://93627442aadceb049c5d5ee53d8c304269c431a3f20183dc5c2bf066ce69058e"
               (get-in db1 [:commit :address]))))
      (testing "stable default context id"
        (is (= "fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
               (get-in db1 [:commit :defaultContext :id]))))
      (testing "stable context address"
        (is (= "fluree:memory://68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
               (get-in db1 [:commit :defaultContext :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://fb15dfb3f737fca3d90e62cbd9d6ced78c16194b40e58bea2e60c4205ea5300d"
               (get-in db1 [:commit :data :address])))))))
