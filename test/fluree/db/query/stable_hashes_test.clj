(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest stable-hashes-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "stable-commit-id"
                                 {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db0    @(fluree/stage
                   (fluree/db ledger)
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
                     :schema/age   30}])
          db1    @(fluree/commit! ledger db0)]
      (testing "stable commit id"
        (is (= "fluree:commit:sha256:bkmm63rf3nrz5ghujiw2sy6duhqex5jctfspneashahjyny2jrcm"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://1c2e42c6bfdfae91c4d441c15e1d7c6c464d39c2e65b72d082244306da402938"
               (get-in db1 [:commit :address]))))
      (testing "stable default context id"
        (is (= "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
               (get-in db1 [:commit :defaultContext :id]))))
      (testing "stable context address"
        (is (= "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
               (get-in db1 [:commit :defaultContext :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bb5xl62nkyxzwyv3ey5zpnikd7k633ch6cjapphrdu75sk7zdgpkr"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://88ae1b85a97df6e9df03d08eeaf367b192ff1e2f7edb6ebb7fd0ebbe5f8933a6"
               (get-in db1 [:commit :data :address])))))))
