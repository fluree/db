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
        (is (= "fluree:commit:sha256:bc4pgf733mfhelp4ueeciwbfftthr4juvh2btondbmzjbvf6zpf5"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://10e1db9dc617a4d4b01a3fbdea6eb97a52f4173a8b01c2cdbb66ec05f90a41c7"
               (get-in db1 [:commit :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bbsfu5hq2mkalfpirabumzsohpwwz3legce6hie6tpb6nlqwvzigc"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://1066be3c23dc5635881c1b4ce74cd944fc96b25556bc9508f4a5c1843a4421af"
               (get-in db1 [:commit :data :address])))))))
