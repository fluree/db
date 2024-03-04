(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.json-ld.api :as fluree]
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
        (is (= "fluree:commit:sha256:bkrvlgxear36ne2asqn764os64z37zz7etuhds57vljxaguoqqeh"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://9da9356cc9b1cff7c4b7984f4ac1a1b6c02c3d72b54b64538ae79e28192583b0"
               (get-in db1 [:commit :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:byqg2alei2vf5fyre2bjcdc5w5c77ejbg3h4x5gxt72ze6snnbmq"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://7aa94e177be8d03a59de148cedbb10c18203fb970451299dee03f9417942e17d"
               (get-in db1 [:commit :data :address])))))))
