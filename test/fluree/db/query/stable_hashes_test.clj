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
        (is (= "fluree:commit:sha256:bn7xmmt2hnsjxe7za3e663zhbdv2udnczetgspjpmvuusgtveioe"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://0af069b101e5edadf788674a0733572404bdddcb37c994bc3624c9b11d7958e6"
               (get-in db1 [:commit :address]))))
      (testing "stable default context id"
        (is (= "fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
               (get-in db1 [:commit :defaultContext :id]))))
      (testing "stable context address"
        (is (= "fluree:memory://68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
               (get-in db1 [:commit :defaultContext :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bbi3wkfjrhyuwffoyj2uuds5dxpl5akmtqtsv33tn52lifgxp64le"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://b0e96920a7245f795fd287eebd47ca03302f23025737a136dc98009d11a16d69"
               (get-in db1 [:commit :data :address])))))))
