(ns fluree.connector.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.connector.api :as conn]
            [fluree.store.api :as store]))

(deftest connector
  (with-redefs [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [context                {"ex" "https://example.com/"}
          tx                     {"@context" context
                                  "@id"      "ex:dan"
                                  "ex:foo"   "bar"}
          expected-create-result "fluree:ledger:memory:head/testconn"
          expected-tx-result     {:id "fluree:ledger:2a09ed1d61b94f89b92dc49e26059b0ceaf05a4e0f47ca413f455b05b5df0d73",
                                  :type :ledger,
                                  :ledger/address "fluree:ledger:memory:head/testconn",
                                  :ledger/name "testconn",
                                  :ledger/v 0}
          expected-query-result  [{"@id" "ex:dan", "ex:foo" "bar"}]]

      (testing "shared store"
        (let [conn              (conn/connect {:conn/store-config {:store/method :memory}})
              ledger-address    (conn/create conn "testconn")
              after-ledger-init @(-> conn :store :storage-atom)

              ledger-cred     (conn/transact conn ledger-address tx)
              after-ledger-tx @(-> conn :store :storage-atom)

              query-results (conn/query conn ledger-address {:context context
                                                             :select {"?s" [:*]}
                                                             :where [["?s" "@id" "ex:dan"]]})]
          (testing "wrote head and init entry"
            (is (= expected-create-result
                   ledger-address))
            (is (= ["head/testconn"
                    "testconn/entry/init"]
                   (sort (keys after-ledger-init)))))

          (testing "added commit and entry"
            ;; the ledger head encompasses the :db/address, which is non-deterministic
            (is (= expected-tx-result
                   (dissoc ledger-cred :ledger/head))))

          (testing "query results"
            (is (= expected-query-result
                   query-results)))

          (conn/close conn)))

      (testing "a-la-carte config"
        (let [conn                   (conn/connect {:conn/publisher-config
                                                    {:pub/store-config {:store/method :memory}}
                                                    :conn/transactor-config
                                                    {:txr/store-config {:store/method :memory}}
                                                    :conn/indexer-config
                                                    {:idxr/store-config {:store/method :memory}}})
              ledger-address         (conn/create conn "testconn")
              txr-after-ledger-init  @(-> conn :transactor :store :storage-atom)
              pub-after-ledger-init  @(-> conn :publisher :store :storage-atom)
              idxr-after-ledger-init @(-> conn :indexer :store :storage-atom)
              ledger-cred            (conn/transact conn ledger-address tx)
              txr-after-ledger-tx    @(-> conn :transactor :store :storage-atom)
              pub-after-ledger-tx    @(-> conn :publisher :store :storage-atom)
              idxr-after-ledger-tx   @(-> conn :indexer :store :storage-atom)

              query-results (conn/query conn ledger-address {:context context
                                                             :select {"?s" [:*]}
                                                             :where [["?s" "@id" "ex:dan"]]})]
          (testing "txr init writes nothing"
            (is (= {}
                   txr-after-ledger-init)))
          (testing "pub init sets head at init entry"
            (is (= ["head/testconn" "testconn/entry/init"]
                   (sort (keys pub-after-ledger-init))))
            (is (= "testconn/entry/init"
                   (-> pub-after-ledger-init (get "head/testconn")))))

          (testing "db is initialized after conn create"
            (is (= 0
                   (count idxr-after-ledger-init))))

          (testing "txr tx writes commit"
            (is (= ["testconn/commit/09b558dda8e4295f2738e700f3ee6424dd021e53d3fb4ac66295b5a20158e744"]
                   (keys txr-after-ledger-tx))))
          (testing "pub tx sets head at new entry"
            (is (= 4
                   (count pub-after-ledger-tx)))
            (is (not (nil? (get pub-after-ledger-tx (-> pub-after-ledger-tx (get "head/testconn")))))))

          (testing "transact"
            (is (= expected-tx-result
                   (dissoc ledger-cred :ledger/head))))
          (conn/close conn))))))
