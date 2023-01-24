(ns fluree.connector.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.connector.api :as conn]
            [fluree.store.api :as store]
            [fluree.connector.model :as conn-model]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils]))

(deftest connector
  (with-redefs [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [did     (did/private->did-map test-utils/default-private-key)
          context {"ex" "https://example.com/" "f" "https://ns.flur.ee"}
          tx      {"@context" context
                   "@id"      "ex:dan"
                   "ex:foo"   "bar"}]

      (testing "shared store"
        (let [conn (conn/connect {:conn/mode  :fluree
                                  :conn/did   did
                                  :conn/trust :all
                                  :conn/store-config {:store/method :memory}})
              ledger-address    (conn/create conn "testconn")
              after-ledger-init @(-> conn :store :storage-atom)

              ledger          (conn/transact conn ledger-address tx)
              after-ledger-tx @(-> conn :store :storage-atom)

              query-results (conn/query conn ledger-address {:context context
                                                             :select  {"?s" [:*]}
                                                             :where   [["?s" "@id" "ex:dan"]]})]
          (testing "wrote ledger head, commit head, and init commit"
            (is (= "fluree:ledger:memory:head/testconn"
                   ledger-address))
            (is (= ["head/testconn"
                    "testconn/commit/head"
                    "testconn/commit/init"]
                   (sort (keys after-ledger-init)))))

          (testing "added commit and db summaries to ledger"
            (is (= {"@type"                             "https://ns.flur.ee/Ledger/",
                    "@id"                               "fluree:ledger:memory:head/testconn",
                    "https://ns.flur.ee/Ledger#name"    "testconn",
                    "https://ns.flur.ee/Ledger#address"
                    "fluree:ledger:memory:head/testconn",
                    "https://ns.flur.ee/Ledger#v"       0,
                    "https://ns.flur.ee/Ledger#context" nil,
                    "https://ns.flur.ee/Ledger#head"
                    {"@type" "https://ns.flur.ee/LedgerEntry/",
                     "https://ns.flur.ee/LedgerEntry#created"
                     "1970-01-01T00:00:00.00000Z",
                     "https://ns.flur.ee/LedgerEntry#commit"
                     {"@type"                          "https://ns.flur.ee/CommitSummary/",
                      "https://ns.flur.ee/Commit#size" 3,
                      "https://ns.flur.ee/Commit#t"    1,
                      "https://ns.flur.ee/Commit#v"    0,
                      "https://ns.flur.ee/Commit#previous"
                      "fluree:commit:memory:testconn/commit/init",
                      "https://ns.flur.ee/Commit#address"
                      "fluree:commit:memory:testconn/commit/043e4689a61e0ebc2fe7870cb65423fdb4cbd035b00f5e0237d8fe69d621d10d"},
                     "https://ns.flur.ee/LedgerEntry#db"
                     {"@type"                                 "https://ns.flur.ee/DbBlockSummary/",
                      "https://ns.flur.ee/DbBlock#v"          0,
                      "https://ns.flur.ee/DbBlock#t"          1,
                      "https://ns.flur.ee/DbBlock#size"       844,
                      "https://ns.flur.ee/DbBlock#reindexMin" 100000,
                      "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
                      "https://ns.flur.ee/DbBlock#address"
                      "fluree:db:memory:testconn/db/02c465450d5b3782690d0c19fc593d340843fe95509864dde4fff0b4936ac92a"}}}
                   ledger)))

          (testing "query results"
            (is (= [{"@id" "ex:dan", "ex:foo" "bar"}]
                   query-results)))

          (conn/close conn)))

      (testing "a-la-carte config"
        (let [conn                   (conn/connect {:conn/mode :fluree
                                                    :conn/publisher-config
                                                    {:pub/store-config {:store/method :memory}
                                                     :pub/did          did
                                                     :pub/trust        :all}
                                                    :conn/transactor-config
                                                    {:txr/store-config {:store/method :memory}
                                                     :txr/did          did
                                                     :txr/trust        :all}
                                                    :conn/indexer-config
                                                    {:idxr/store-config {:store/method :memory}
                                                     :idxr/did          did
                                                     :idxr/trust        :all}})
              ledger-address         (conn/create conn "testconn")
              txr-after-ledger-init  @(-> conn :transactor :store :storage-atom)
              pub-after-ledger-init  @(-> conn :publisher :store :storage-atom)
              idxr-after-ledger-init @(-> conn :indexer :store :storage-atom)
              ledger                 (conn/transact conn ledger-address tx)
              txr-after-ledger-tx    @(-> conn :transactor :store :storage-atom)
              pub-after-ledger-tx    @(-> conn :publisher :store :storage-atom)
              idxr-after-ledger-tx   @(-> conn :indexer :store :storage-atom)

              query-results (conn/query conn ledger-address {:context context
                                                             :select  {"?s" [:*]}
                                                             :where   [["?s" "@id" "ex:dan"]]})]
          (testing "txr init writes nothing"
            (is (= ["testconn/commit/head" "testconn/commit/init"]
                   (sort (keys txr-after-ledger-init)))))
          (testing "pub init sets head"
            (is (= ["head/testconn"]
                   (sort (keys pub-after-ledger-init)))))

          (testing "db is initialized after conn create"
            (is (= 0
                   (count idxr-after-ledger-init))))

          (testing "txr tx writes commit"
            (is (= ["testconn/commit/043e4689a61e0ebc2fe7870cb65423fdb4cbd035b00f5e0237d8fe69d621d10d"
                    "testconn/commit/head"
                    "testconn/commit/init"]
                   (sort (keys txr-after-ledger-tx)))))
          (testing "pub tx overwrites head in place"
            (is (= 1
                   (count pub-after-ledger-tx))))

          (testing "transact"
            (is (= {"@type"                             "https://ns.flur.ee/Ledger/",
                    "@id"                               "fluree:ledger:memory:head/testconn",
                    "https://ns.flur.ee/Ledger#name"    "testconn",
                    "https://ns.flur.ee/Ledger#address"
                    "fluree:ledger:memory:head/testconn",
                    "https://ns.flur.ee/Ledger#v"       0,
                    "https://ns.flur.ee/Ledger#context" nil,
                    "https://ns.flur.ee/Ledger#head"
                    {"@type" "https://ns.flur.ee/LedgerEntry/",
                     "https://ns.flur.ee/LedgerEntry#created"
                     "1970-01-01T00:00:00.00000Z",
                     "https://ns.flur.ee/LedgerEntry#commit"
                     {"@type"                          "https://ns.flur.ee/CommitSummary/",
                      "https://ns.flur.ee/Commit#size" 3,
                      "https://ns.flur.ee/Commit#t"    1,
                      "https://ns.flur.ee/Commit#v"    0,
                      "https://ns.flur.ee/Commit#previous"
                      "fluree:commit:memory:testconn/commit/init",
                      "https://ns.flur.ee/Commit#address"
                      "fluree:commit:memory:testconn/commit/043e4689a61e0ebc2fe7870cb65423fdb4cbd035b00f5e0237d8fe69d621d10d"},
                     "https://ns.flur.ee/LedgerEntry#db"
                     {"@type"                                 "https://ns.flur.ee/DbBlockSummary/",
                      "https://ns.flur.ee/DbBlock#v"          0,
                      "https://ns.flur.ee/DbBlock#t"          1,
                      "https://ns.flur.ee/DbBlock#size"       844,
                      "https://ns.flur.ee/DbBlock#reindexMin" 100000,
                      "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
                      "https://ns.flur.ee/DbBlock#address"
                      "fluree:db:memory:testconn/db/02c465450d5b3782690d0c19fc593d340843fe95509864dde4fff0b4936ac92a"}}}
                   ledger)))

          (testing "query results"
            (is (= [{"@id" "ex:dan", "ex:foo" "bar"}]
                   query-results)))
          (conn/close conn))))))
