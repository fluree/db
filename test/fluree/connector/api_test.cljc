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
        (let [conn              (conn/connect {:conn/mode         :fluree
                                               :conn/did          did
                                               :conn/trust        :all
                                               :conn/store-config {:store/method :memory}})
              ledger-address    (conn/create conn "testconn")
              after-ledger-init @(-> conn :store :storage-atom)

              subscription-result (atom [])
              subscription-cb     (fn [block opts] (swap! subscription-result conj [block opts]))
              subscription-key    (conn/subscribe conn ledger-address subscription-cb {:authClaims {}})

              ledger          (conn/transact conn ledger-address tx)
              after-ledger-tx @(-> conn :store :storage-atom)

              query-results (conn/query conn ledger-address {:context context
                                                             :select  {"?s" [:*]}
                                                             :where   [["?s" "@id" "ex:dan"]]})
              everything    (conn/query conn ledger-address {:context context
                                                             :select  ["?s" "?p" "?o"]
                                                             :where   [["?s" "?p" "?o"]]})]
          (testing "wrote ledger head, commit head, and init commit"
            (is (= "fluree:ledger:memory:head/testconn"
                   ledger-address))
            (is (= ["head/testconn"
                    "testconn/tx-summary/HEAD"
                    "testconn/tx-summary/init"]
                   (sort (keys after-ledger-init)))))
          (testing "subscription was called"
            (is (= [[{"https://ns.flur.ee/DbBlock#reindexMin" 100000,
                      "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
                      "https://ns.flur.ee/DbBlock#size" 844,
                      "https://ns.flur.ee/DbBlock#v" 0,
                      "https://ns.flur.ee/DbBlock#assert"
                      [{"https://example.com/foo" "bar",
                        "@id" "https://example.com/dan"}],
                      "https://ns.flur.ee/DbBlock#txId"
                      "feae031efbec78a61d38c2d4bdd6f23ac4e287c95f9c142fc93e1e977675f212",
                      "https://ns.flur.ee/DbBlock#retract" [],
                      "@type" "https://ns.flur.ee/DbBlock/",
                      "https://ns.flur.ee/DbBlock#t" 1}
                     {:authClaims {}}]]
                   @subscription-result)))

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
                     {"@type"                                  "https://ns.flur.ee/TxHead/",
                      "https://ns.flur.ee/TxSummary#txAddress" "",
                      "https://ns.flur.ee/TxSummary#txId"
                      "feae031efbec78a61d38c2d4bdd6f23ac4e287c95f9c142fc93e1e977675f212",
                      "https://ns.flur.ee/TxSummary#size"      3,
                      "https://ns.flur.ee/TxSummary#v"         0,
                      "https://ns.flur.ee/TxSummary#previous"
                      "fluree:tx-summary:memory:testconn/tx-summary/init",
                      "https://ns.flur.ee/TxHead#address"
                      "fluree:tx-summary:memory:testconn/tx-summary/fef7fac7e4979ca2e917304de3480d384b07c96b1fad1ee91b5d41d3fa514df8"},
                     "https://ns.flur.ee/LedgerEntry#db"
                     {"https://ns.flur.ee/DbBlock#reindexMin" 100000,
                      "https://ns.flur.ee/DbBlock#address"
                      "fluree:db:memory:testconn/db/263d0466196176ea76604db350408922df88698e42e3c79b283dce70d26114c7",
                      "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
                      "https://ns.flur.ee/DbBlock#size"       844,
                      "https://ns.flur.ee/DbBlock#v"          0,
                      "https://ns.flur.ee/DbBlock#txId"
                      "feae031efbec78a61d38c2d4bdd6f23ac4e287c95f9c142fc93e1e977675f212",
                      "@type"                                 "https://ns.flur.ee/DbBlockSummary/",
                      "https://ns.flur.ee/DbBlock#t"          1}}}
                   ledger)))

          (testing "query results"
            (is (= [{"@id" "ex:dan" "ex:foo" "bar"}]
                   query-results))
            ;; TODO: create a vocab for tx flakes.
            (is (= [["ex:dan" "@id" "https://example.com/dan"]
                    ["ex:dan" "ex:foo" "bar"]
                    ["ex:foo" "@id" "https://example.com/foo"]
                    ["http://www.w3.org/2000/01/rdf-schema#Class" "@id" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "@id" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
                    ["@id" "@id" "@id"]]
                   everything)))

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
            (is (= ["testconn/tx-summary/HEAD" "testconn/tx-summary/init"]
                   (sort (keys txr-after-ledger-init)))))
          (testing "pub init sets head"
            (is (= ["head/testconn"]
                   (sort (keys pub-after-ledger-init)))))

          (testing "db is initialized after conn create"
            (is (= 0
                   (count idxr-after-ledger-init))))

          (testing "txr tx writes commit"
            (is (= ["testconn/tx-summary/HEAD"
                    "testconn/tx-summary/fef7fac7e4979ca2e917304de3480d384b07c96b1fad1ee91b5d41d3fa514df8"
                    "testconn/tx-summary/init"]
                   (sort (keys txr-after-ledger-tx)))))
          (testing "pub tx overwrites head in place"
            (is (= 1
                   (count pub-after-ledger-tx))))

          (testing "transact"
            (is (= {"@type" "https://ns.flur.ee/Ledger/"
                    "@id" "fluree:ledger:memory:head/testconn"
                    "https://ns.flur.ee/Ledger#name" "testconn"
                    "https://ns.flur.ee/Ledger#address"
                    "fluree:ledger:memory:head/testconn"
                    "https://ns.flur.ee/Ledger#v" 0
                    "https://ns.flur.ee/Ledger#context" nil
                    "https://ns.flur.ee/Ledger#head"
                    {"@type" "https://ns.flur.ee/LedgerEntry/"
                     "https://ns.flur.ee/LedgerEntry#created"
                     "1970-01-01T00:00:00.00000Z"
                     "https://ns.flur.ee/LedgerEntry#commit"
                     {"@type" "https://ns.flur.ee/TxHead/"
                      "https://ns.flur.ee/TxSummary#txAddress" ""
                      "https://ns.flur.ee/TxSummary#txId"
                      "feae031efbec78a61d38c2d4bdd6f23ac4e287c95f9c142fc93e1e977675f212"
                      "https://ns.flur.ee/TxSummary#size" 3
                      "https://ns.flur.ee/TxSummary#v" 0
                      "https://ns.flur.ee/TxSummary#previous"
                      "fluree:tx-summary:memory:testconn/tx-summary/init"
                      "https://ns.flur.ee/TxHead#address"
                      "fluree:tx-summary:memory:testconn/tx-summary/fef7fac7e4979ca2e917304de3480d384b07c96b1fad1ee91b5d41d3fa514df8"}
                     "https://ns.flur.ee/LedgerEntry#db"
                     {"https://ns.flur.ee/DbBlock#reindexMin" 100000
                      "https://ns.flur.ee/DbBlock#address"
                      "fluree:db:memory:testconn/db/263d0466196176ea76604db350408922df88698e42e3c79b283dce70d26114c7"
                      "https://ns.flur.ee/DbBlock#reindexMax" 1000000
                      "https://ns.flur.ee/DbBlock#size" 844
                      "https://ns.flur.ee/DbBlock#v" 0
                      "https://ns.flur.ee/DbBlock#txId"
                      "feae031efbec78a61d38c2d4bdd6f23ac4e287c95f9c142fc93e1e977675f212"
                      "@type" "https://ns.flur.ee/DbBlockSummary/"
                      "https://ns.flur.ee/DbBlock#t" 1}}}
                   ledger)))

          (testing "query results"
            (is (= [{"@id" "ex:dan" "ex:foo" "bar"}]
                   query-results)))
          (conn/close conn))))))
