(ns fluree.publisher.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.common.iri :as iri]
   [fluree.common.model :as model]
   [fluree.publisher.api :as pub]
   [fluree.db.did :as did]
   [fluree.db.test-utils :as test-utils]))

(deftest publisher
  (with-redefs #_:clj-kondo/ignore
    [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [pub (pub/start {:pub/store-config {:store/method :memory}
                          :pub/did          (did/private->did-map test-utils/default-private-key)
                          :pub/trust        :all})

          ledger-name1 "testpub1"
          ledger-name2 "testpub2"
          ledger-name3 "testpub3"

          opts {:tx-address "fluree:commit:memory:testpub1/commit/init"
                :db-address "fluree:db:memory:testpub1/db/init"
                :context    {"foo" "foo:bar"}}

          init1 (pub/init pub ledger-name1 opts)
          init2 (pub/init pub ledger-name2 opts)
          init3 (pub/init pub ledger-name3 opts)

          dup-ledger-err (try (pub/init pub ledger-name1 opts)
                              (catch Exception e
                                (ex-data e)))
          initial-ledger (pub/resolve pub ledger-name1)

          tx-head {iri/type              iri/TxHead
                   iri/TxHeadAddress     "fluree:tx-summary:memory:testpub1/tx-summary/<id>"
                   iri/TxSummaryV        0
                   iri/TxSummarySize     500
                   iri/TxSummaryTxId     "<tx-hash>"
                   iri/TxSummaryPrevious "fluree:tx-summary:memory:testpub/tx-summary/<prev-id>"}
          db-head {iri/type            iri/DbBlockSummary
                   iri/DbBlockAddress  "fluree:db:memory:testpub1/db/<id>"
                   iri/DbBlockT        5
                   iri/DbBlockSize     500
                   iri/DbBlockV        0
                   iri/DbBlockPrevious "fluree:db:memory:testpub/db/<prev-id>"}
          ledger1 (pub/publish pub ledger-name1 {:tx-summary tx-head
                                                 :db-summary db-head})
          ledger2 (pub/publish pub ledger-name1 {:tx-summary tx-head
                                                 :db-summary db-head})]
      (testing "init"
        (is (= "fluree:ledger:memory:ledger/testpub1"
               (get init1 iri/LedgerAddress)))
        (is (= {:ledger-name "testpub1",
                :opts        opts}
               dup-ledger-err))
        (is (= init1
               initial-ledger))
        (is (= {"@type" "https://ns.flur.ee/Ledger/",
                "@id" "fluree:ledger:memory:ledger/testpub1",
                "https://ns.flur.ee/Ledger#name" "testpub1",
                "https://ns.flur.ee/Ledger#address"
                "fluree:ledger:memory:ledger/testpub1",
                "https://ns.flur.ee/Ledger#v" 0,
                "https://ns.flur.ee/Ledger#context" {"foo" "foo:bar"},
                "https://ns.flur.ee/Ledger#head"
                {"@type" "https://ns.flur.ee/LedgerEntry/",
                 "https://ns.flur.ee/LedgerEntry#created"
                 "1970-01-01T00:00:00.00000Z",
                 "https://ns.flur.ee/LedgerEntry#txHead" {"https://ns.flur.ee/TxHead#address" "fluree:commit:memory:testpub1/commit/init"},
                 "https://ns.flur.ee/LedgerEntry#dbHead" {"https://ns.flur.ee/DbBlock#address" "fluree:db:memory:testpub1/db/init"}}}
               initial-ledger)))

      (testing "list"
        (is (= ["fluree:ledger:memory:ledger/testpub1"
                "fluree:ledger:memory:ledger/testpub2"
                "fluree:ledger:memory:ledger/testpub3"]
               (map #(get % iri/LedgerAddress) (pub/list pub)))))

      (testing "resolve"
        (is (= {"@type" "https://ns.flur.ee/Ledger/",
                "@id" "fluree:ledger:memory:ledger/testpub1",
                "https://ns.flur.ee/Ledger#name" "testpub1",
                "https://ns.flur.ee/Ledger#address"
                "fluree:ledger:memory:ledger/testpub1",
                "https://ns.flur.ee/Ledger#v" 0,
                "https://ns.flur.ee/Ledger#context" {"foo" "foo:bar"},

                "https://ns.flur.ee/Ledger#head"
                {"@type" "https://ns.flur.ee/LedgerEntry/",
                 "https://ns.flur.ee/LedgerEntry#created" "1970-01-01T00:00:00.00000Z",

                 "https://ns.flur.ee/LedgerEntry#txHead"
                 {"@type" "https://ns.flur.ee/TxHead/",
                  "https://ns.flur.ee/TxHead#address"
                  "fluree:tx-summary:memory:testpub1/tx-summary/<id>",
                  "https://ns.flur.ee/TxSummary#v" 0,
                  "https://ns.flur.ee/TxSummary#size" 500,
                  "https://ns.flur.ee/TxSummary#txId" "<tx-hash>",
                  "https://ns.flur.ee/TxSummary#previous"
                  "fluree:tx-summary:memory:testpub/tx-summary/<prev-id>"},

                 "https://ns.flur.ee/LedgerEntry#dbHead"
                 {"@type" "https://ns.flur.ee/DbBlockSummary/",
                  "https://ns.flur.ee/DbBlock#address"
                  "fluree:db:memory:testpub1/db/<id>",
                  "https://ns.flur.ee/DbBlock#t" 5,
                  "https://ns.flur.ee/DbBlock#size" 500,
                  "https://ns.flur.ee/DbBlock#v" 0,
                  "https://ns.flur.ee/DbBlock#previous"
                  "fluree:db:memory:testpub/db/<prev-id>"}}}
               ledger2)))
      (testing "push"
        (is (model/valid? pub/Ledger ledger1))
        (is (= "fluree:tx-summary:memory:testpub1/tx-summary/<id>"
               (-> ledger1 (get iri/LedgerHead) (get iri/LedgerTxHead) (get iri/TxHeadAddress))))
        (is (= "fluree:db:memory:testpub1/db/<id>"
               (-> ledger1 (get iri/LedgerHead) (get iri/LedgerDbHead) (get iri/DbBlockAddress))))
        (is (model/valid? pub/Ledger ledger2))
        (is (= "fluree:tx-summary:memory:testpub1/tx-summary/<id>"
               (-> ledger2 (get iri/LedgerHead) (get iri/LedgerTxHead) (get iri/TxHeadAddress))))
        (is (= "fluree:db:memory:testpub1/db/<id>"
               (-> ledger2 (get iri/LedgerHead) (get iri/LedgerDbHead) (get iri/DbBlockAddress))))))))
