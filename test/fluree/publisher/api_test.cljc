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
    (let [pub             (pub/start {:pub/store-config {:store/method :memory}
                                      :pub/did (did/private->did-map test-utils/default-private-key)
                                      :pub/trust :all})
          opts            {:commit-address "fluree:commit:memory:testpub1/commit/init"
                           :db-address     "fluree:db:memory:testpub1/db/init"
                           :context        {"foo" "foo:bar"}}
          ledger-address  (pub/init pub "testpub1" opts)
          ledger-address2 (pub/init pub "testpub2" opts)
          ledger-address3 (pub/init pub "testpub3" opts)

          dup-ledger-err (try (pub/init pub "testpub1" opts)
                              (catch Exception e
                                (ex-data e)))
          initial-ledger (pub/pull pub ledger-address)

          commit-summary {iri/type           iri/CommitSummary
                          iri/CommitAddress  "fluree:commit:memory:testpub1/commit/<id>"
                          iri/CommitT        5
                          iri/CommitSize     500
                          iri/CommitV        0
                          iri/CommitPrevious "fluree:commit:memory:testpub/commit/<prev-id>"}
          db-summary     {iri/type            iri/DbBlockSummary
                          iri/DbBlockAddress  "fluree:db:memory:testpub1/db/<id>"
                          iri/DbBlockT        5
                          iri/DbBlockSize     500
                          iri/DbBlockV        0
                          iri/DbBlockPrevious "fluree:db:memory:testpub/db/<prev-id>"}
          ledger1        (pub/push pub ledger-address {:commit-summary commit-summary
                                                       :db-summary     db-summary})
          ledger2        (pub/push pub ledger-address {:commit-summary commit-summary
                                                       :db-summary     db-summary})]
      (testing "init"
        (is (= "fluree:ledger:memory:head/testpub1"
               ledger-address))
        (is (= {:ledger-name    "testpub1",
                :ledger-address "fluree:ledger:memory:head/testpub1",
                :opts           opts}
               dup-ledger-err)))

      (testing "list"
        (is (= ["fluree:ledger:memory:head/testpub1"
                "fluree:ledger:memory:head/testpub2"
                "fluree:ledger:memory:head/testpub3"]
               (map #(get % iri/LedgerAddress) (pub/list pub)))))

      (testing "pull"
        (is (= {"@type"                             "https://ns.flur.ee/Ledger/",
                "@id"                               "fluree:ledger:memory:head/testpub1",
                "https://ns.flur.ee/Ledger#name"    "testpub1",
                "https://ns.flur.ee/Ledger#address"
                "fluree:ledger:memory:head/testpub1",
                "https://ns.flur.ee/Ledger#v"       0,
                "https://ns.flur.ee/Ledger#context" {"foo" "foo:bar"},
                "https://ns.flur.ee/Ledger#head"
                {"@type" "https://ns.flur.ee/LedgerEntry/",
                 "https://ns.flur.ee/LedgerEntry#created"
                 "1970-01-01T00:00:00.00000Z",
                 "https://ns.flur.ee/LedgerEntry#commit"
                 {"https://ns.flur.ee/Commit#address" "fluree:commit:memory:testpub1/commit/init"},
                 "https://ns.flur.ee/LedgerEntry#db"
                 {"https://ns.flur.ee/DbBlock#address" "fluree:db:memory:testpub1/db/init"}}}
               initial-ledger)))
      (testing "push"
        (is (model/valid? pub/Ledger ledger1))
        (is (= "fluree:commit:memory:testpub1/commit/<id>"
               (-> ledger1 (get iri/LedgerHead) (get iri/LedgerEntryCommit) (get iri/CommitAddress))))
        (is (= "fluree:db:memory:testpub1/db/<id>"
               (-> ledger1 (get iri/LedgerHead) (get iri/LedgerEntryDb) (get iri/DbBlockAddress))))
        (is (model/valid? pub/Ledger ledger2))
        (is (= "fluree:commit:memory:testpub1/commit/<id>"
               (-> ledger2 (get iri/LedgerHead) (get iri/LedgerEntryCommit) (get iri/CommitAddress))))
        (is (= "fluree:db:memory:testpub1/db/<id>"
               (-> ledger2 (get iri/LedgerHead) (get iri/LedgerEntryDb) (get iri/DbBlockAddress))))))))
