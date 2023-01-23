(ns fluree.transactor.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.transactor.api :as txr]
            [fluree.store.api :as store]
            [fluree.common.identity :as ident]
            [fluree.common.iri :as iri]
            [fluree.common.model :as model]
            [fluree.transactor.model :as txr-model]))

(deftest transactor
  (let [mem-store   (store/start {:store/method :memory})
        txr         (txr/start {:txr/method :file
                                :txr/store  mem-store})
        ledger-name "testtransactor"

        init-address    (txr/init txr ledger-name)
        head-summary0   (txr/load txr ledger-name)
        commit-address0 (get head-summary0 iri/CommitAddress)
        init-commit     (txr/resolve txr init-address)

        commit-summary1 (txr/commit txr ledger-name {"foo" "bar"})
        commit-address1 (get commit-summary1 iri/CommitAddress)
        head-summary1   (txr/load txr ledger-name)
        commit1         (txr/resolve txr commit-address1)


        commit-summary2 (txr/commit txr ledger-name {"bar" "foo"})
        commit-address2 (get commit-summary2 iri/CommitAddress)
        head-summary2   (txr/load txr ledger-name)
        commit2         (txr/resolve txr commit-address2)

        committed @(-> txr :store :storage-atom)]
    (testing "init"
      (testing "address is hardcoded"
        (is (= "fluree:commit:memory:testtransactor/commit/init"
               init-address)))
      (testing "address can be loaded"
        (is (= init-address commit-address0)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/CommitSummary head-summary0)))
      (testing "commit summary has expected data"
        (is (= {"@type"                          "https://ns.flur.ee/CommitSummary/",
                "https://ns.flur.ee/Commit#size" 0,
                "https://ns.flur.ee/Commit#t"    0,
                "https://ns.flur.ee/Commit#v"    0,
                "https://ns.flur.ee/Commit#address"
                "fluree:commit:memory:testtransactor/commit/init"}
               head-summary0)))
      (testing "commit has expected shape"
        (is (model/valid? txr-model/Commit init-commit)))
      (testing "commit has expected data"
        (is (= {"@type"                          "https://ns.flur.ee/Commit/",
                "https://ns.flur.ee/Commit#tx"   nil,
                "https://ns.flur.ee/Commit#size" 0,
                "https://ns.flur.ee/Commit#t"    0,
                "https://ns.flur.ee/Commit#v"    0}
               init-commit))))

    (testing "commit1"
      (testing "address is deterministic"
        (is (= "fluree:commit:memory:testtransactor/commit/816230d96193c439485fba615a34884bd21c15a3c044900101e3aa65af597712"
               commit-address1)))
      (testing "address can be loaded"
        (is (= commit-summary1 head-summary1)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/CommitSummary head-summary1)))
      (testing "commit summary previous is accurate"
        (is (= commit-address0 (get commit-summary1 iri/CommitPrevious))))
      (testing "commit t is accurate"
        (is (= (inc (get head-summary0 iri/CommitT))
               (get head-summary1 iri/CommitT))))
      (testing "commit has expected shape"
        (is (model/valid? txr-model/Commit commit1))))

    (testing "commit2"
      (testing "address is deterministic"
        (is (= "fluree:commit:memory:testtransactor/commit/dec362f4dc7d70822c225ac968ced19ea7d82311172979d668a12432b08d0a69"
               commit-address2)))
      (testing "address can be loaded"
        (is (= commit-summary2 head-summary2)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/CommitSummary head-summary2)))
      (testing "commit summary previous is accurate"
        (is (= commit-address1 (get commit-summary2 iri/CommitPrevious))))
      (testing "commit t is accurate"
        (is (= (inc (get head-summary1 iri/CommitT))
               (get head-summary2 iri/CommitT))))
      (testing "commit has expected shape"
        (is (model/valid? txr-model/Commit commit2))))
    (testing "storage"
      (is (= ["testtransactor/commit/816230d96193c439485fba615a34884bd21c15a3c044900101e3aa65af597712"
              "testtransactor/commit/dec362f4dc7d70822c225ac968ced19ea7d82311172979d668a12432b08d0a69"
              "testtransactor/commit/head"
              "testtransactor/commit/init"]
             (sort (keys committed)))))))
