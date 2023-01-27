(ns fluree.transactor.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.transactor.api :as txr]
            [fluree.store.api :as store]
            [fluree.common.identity :as ident]
            [fluree.common.iri :as iri]
            [fluree.common.model :as model]
            [fluree.transactor.model :as txr-model]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils]))

(deftest transactor
  (let [mem-store   (store/start {:store/method :memory})
        txr         (txr/start {:txr/did   (did/private->did-map test-utils/default-private-key)
                                :txr/trust :all
                                :txr/store mem-store})
        ledger-name "testtransactor"

        init-address    (txr/init txr ledger-name)
        head-summary0   (txr/head txr ledger-name)
        tx-address0     (get head-summary0 iri/TxHeadAddress)
        init-tx-summary (txr/resolve txr init-address)

        tx-head1    (txr/transact txr ledger-name {"foo" "bar"})
        tx-address1 (get tx-head1 iri/TxHeadAddress)
        head1       (txr/head txr ledger-name)
        tx-summary1 (txr/resolve txr tx-address1)


        tx-head2    (txr/transact txr ledger-name {"bar" "foo"})
        tx-address2 (get tx-head2 iri/TxHeadAddress)
        head2       (txr/head txr ledger-name)
        tx-summary2 (txr/resolve txr tx-address2)

        committed @(-> txr :store :storage-atom)]
    (testing "init"
      (testing "address is hardcoded"
        (is (= "fluree:tx-summary:memory:testtransactor/tx-summary/init"
               init-address)))
      (testing "address can be loaded"
        (is (= init-address tx-address0)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/TxHead head-summary0)))
      (testing "tx-head has expected data"
        (is (= {"@type" "https://ns.flur.ee/TxHead/",
                "https://ns.flur.ee/TxSummary#txAddress" "",
                "https://ns.flur.ee/TxSummary#txId"
                "5da3a4c7f117944275b4c8629c4916403625d5a4a6573a01ecb03f0e9d2edbe6",
                "https://ns.flur.ee/TxSummary#size" 0,
                "https://ns.flur.ee/TxSummary#v" 0,
                "https://ns.flur.ee/TxHead#address"
                "fluree:tx-summary:memory:testtransactor/tx-summary/init"}
               head-summary0)))
      (testing "tx-summary has expected shape"
        (is (model/valid? txr-model/TxSummary init-tx-summary)))
      (testing "tx-summary has expected data"
        (is (= {"@type" "https://ns.flur.ee/TxSummary/",
                "https://ns.flur.ee/TxSummary#txAddress" "",
                "https://ns.flur.ee/TxSummary#tx" nil,
                "https://ns.flur.ee/TxSummary#txId"
                "5da3a4c7f117944275b4c8629c4916403625d5a4a6573a01ecb03f0e9d2edbe6",
                "https://ns.flur.ee/TxSummary#size" 0,
                "https://ns.flur.ee/TxSummary#v" 0}
               init-tx-summary))))

    (testing "tx-summary1"
      (testing "address is deterministic"
        (is (= "fluree:tx-summary:memory:testtransactor/tx-summary/5a565475717f95abff284b0b14a237816df6ebb1c834104e1974725d0a42e7f7"
               tx-address1)))
      (testing "address can be loaded"
        (is (= tx-head1 head1)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/TxHead head1)))
      (testing "commit summary previous is accurate"
        (is (= tx-address0 (get tx-summary1 iri/TxSummaryPrevious))))
      (testing "commit has expected shape"
        (is (model/valid? txr-model/TxSummary tx-summary1))))

    (testing "tx-summary2"
      (testing "address is deterministic"
        (is (= "fluree:tx-summary:memory:testtransactor/tx-summary/07e084158fba01f576533efce3734b1afbdaa87cf6f8b2f09160f2c519d17a93"
               tx-address2)))
      (testing "head address can be resolved"
        (is (= tx-head2 head2)))
      (testing "commit summary has expected shape"
        (is (model/valid? txr-model/TxHead head2)))
      (testing "commit summary previous is accurate"
        (is (= tx-address1 (get tx-summary2 iri/TxSummaryPrevious))))
      (testing "commit has expected shape"
        (is (model/valid? txr-model/TxSummary tx-summary2))))
    (testing "storage"
      (is (= ["testtransactor/tx-summary/07e084158fba01f576533efce3734b1afbdaa87cf6f8b2f09160f2c519d17a93"
              "testtransactor/tx-summary/5a565475717f95abff284b0b14a237816df6ebb1c834104e1974725d0a42e7f7"
              "testtransactor/tx-summary/HEAD"
              "testtransactor/tx-summary/init"]
             (sort (keys committed)))))))
