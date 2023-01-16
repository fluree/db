(ns fluree.transactor.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.transactor.api :as txr]
            [fluree.store.api :as store]
            [fluree.common.identity :as ident]))

(deftest transactor
  (let [tx {:foo "bar"}

        expected-commit-summary
        {:address     "fluree:commit:memory:testledger1/commit/7a38bf81f383f69433ad6e900d35b3e2385593f76a7b7ab5d4355b8ba41ee24b"
         :hash        "7a38bf81f383f69433ad6e900d35b3e2385593f76a7b7ab5d4355b8ba41ee24b"
         :commit/size 13
         :commit/t    1
         :commit/v    0
         :commit/prev "address of previous commit"}

        expected-commit
        {:address "fluree:commit:memory:testledger1/commit/7a38bf81f383f69433ad6e900d35b3e2385593f76a7b7ab5d4355b8ba41ee24b"
         :hash    "7a38bf81f383f69433ad6e900d35b3e2385593f76a7b7ab5d4355b8ba41ee24b"
         :value   {:commit/size 13
                   :commit/tx   tx
                   :commit/t    1
                   :commit/v    0
                   :commit/prev "address of previous commit"}}

        mem-store (store/start {:store/method :memory})
        txr       (txr/start {:txr/method :file
                              :txr/store  mem-store})


        tx-info {:commit/t    1
                 :commit/prev "address of previous commit"
                 :ledger/name "testledger1"}

        {:keys [address] :as commit-info} (txr/commit txr tx tx-info)
        {:keys [address/path]}            (ident/address-parts address)

        committed (-> txr :store :storage-atom deref )

        resolved (txr/resolve txr address)]
    (testing "commit returns expected commit-info"
      (is (= expected-commit-summary
             commit-info)))
    (testing "commit writes a commit to the store"
      (is (= {path expected-commit}
             committed)))
    (testing "resolve returns the commit"
      (is (= expected-commit
             resolved)))))
