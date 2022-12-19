(ns fluree.transactor.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.transactor.api :as txr]
            [fluree.store.api :as store]
            [fluree.common.identity :as ident]))

(deftest transactor
  (let [expected-commit-summary
        {:address "fluree:commit:memory:testledger1/commit/41f73e7a3279d64e6ef7a62cff2aeaf325a6ca12f1e714296681141d79557ccf",
         :hash "41f73e7a3279d64e6ef7a62cff2aeaf325a6ca12f1e714296681141d79557ccf",
         :type :commit,
         :commit/size 0,
         :commit/flakes 0,
         :commit/t 1,
         :commit/v 0,
         :commit/prev "address of previous commit"}

        expected-commit
        {:address "fluree:commit:memory:testledger1/commit/41f73e7a3279d64e6ef7a62cff2aeaf325a6ca12f1e714296681141d79557ccf",
         :hash "41f73e7a3279d64e6ef7a62cff2aeaf325a6ca12f1e714296681141d79557ccf",
         :value {:type :commit,
                 :commit/size 0,
                 :commit/flakes 0,
                 :commit/assert [],
                 :commit/retract [],
                 :commit/t 1,
                 :commit/v 0,
                 :commit/prev "address of previous commit"}}

        mem-store (store/start {:store/method :memory})
        txr       (txr/start {:txr/method :file
                              :txr/store  mem-store})

        tx          {:foo "bar"}
        tx-info     {:db/address  "fluree:db:memory/testledger1/id"
                     :db/context  {}
                     :db/t        1
                     :db/flakes   0
                     :db/size     0
                     :db/assert   []
                     :db/retract  []
                     :commit/prev "address of previous commit"
                     :txr/store   mem-store
                     :ledger/name "testledger1"}

        {:keys [address] :as commit-info} (txr/commit txr tx tx-info)
        {:keys [address/path]} (ident/address-parts address)

        committed   (-> txr :store :storage-atom deref )

        resolved    (txr/resolve txr address)]
    (testing "commit returns expected commit-info"
      (is (= expected-commit-summary
             commit-info)))
    (testing "commit writes a commit to the store"
      (is (= {path expected-commit}
             committed)))
    (testing "resolve returns the commit"
      (is (= expected-commit
             resolved)))))
