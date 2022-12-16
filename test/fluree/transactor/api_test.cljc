(ns fluree.transactor.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.transactor.api :as txr]
            [fluree.store.api :as store]))

(deftest transactor
  (let [expected-commit-info
        {:id "fluree:commit:988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
         :commit/address "fluree:commit:memory:testledger1/commits/988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
         :commit/size 0
         :commit/flakes 0
         :commit/t 1
         :commit/v 0}

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
                     :commit/prev "fluree:commit:testledger1/abc123"
                     :txr/store   mem-store
                     :ledger/name "testledger1"}
        commit-info (txr/commit txr tx tx-info)
        committed   (-> txr :store :storage-atom deref)]
    (is (= expected-commit-info
           commit-info))
    (is (= {"testledger1/commits/988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
            {:id "fluree:commit:988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
             :type :commit
             :commit/address "fluree:commit:memory:testledger1/commits/988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
             :db/address "fluree:db:memory/testledger1/id"
             :commit/hash "988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
             :commit/size 0
             :commit/flakes 0
             :commit/tx
             #:commit{:assert []
                      :retract []
                      :context {}
                      :t 1
                      :v 0
                      :prev "fluree:commit:testledger1/abc123"}}}
           committed))))
