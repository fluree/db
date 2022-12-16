(ns fluree.publisher.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.publisher.api :as pub]))

(deftest publisher
  (with-redefs #_:clj-kondo/ignore
    [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [pub            (pub/start {:pub/store-config {:store/method :memory}})
          ledger-address (pub/init pub "testledger1" {})
          dup-ledger-err (try (pub/init pub "testledger1" {})
                              (catch Exception e
                                (ex-data e)))
          initial-ledger (pub/pull pub ledger-address)

          commit-info {:id "fluree:commit:988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
                       :type :commit
                       :commit/address "fluree:commit:memory:testledger1/commit/988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
                       :db/address "fluree:db:memory/testledger1/id"
                       :commit/hash "988d5119c56068df2f1e1f09311d50e9fdaeb019c62af6ff4430779a441a665b"
                       :commit/size 0
                       :commit/flakes 0}
          index-info  {}
          ledger1        (pub/push pub ledger-address {:commit-info commit-info
                                                       :index-info  index-info})
          ledger2        (pub/push pub ledger-address {:commit-info commit-info
                                                       :index-info  index-info})]
      (testing "init"
        (is (= "fluree:ledger:memory:testledger1/head"
               ledger-address))
        (is (= {:ledger-name    "testledger1",
                :ledger-address "fluree:ledger:memory:testledger1/head",
                :opts           {}}
               dup-ledger-err)))
      ;; these are nondeterministic because they include db-addresses, which are random
      #_(testing "pull"
          (is (= {:id "fluree:ledger:c12af9635b8e3bf4f2cade900594c0e295217af80bf4fe2f09b7cf4bbfea9279",
                  :type :ledger,
                  :ledger/address "fluree:ledger:memory:testledger1/head",
                  :ledger/name "testledger1",
                  :ledger/v 0,
                  :ledger/head
                  {:type :ledger-entry,
                   :entry/time "1970-01-01T00:00:00.00000Z",
                   :entry/db {},
                   :id "fluree:ledger-entry:a4d0604acf7ed0c3b41c51089bc07e5770956e7f3000cd9a01e06342a3565f87",
                   :entry/address "fluree:ledger-entry:memory:testledger1/entry/a4d0604acf7ed0c3b41c51089bc07e5770956e7f3000cd9a01e06342a3565f87"}}
                 (update-in initial-ledger [:ledger/head :entry/db] dissoc :db/address))))
      #_(testing "push"
          (is (= "fluree:ledger-entry:memory:testledger1/entry/7a58bf777f3e86148a53ac2c948b3f69ab8eea10efca18a29b561a21b2571d09"
                 (-> ledger1 :ledger/head :entry/address)))
          (is (= "fluree:ledger-entry:memory:testledger1/entry/81b257f9fcf00f95ce9396c13adca1ba67fd5426b6e954d3ee0cfdaeba938d7c"
                 (-> ledger2 :ledger/head :entry/address)))
          (is (= (-> ledger1 :ledger/head :ledger/address)
                 (-> ledger2 :ledger/head :ledger/previous)))))))
