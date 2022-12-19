(ns fluree.publisher.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.publisher.api :as pub]))

(deftest publisher
  (with-redefs #_:clj-kondo/ignore
    [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [pub             (pub/start {:pub/store-config {:store/method :memory}})
          ledger-address  (pub/init pub "testledger1" {})
          ledger-address2 (pub/init pub "testledger2" {})
          ledger-address3 (pub/init pub "testledger3" {})

          dup-ledger-err (try (pub/init pub "testledger1" {})
                              (catch Exception e
                                (ex-data e)))
          initial-ledger (pub/pull pub ledger-address)

          commit-info {:id             "fluree:commit:<id>"
                       :type           :commit
                       :commit/address "fluree:commit:memory:testledger1/commit/<id>"
                       :db/address     "fluree:db:memory/testledger1/<id>"
                       :commit/hash    "<id>"
                       :commit/size    0
                       :commit/flakes  0}
          index-info  {}
          ledger1     (pub/push pub ledger-address {:commit-info commit-info
                                                    :index-info  index-info})
          ledger2     (pub/push pub ledger-address {:commit-info commit-info
                                                    :index-info  index-info})]
      (testing "init"
        (is (= "fluree:ledger:memory:head/testledger1"
               ledger-address))
        (is (= {:ledger-name    "testledger1",
                :ledger-address "fluree:ledger:memory:head/testledger1",
                :opts           {}}
               dup-ledger-err)))

      (testing "list"
        (is (= ["fluree:ledger:c12af9635b8e3bf4f2cade900594c0e295217af80bf4fe2f09b7cf4bbfea9279"
                "fluree:ledger:7f8676efcef43bb6eadff998e98c3c28de03d9c0d21ac0b9b4114eefd1d22e84"
                "fluree:ledger:643baa704b68c6624865425bc7cdd463d11162c84dad1a2c6e6492a7f7ced12c"]
               ;; id is the hash of the rest of the ledger data structure
               (map :id (pub/list pub)))))

      (testing "pull"
        (is (= {:id             "fluree:ledger:c12af9635b8e3bf4f2cade900594c0e295217af80bf4fe2f09b7cf4bbfea9279",
                :type           :ledger,
                :ledger/address "fluree:ledger:memory:head/testledger1",
                :ledger/name    "testledger1",
                :ledger/v       0,
                :ledger/head
                {:type       :ledger-entry,
                 :entry/time "1970-01-01T00:00:00.00000Z",
                 :entry/db   #:db{:address nil},
                 :id
                 "fluree:ledger-entry:45b9402852f7ae8a8a1f8276cf2838ff9742199deb7ecf6682e2192abb850f0e",
                 :entry/address
                 "fluree:ledger-entry:memory:testledger1/entry/45b9402852f7ae8a8a1f8276cf2838ff9742199deb7ecf6682e2192abb850f0e"}}
               initial-ledger)))
      (testing "push"
        (is (= "fluree:ledger-entry:memory:testledger1/entry/c0b542b796a25914f73a4566225a72b96af71e2c3633b01e9b22f3086c75735f"
               (-> ledger1 :ledger/head :entry/address)))
        (is (= "fluree:ledger-entry:memory:testledger1/entry/6af2a876dddda19e769d91777990418cff4749dd0a0dbe25c4a6a111870124ea"
               (-> ledger2 :ledger/head :entry/address)))
        (is (= (-> ledger1 :ledger/head :ledger/address)
               (-> ledger2 :ledger/head :ledger/previous)))))))
