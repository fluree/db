(ns fluree.publisher.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.publisher.api :as pub]))

(deftest publisher
  (with-redefs #_:clj-kondo/ignore
    [fluree.common.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
    (let [pub             (pub/start {:pub/store-config {:store/method :memory}})
          ledger-address  (pub/init pub "testpub1" {})
          ledger-address2 (pub/init pub "testpub2" {})
          ledger-address3 (pub/init pub "testpub3" {})

          dup-ledger-err (try (pub/init pub "testpub1" {})
                              (catch Exception e
                                (ex-data e)))
          initial-ledger (pub/pull pub ledger-address)

          commit-info {:id             "fluree:commit:<id>"
                       :type           :commit
                       :commit/address "fluree:commit:memory:testpub1/commit/<id>"
                       :db/address     "fluree:db:memory/testpub1/<id>"
                       :commit/hash    "<id>"
                       :commit/size    0
                       :commit/flakes  0}
          index-info  {}
          ledger1     (pub/push pub ledger-address {:commit-info commit-info
                                                    :index-info  index-info})
          ledger2     (pub/push pub ledger-address {:commit-info commit-info
                                                    :index-info  index-info})]
      (testing "init"
        (is (= "fluree:ledger:memory:head/testpub1"
               ledger-address))
        (is (= {:ledger-name    "testpub1",
                :ledger-address "fluree:ledger:memory:head/testpub1",
                :opts           {}}
               dup-ledger-err)))

      (testing "list"
        (is (= ["fluree:ledger:1813e4b2b3ac82defd866f4d6110acf140e9ab03d2a83f922e4ba5810cc827b5"
                "fluree:ledger:8e247c707225fafa2833118987104ce471fa565e1339eb6c696da16caf81cf87"
                "fluree:ledger:1e4077c50cec99bd83ebce142d647ef3a2b9f20f06e0458407b174cc0588850e"]
               ;; id is the hash of the rest of the ledger data structure
               (map :id (pub/list pub)))))

      (testing "pull"
        (is (= {:id "fluree:ledger:1813e4b2b3ac82defd866f4d6110acf140e9ab03d2a83f922e4ba5810cc827b5",
                :type :ledger,
                :ledger/address "fluree:ledger:memory:head/testpub1",
                :ledger/name "testpub1",
                :ledger/v 0,
                :ledger/head
                {:type :ledger-entry,
                 :entry/time "1970-01-01T00:00:00.00000Z",
                 :entry/db-summary #:db{:address nil},
                 :id "fluree:ledger-entry:18ca87d2aa39e4ca91776a873109236ac597ba4ba2a8fad7a46cd059a4fab22a",
                 :entry/address "fluree:ledger-entry:memory:testpub1/entry/18ca87d2aa39e4ca91776a873109236ac597ba4ba2a8fad7a46cd059a4fab22a"}}
               initial-ledger)))
      (testing "push"
        (is (= "fluree:ledger-entry:memory:testpub1/entry/144333f454be9f1a433654fd8efc9ed851e78d392d713fd4eabc21e76d72f804"
               (-> ledger1 :ledger/head :entry/address)))
        (is (= "fluree:ledger-entry:memory:testpub1/entry/26f354ea8800dd2b3b3811bce98c838520494e33c29316a961883dc363c6995c"
               (-> ledger2 :ledger/head :entry/address)))
        (is (= (-> ledger1 :ledger/head :ledger/address)
               (-> ledger2 :ledger/head :ledger/previous)))))))
