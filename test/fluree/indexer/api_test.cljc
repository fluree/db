(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]
   [clojure.core.async :as async]))

(deftest indexer
  (let [idxr (idxr/start {:reindex-min-bytes 1
                          :idxr/store-config {:store/method :memory}})

        db0-address (idxr/init idxr "indexertest" {})

        ;; two different stages onto the same db
        db1-summary (idxr/stage idxr db0-address
                                {"@context" {"me" "http://dan.com/"}
                                 "@id" "me:dan"
                                 "me:prop1" "bar"})
        db2-summary (idxr/stage idxr (:db/address db1-summary)
                                {"@context" {"me" "http://dan.com/"}
                                 "@id" "me:dan"
                                 "me:prop2" "foo"})
        sibling-stage-summary  (idxr/stage idxr db0-address
                                           {"@context" {"me" "http://dan.com/"}
                                            "@id" "me:dan"
                                            "me:prop1" "DIFFERENT BRANCH"})
        db0-results (idxr/query idxr db0-address {:select ["?s" "?p" "?o"] :where [["?s" "?p" "?o"]]})
        db1-results (idxr/query idxr (:db/address db1-summary)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        db2-results (idxr/query idxr (:db/address db2-summary)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        sibling-stage-results (idxr/query idxr (:db/address sibling-stage-summary)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]

    (testing "initial db"
      (is (= "fluree:db:memory:indexertest/tx/init"
             db0-address))
      (is (= [] db0-results)))
    (testing "consecutive stages"
      (is (= {:db/v 0
              :db/t 1
              :db/flakes 6
              :db/size 518
              :db/address "fluree:db:memory:indexertest/tx/646b0fd9f7d067fbdc3afb8c0e60723f01a32eedae0e02be7093f6ec0c1a47c1"}
             db1-summary))
      (is (= {:db/v 0
              :db/t 2
              :db/flakes 8
              :db/size 648
              :db/address "fluree:db:memory:indexertest/tx/0eaaacd3eb8d6d938f2ea2430582b26149aecc01992ac036f35a929da75719d9"}
             db2-summary))
      (is (= [{"@id" "http://dan.com/dan"
               "http://dan.com/prop1" "bar"
               "http://dan.com/prop2" "foo"}]
             db2-results)))
    (testing "two sibling stages"
      (is (not= (:db/address db1-summary)
                (:db/address sibling-stage-summary)))

      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "bar"}]
             db1-results))
      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "DIFFERENT BRANCH"}]
             sibling-stage-results)))

    (testing "indexer persistence"
      (let [store (:store idxr)]
        (is (= ["indexertest/tx/0eaaacd3eb8d6d938f2ea2430582b26149aecc01992ac036f35a929da75719d9"
                "indexertest/tx/646b0fd9f7d067fbdc3afb8c0e60723f01a32eedae0e02be7093f6ec0c1a47c1"
                "indexertest/tx/c214cee0034979b821546250d54a4719fe9f599fd2c3624bf664012bc5db161e"]
               (sort (async/<!! (store/list store "indexertest/tx")))))
        ;; index keys are nondeterministic, so can only assert count
        (is (= 26
               (count (async/<!! (store/list store "indexertest/index")))))))))
