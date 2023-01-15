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
        db1-summary           (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "bar"})
        db2-summary           (idxr/stage idxr (:db/address db1-summary)
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop2" "foo"})
        sibling-stage-summary (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "DIFFERENT BRANCH"})
        db0-results           (idxr/query idxr db0-address {:select ["?s" "?p" "?o"] :where [["?s" "?p" "?o"]]})
        db1-results           (idxr/query idxr (:db/address db1-summary)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        db2-results           (idxr/query idxr (:db/address db2-summary)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        sibling-stage-results (idxr/query idxr (:db/address sibling-stage-summary)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]

    (testing "initial db"
      (is (= "fluree:db:memory:indexertest/tx/init"
             db0-address))
      (is (= [] db0-results)))
    (testing "consecutive stages"
      (is (= {:db/v       0
              :db/t       -1
              :db/flakes  6
              :db/size    518
              :db/opts    {:reindex-min-bytes 1 :reindex-max-bytes 1000000}
              :db/address "fluree:db:memory:indexertest/tx/b800e024577355a8240d5ba656bddf35f0219e2f88c57b7cfd918ddc1245b4d2"}
             db1-summary))
      (is (= {:db/v       0
              :db/t       -2
              :db/flakes  8
              :db/size    648
              :db/opts    {:reindex-min-bytes 1 :reindex-max-bytes 1000000}
              :db/address "fluree:db:memory:indexertest/tx/25d49202faa4eed6d9b7f90b1636dc856666304377a283c6ccbfff74ff8dc441"}
             db2-summary))
      (is (= [{"@id"                  "http://dan.com/dan"
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
      (let [store (:store idxr)

            idxr2 (idxr/start {:reindex-min-bytes 10 :idxr/store store})

            loaded-summary (idxr/load idxr2 (:db/address db2-summary))
            loaded-results (idxr/query idxr (:db/address db2-summary)
                                       {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]
        (is (= ["indexertest/tx/25d49202faa4eed6d9b7f90b1636dc856666304377a283c6ccbfff74ff8dc441"
                "indexertest/tx/a61507f3e97d28f89d6507e701d012aedca99ae8c152f2fd090ac8d5165fa9cd"
                "indexertest/tx/b800e024577355a8240d5ba656bddf35f0219e2f88c57b7cfd918ddc1245b4d2"]
               (sort (async/<!! (store/list store "indexertest/tx")))))
        ;; index keys are nondeterministic, so can only assert count
        (is (= 26
               (count (async/<!! (store/list store "indexertest/index")))))
        (is (= db2-summary
               loaded-summary))
        (is (= db2-results
               loaded-results))))))
