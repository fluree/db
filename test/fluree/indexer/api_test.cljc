(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]
   [clojure.core.async :as async]))

(deftest indexer
  (let [idxr (idxr/start {:idxr/store-config {:store/method :memory}})

        db0-address (idxr/init idxr "indexertest" {:reindex-min-bytes 1})

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
              :db/flakes  9
              :db/size    828
              :db/opts    {:reindex-min-bytes 1 :reindex-max-bytes 1000000}
              :db/address "fluree:db:memory:indexertest/tx/f3b202f5a979e541afc3f54382dfa0b275478e4de3b6a4cde39371e1e5a81051"}
             db1-summary))
      (is (= {:db/v       0
              :db/t       -2
              :db/flakes  11
              :db/size    958
              :db/opts    {:reindex-min-bytes 1 :reindex-max-bytes 1000000}
              :db/address "fluree:db:memory:indexertest/tx/33a22485645adab934230a5f1afde653acfc18845bb5ff2fbf313a6d0278cb41"}
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

            idxr2 (idxr/start {:idxr/store store})

            loaded-summary (idxr/load idxr2 (:db/address db2-summary))
            loaded-results (idxr/query idxr (:db/address db2-summary)
                                       {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]
        (is (= ["indexertest/tx/33a22485645adab934230a5f1afde653acfc18845bb5ff2fbf313a6d0278cb41"
                "indexertest/tx/58dc43a82036f06882973cc0451312a1d3fdcda4fb890166cca5d0c2b5bb0d4e"
                "indexertest/tx/f3b202f5a979e541afc3f54382dfa0b275478e4de3b6a4cde39371e1e5a81051"]
               (sort (async/<!! (store/list store "indexertest/tx")))))
        ;; index keys are nondeterministic, so can only assert count
        (is (= 26
               (count (async/<!! (store/list store "indexertest/index")))))
        ;; TODO: merge-flakes counts the db stats differently than final-db
        #_(is (= db2-summary
               loaded-summary))
        ;; query results are the same
        (is (= db2-results
               loaded-results))))))
