(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]))

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
      (is (= "fluree:db:memory:indexertest/t/init"
             db0-address))
      (is (= [] db0-results)))
    (testing "consecutive stages"
      (is (= {:db/v 0
              :db/t 1
              :db/address "fluree:db:memory:indexertest/t/97932cb8c1fe5eed05782ecaea44051b3a150822709d122e54ac4a50e5ec733b"
              :db/flakes 6
              :db/size 518}
             db1-summary))
      (is (= {:db/v 0
              :db/t 2
              :db/address "fluree:db:memory:indexertest/t/6e1fb1b747c48d9b76ba903eb6033b8d258f4e2926f5b7e23aed3d48196eeb8b"
              :db/flakes 8
              :db/size 648}
             db2-summary))
      (is (= [{"@id" "http://dan.com/dan"
               "http://dan.com/prop1" "bar"
               "http://dan.com/prop2" "foo"}]
             db2-results))



      )
    (testing "two sibling stages"
      (is (not= (:db/address db1-summary)
                (:db/address sibling-stage-summary)))

      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "bar"}]
             db1-results))
      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "DIFFERENT BRANCH"}]
             sibling-stage-results)))

    ))
