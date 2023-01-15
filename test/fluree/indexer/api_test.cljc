(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]))

(deftest indexer
  (let [idxr (idxr/start {;; :reindex-min-bytes 0
                          :idxr/store-config {:store/method :memory}})

        db0-address (idxr/init idxr "indexertest" {})

        ;; two different stages onto the same db
        db1-summary (idxr/stage idxr db0-address {"@context" {"me" "http://dan.com/"}
                                                  "@id" "me:dan"
                                                  "me:prop1" "bar"})
        db2-summary (idxr/stage idxr db0-address {"@context" {"me" "http://dan.com/"}
                                                  "@id" "me:dan"
                                                  "me:prop2" "DIFFERENT!"})
        db0-results (idxr/query idxr db0-address {:select ["?s" "?p" "?o"] :where [["?s" "?p" "?o"]]})
        db1-results (idxr/query idxr (:db/address db1-summary)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        db2-results (idxr/query idxr (:db/address db2-summary)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]

    (is (= "fluree:db:memory:indexertest/t/init"
           db0-address))
    (is (= [] db0-results))
    (is (= {:db/v 0
            :db/t 1
            :db/address "fluree:db:memory:indexertest/t/97932cb8c1fe5eed05782ecaea44051b3a150822709d122e54ac4a50e5ec733b"
            :db/flakes 6
            :db/size 518}
           db1-summary))
    (is (= {:db/v 0
            :db/t 1
            :db/address "fluree:db:memory:indexertest/t/40f580af1932fff674426855fe84d252f62dd6becddba4091ff1d8a77d61f6a7"
            :db/flakes 6
            :db/size 532}
           db2-summary))
    (is (not= (:db/address db1-summary)
              (:db/address db2-summary)))
    ;; prop2 isn't in db1
    (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "bar"}]
           db1-results))
    ;; prop1 isn't in db2
    (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop2" "DIFFERENT!"}]
           db2-results))))
