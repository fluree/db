(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]))

(deftest indexer
  (let [idxr (idxr/start {:idxr/store-config {:store/method :memory}})

        db0-address (idxr/init idxr "test1" {})
        ;; two different stages onto the same db
        db1-info (idxr/stage idxr db0-address {"@context" {"me" "http://dan.com/"}
                                               "@id"      "me:dan"
                                               "me:prop1" "bar"})
        db2-info (idxr/stage idxr db0-address {"@context" {"me" "http://dan.com/"}
                                               "@id"      "me:dan"
                                               "me:prop2" "DIFFERENT!"})
        db0-results (idxr/query idxr db0-address {:select ["?s" "?p" "?o"] :where [["?s" "?p" "?o"]]})
        db1-results (idxr/query idxr (:db/address db1-info)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        db2-results (idxr/query idxr (:db/address db2-info)
                                {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]
    (is (= "fluree:db:memory:test1/init"
           db0-address))
    (is (= [] db0-results))
    ;; address has a random-uuid, not deterministic
    (is (= {:db/t 1
            :db/flakes 6
            :db/size 518
            :db/assert []
            :db/retract []}
           (dissoc db1-info :db/address)))
    (is (= {:db/t 1
            :db/flakes 6
            :db/size 532
            :db/assert []
            :db/retract []}
           (dissoc db2-info :db/address)))
    (is (not= (:db/address db1-info)
              (:db/address db2-info)))
    ;; prop2 isn't in db1
    (is (= [{"@id" "http://dan.com/dan", "http://dan.com/prop1" "bar"}]
           db1-results))
    ;; prop1 isn't in db2
    (is (= [{"@id" "http://dan.com/dan", "http://dan.com/prop2" "DIFFERENT!"}]
           db2-results))))
