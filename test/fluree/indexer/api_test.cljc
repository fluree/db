(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]))


(def counter (atom 0))
(defn deterministic-db-address
  [db]
  (str "db-address-" (swap! counter inc)))

(deftest indexer
  (reset! counter 0)
  (with-redefs [fluree.indexer.db/create-db-address deterministic-db-address]
    (let [idxr (idxr/start {:idxr/store-config {:store/method :memory}})

          db0-address (idxr/init idxr {})
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
      (is (= "db-address-1"
             db0-address))
      (is (= [] db0-results))
      (is (= {:db/v 0,
              :db/t 1,
              :db/address "db-address-2"
              :db/flakes 6,
              :db/size 518,
              :db/context {"f" "https://ns.flur.ee/ledger#"},
              :db/assert
              [{"http://dan.com/prop1" "bar", "@id" "http://dan.com/dan"}],
              :db/retract []}
             db1-summary))
      (is (= {:db/v 0,
              :db/t 1,
              :db/address "db-address-3"
              :db/flakes 6,
              :db/size 532,
              :db/context {"f" "https://ns.flur.ee/ledger#"},
              :db/assert
              [{"http://dan.com/prop2" "DIFFERENT!",
                "@id" "http://dan.com/dan"}],
              :db/retract []}
             db2-summary))
      (is (not= (:db/address db1-summary)
                (:db/address db2-summary)))
      ;; prop2 isn't in db1
      (is (= [{"@id" "http://dan.com/dan", "http://dan.com/prop1" "bar"}]
             db1-results))
      ;; prop1 isn't in db2
      (is (= [{"@id" "http://dan.com/dan", "http://dan.com/prop2" "DIFFERENT!"}]
             db2-results)))))
