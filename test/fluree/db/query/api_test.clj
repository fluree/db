(ns fluree.db.query.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.query.range :refer [index-range]]
            [fluree.db.util.async :refer [<??]]))

(deftest ^:integration fuel-test
  (testing "fuel tracking"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "test/fuel-tracking"
                                 {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db0    (fluree/db ledger)]
      (testing "transactions"
        (testing "with the `:meta` option"
          (let [response    @(fluree/stage db0 test-utils/people {:meta true})
                db          (:result response)
                flake-total (count (<?? (index-range db :spot)))]
            (is (= flake-total (:fuel response))
                "Reports fuel for all the generated flakes")))
        (testing "without the `:meta` option"
          (let [response    @(fluree/stage db0 test-utils/people)]
            (is (nil? (:fuel response))
                "Returns no fuel"))))
      (testing "queries"
        (let [db          @(fluree/stage db0 test-utils/people)
              flake-total (count (<?? (index-range db :spot)))
              query       '{:select [?s ?p ?o]
                            :where  [[?s ?p ?o]]}]
          (testing "queries not returning metadata"
            (let [sut @(fluree/query db query)]
              (is (nil? (:fuel sut))
                  "Reports no fuel")))
          (testing "queries returning metadata"
            (let [query* (assoc-in query [:opts :meta] true)
                  sut    @(fluree/query db query*)]
              (is (= flake-total (:fuel sut))
                  "Reports that all flakes were traversed"))))))))
