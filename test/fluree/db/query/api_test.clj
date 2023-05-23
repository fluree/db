(ns fluree.db.query.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.query.range :refer [index-range]]
            [fluree.db.util.async :refer [<??]]))

(deftest ^:integration fuel-test
  (testing "fuel tracking"
    (let [conn        (test-utils/create-conn)
          people      (test-utils/load-people conn)
          db          (fluree/db people)
          flake-total (-> db
                          (index-range :spot)
                          <??
                          count)
          query       '{:select [?s ?p ?o]
                        :where  [[?s ?p ?o]]}]
      (testing "queries not returning "
        (let [sut    @(fluree/query db query)]
          (is (nil? (:fuel sut))
              "Reports no fuel")))
      (testing "queries returning metadata"
        (let [query* (assoc-in query [:opts :meta] true)
              sut    @(fluree/query db query*)]
          (is (= flake-total (:fuel sut))
              "Reports that all flakes were traversed"))))))
