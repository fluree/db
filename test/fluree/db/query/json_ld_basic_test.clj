(ns fluree.db.query.json-ld-basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-fixtures :as test]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest json-ld-basic-query
  (testing "json-ld"
    (testing "basic-query"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:select [:*]
                                         :from   :wiki/Q836821})]
        (is (= (count query-res) 1)
            "Just one result match")
        (is (= query-res [{:id                               :wiki/Q836821,
                           :rdf/type                         [:schema/Movie],
                           :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                           :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                           :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                           :schema/isBasedOn                 {:id :wiki/Q3107329}}])
            "Basic select * is working will context normalization")))))