(ns fluree.db.query.json-ld-basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-fixtures :as test]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest json-ld-basic-query
  (testing "json-ld"
    (testing "basic wildcard single subject query"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:select [:*]
                                         :from   :wiki/Q836821})]
        (is (= (count query-res) 1)
            "There should only be one subject in the result set")
        (is (= query-res [{:id                               :wiki/Q836821,
                           :rdf/type                         [:schema/Movie],
                           :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                           :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                           :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                           :schema/isBasedOn                 {:id :wiki/Q3107329}}])
            "Basic select * is working will context normalization")))
    (testing "basic single subject query with explicit field selection"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:select [:id :schema/name]
                                         :from   :wiki/Q836821})]
        (is (= query-res [{:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}]))))
    (testing "basic single subject query with selectOne"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:selectOne [:id :schema/name]
                                         :from      :wiki/Q836821})]
        (is (= query-res {:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}))))
    (testing "basic single subject query with graph crawl"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:selectOne [:* {:schema/isBasedOn [:*]}]
                                         :from      :wiki/Q836821})]
        (is (= query-res {:id                               :wiki/Q836821,
                          :rdf/type                         [:schema/Movie],
                          :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                          :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                          :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                          :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                             :rdf/type      [:schema/Book],
                                                             :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                             :schema/isbn   "0-330-25864-8",
                                                             :schema/author {:id :wiki/Q42}}}))))
    (testing "basic single subject query using depth graph crawl"
      (testing "using only wildcard"
        (let [movies    (test/get-ledger :test/movies)
              db        (fluree/db movies)
              query-res @(fluree/query db {:selectOne [:*]
                                           :from      :wiki/Q836821
                                           :depth     3})]
          (is (= query-res {:id                               :wiki/Q836821,
                            :rdf/type                         [:schema/Movie],
                            :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                            :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                            :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                            :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                               :rdf/type      [:schema/Book],
                                                               :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                               :schema/isbn   "0-330-25864-8",
                                                               :schema/author {:id          :wiki/Q42,
                                                                               :rdf/type    [:schema/Person],
                                                                               :schema/name "Douglas Adams"}}}))))
      (testing "using graph sub-selection"
        (let [movies    (test/get-ledger :test/movies)
              db        (fluree/db movies)
              query-res @(fluree/query db {:selectOne [:* {:schema/isBasedOn [:*]}]
                                           :from      :wiki/Q836821
                                           :depth     3})]
          (is (= query-res {:id                               :wiki/Q836821,
                            :rdf/type                         [:schema/Movie],
                            :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                            :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                            :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                            :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                               :rdf/type      [:schema/Book],
                                                               :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                               :schema/isbn   "0-330-25864-8",
                                                               :schema/author {:id          :wiki/Q42,
                                                                               :rdf/type    [:schema/Person],
                                                                               :schema/name "Douglas Adams"}}})))))))


(deftest json-ld-rdf-type-query
  (testing "json-ld rdf type queries"
    (testing "basic analytical RDF type query"
      (let [movies    (test/get-ledger :test/movies)
            db        (fluree/db movies)
            query-res @(fluree/query db {:selectOne {'?s [:* {:f/role [:*]}]}
                                         :where     [['?s :rdf/type :f/DID]]})]
        (is (= (dissoc query-res :id)                       ;; :id is a DID and will be unique per DB so exclude from comparison
               {:rdf/type [:f/DID],
                :f/role   {:id              "fluree-root-role",
                           :rdf/type        [:f/Role],
                           :skos/definition "Default role that gives full root access to a ledger.",
                           :skos/prefLabel  "Root role",
                           :f/rules         {:id "fluree-root-rule"}}})
            "Standard bootstrap data isn't matching.")))))
