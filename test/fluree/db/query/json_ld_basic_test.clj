(ns fluree.db.query.json-ld-basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration json-ld-basic-query
  (testing "json-ld"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)
          db     (fluree/db movies)]
      (testing "basic wildcard single subject query"
        (let [query-res @(fluree/query db {:select [:*]
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
        (let [query-res @(fluree/query db {:select [:id :schema/name]
                                           :from   :wiki/Q836821})]
          (is (= query-res [{:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}]))))
      (testing "basic single subject query with selectOne"
        (let [query-res @(fluree/query db {:selectOne [:id :schema/name]
                                           :from      :wiki/Q836821})]
          (is (= query-res {:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}))))
      (testing "basic single subject query with graph crawl"
        (let [query-res @(fluree/query db {:selectOne [:* {:schema/isBasedOn [:*]}]
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
          (let [query-res @(fluree/query db {:selectOne [:*]
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
          (let [query-res @(fluree/query db {:selectOne [:* {:schema/isBasedOn [:*]}]
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
                                                                                 :schema/name "Douglas Adams"}}}))))))))

(deftest ^:integration json-ld-rdf-type-query
  (testing "json-ld rdf type queries"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)
          db     (fluree/db movies)]
      (testing "basic analytical RFD type query"
        (let [query-res @(fluree/query db {:selectOne {'?s [:* {:f/role [:*]}]}
                                           :where     [['?s :rdf/type :f/DID]]})]
          (is (= (dissoc query-res :id)                       ;; :id is a DID and will be unique per DB so exclude from comparison
                 {:rdf/type [:f/DID],
                  :f/role   {:id              "fluree-root-role",
                             :rdf/type        [:f/Role],
                             :skos/definition "Default role that gives full root access to a ledger.",
                             :skos/prefLabel  "Root role",
                             :f/rules         {:id "fluree-root-rule"}}})
              "Standard bootstrap data isn't matching."))))))


(deftest ^:integration json-ld-list-order-preservation
  (testing "json-ld @container @list option"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)]
      (testing "define @list container in context"
        (let [db        @(fluree/stage movies {:context {:ex      "http://example.org/ns#"
                                                         :ex/list {"@container" "@list"}}
                                               :id      "list-test"
                                               :ex/list [42 2 88 1]})
              query-res @(fluree/query db {:context   {:ex "http://example.org/ns#"}
                                           :selectOne [:*]
                                           :from      "list-test"})]
          (is (= query-res
                 {:id      "list-test"
                  :ex/list [42 2 88 1]})
              "Order of query result is different from transaction.")))
      (testing "define @list directly on subject"
        (let [db        @(fluree/stage movies {:context {:ex "http://example.org/ns#"}
                                               :id      "list-test2"
                                               :ex/list {"@list" [42 2 88 1]}})
              query-res @(fluree/query db {:context   {:ex "http://example.org/ns#"}
                                           :selectOne [:*]
                                           :from      "list-test2"})]
          (is (= query-res
                 {:id      "list-test2"
                  :ex/list [42 2 88 1]})
              "Order of query result is different from transaction."))))))

(deftest ^:integration simple-subject-crawl-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/simple-subject-crawl" {:context {:ex "http://example.org/ns/"}})
        db     @(fluree/stage
                 ledger
                 [{:id           :ex/brian,
                   :type         :ex/User,
                   :schema/name  "Brian"
                   :ex/last      "Smith"
                   :schema/email "brian@example.org"
                   :schema/age   50
                   :ex/favNums   7}
                  {:id           :ex/alice,
                   :type         :ex/User,
                   :schema/name  "Alice"
                   :ex/last      "Smith"
                   :schema/email "alice@example.org"
                   :ex/favColor  "Green"
                   :schema/age   42
                   :ex/favNums   [42, 76, 9]}
                  {:id          :ex/cam,
                   :type        :ex/User,
                   :schema/name "Cam"
                   :ex/last     "Jones"
                   :schema/email    "cam@example.org"
                   :schema/age  34
                   :ex/favColor "Blue"
                   :ex/favNums  [5, 10]
                   :ex/friend   [:ex/brian :ex/alice]}
                  {:id          :ex/david,
                   :type        :ex/User,
                   :schema/name "David"
                   :ex/last     "Jones"
                   :schema/email    "david@example.org"
                   :schema/age  46
                   :ex/favNums  [15 70]
                   :ex/friend   [:ex/cam]}])]
    (testing "using `from`"
      (is (= [{:id :ex/brian,
               :rdf/type [:ex/User]
               :schema/name "Brian"
               :ex/last "Smith"
               :schema/email "brian@example.org"
               :schema/age 50
               :ex/favNums 7}]
             @(fluree/query db {:select [:*]
                                :from :ex/brian}))))
    (testing "using `where`"
      (testing "id"
        (is (= [{:id :ex/brian,
                 :rdf/type [:ex/User]
                 :schema/name "Brian"
                 :ex/last "Smith"
                 :schema/email "brian@example.org"
                 :schema/age 50
                 :ex/favNums 7}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :id :ex/brian]]}))))
      (testing "iri"
        (is (= [{:id :ex/david
                 :rdf/type [:ex/User]
                 :schema/name "David"
                 :ex/last "Jones"
                 :schema/email "david@example.org"
                 :schema/age 46
                 :ex/favNums [15 70]
                 :ex/friend {:id :ex/cam}}
                {:rdf/type [:ex/User]
                 :schema/email "cam@example.org"
                 :ex/favNums [5 10]
                 :schema/age 34
                 :ex/last "Jones"
                 :schema/name "Cam"
                 :id :ex/cam
                 :ex/friend [{:id :ex/brian} {:id :ex/alice}]
                 :ex/favColor "Blue"}
                {:id :ex/alice
                 :rdf/type [:ex/User]
                 :schema/name "Alice"
                 :ex/last "Smith"
                 :schema/email "alice@example.org"
                 :schema/age 42
                 :ex/favNums [9 42 76]
                 :ex/favColor "Green"}
                {:id :ex/brian
                 :rdf/type [:ex/User]
                 :schema/name "Brian"
                 :ex/last "Smith"
                 :schema/email "brian@example.org"
                 :schema/age 50
                 :ex/favNums 7}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :type :ex/User]]}))))
      (testing "tuple"
        (is (= [{:id :ex/alice
                 :rdf/type [:ex/User]
                 :schema/name "Alice"
                 :ex/last "Smith"
                 :schema/email "alice@example.org"
                 :schema/age 42
                 :ex/favNums [9 42 76]
                 :ex/favColor "Green"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :schema/name "Alice"]]})))
        (is (= [{:rdf/type [:ex/User]
                 :schema/email "cam@example.org"
                 :ex/favNums [5 10]
                 :schema/age 34
                 :ex/last "Jones"
                 :schema/name "Cam"
                 :id :ex/cam
                 :ex/friend [{:id :ex/brian} {:id :ex/alice}]
                 :ex/favColor "Blue"}
                {:id :ex/alice
                 :rdf/type [:ex/User]
                 :schema/name "Alice"
                 :ex/last "Smith"
                 :schema/email "alice@example.org"
                 :schema/age 42
                 :ex/favNums [9 42 76]
                 :ex/favColor "Green"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :ex/favColor "?color"]]})))
        (is (= [{:id :ex/alice
                 :rdf/type [:ex/User]
                 :schema/name "Alice"
                 :ex/last "Smith"
                 :schema/email "alice@example.org"
                 :schema/age 42
                 :ex/favNums [9 42 76]
                 :ex/favColor "Green"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :schema/age 42]]})))))))
