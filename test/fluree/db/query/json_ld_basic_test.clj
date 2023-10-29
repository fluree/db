(ns fluree.db.query.json-ld-basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration json-ld-basic-query
  (testing "json-ld"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)
          db     (fluree/db movies)]
      (testing "basic wildcard single subject query"
        (let [q         '{:select {:wiki/Q836821 [:*]}}
              query-res @(fluree/query db q)]
          (is (= [{:id                               :wiki/Q836821,
                   :type                             :schema/Movie,
                   :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                   :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                   :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                   :schema/isBasedOn                 {:id :wiki/Q3107329}}]
                 query-res)
              "Basic select * is working will context normalization")))
      (testing "basic single subject query with explicit field selection"
        (let [query-res @(fluree/query db '{:select {:wiki/Q836821 [:id :schema/name]}})]
          (is (= [{:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}] query-res))))
      (testing "basic single subject query with selectOne"
        (let [query-res @(fluree/query db '{:selectOne {:wiki/Q836821 [:id :schema/name]}})]
          (is (= {:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"} query-res))))
      (testing "basic single subject query with graph crawl"
        (let [query-res @(fluree/query db '{:selectOne {:wiki/Q836821 [:* {:schema/isBasedOn [:*]}]}})]
          (is (= {:id                               :wiki/Q836821,
                  :type                             :schema/Movie,
                  :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                  :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                  :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                  :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                     :type          :schema/Book,
                                                     :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                     :schema/isbn   "0-330-25864-8",
                                                     :schema/author {:id :wiki/Q42}}}
                 query-res))))
      (testing "basic single subject query using depth graph crawl"
        (testing "using only wildcard"
          (let [query-res @(fluree/query db '{:selectOne {:wiki/Q836821 [:*]}
                                              :depth 3})]
            (is (= {:id                               :wiki/Q836821,
                    :type                             :schema/Movie,
                    :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                    :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                       :type      :schema/Book,
                                                       :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                       :schema/isbn   "0-330-25864-8",
                                                       :schema/author {:id          :wiki/Q42,
                                                                       :type    :schema/Person,
                                                                       :schema/name "Douglas Adams"}}}
                   query-res))))
        (testing "using graph sub-selection"
          (let [query-res @(fluree/query db '{:selectOne {:wiki/Q836821 [:* {:schema/isBasedOn [:*]}]}
                                              :depth 3})]
            (is (= {:id                               :wiki/Q836821,
                    :type                             :schema/Movie,
                    :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                    :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                       :type      :schema/Book,
                                                       :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                       :schema/isbn   "0-330-25864-8",
                                                       :schema/author {:id          :wiki/Q42,
                                                                       :type    :schema/Person,
                                                                       :schema/name "Douglas Adams"}}}
                   query-res))))))))

(deftest ^:integration json-ld-rdf-type-query
  (testing "json-ld rdf type queries"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)
          db     (fluree/db movies)]
      (testing "basic analytical RFD type query"
        (let [query-res @(fluree/query db '{:select {?s [:* {:schema/isBasedOn [:*]}]}
                                            :where  [[?s :type :schema/Movie]]})]
          (is (= [{:id :wiki/Q2875,
                   :type :schema/Movie,
                   :schema/disambiguatingDescription "1939 film by Victor Fleming",
                   :schema/isBasedOn {:id :wiki/Q2870,
                                      :type :schema/Book,
                                      :schema/author {:id :wiki/Q173540},
                                      :schema/isbn "0-582-41805-4",
                                      :schema/name "Gone with the Wind"},
                   :schema/name "Gone with the Wind",
                   :schema/titleEIDR "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4"}
                  {:id                               :wiki/Q230552,
                   :type                         :schema/Movie,
                   :schema/name                      "Back to the Future Part III",
                   :schema/disambiguatingDescription "1990 film by Robert Zemeckis",
                   :schema/titleEIDR                 "10.5240/15F9-F913-FF25-8041-E798-O"}
                  {:id                :wiki/Q109331, :type :schema/Movie,
                   :schema/name       "Back to the Future Part II",
                   :schema/titleEIDR  "10.5240/5DA5-C386-2911-7E2B-1782-L",
                   :schema/followedBy {:id :wiki/Q230552}}
                  {:id                               :wiki/Q91540,
                   :type                         :schema/Movie,
                   :schema/name                      "Back to the Future",
                   :schema/disambiguatingDescription "1985 film by Robert Zemeckis",
                   :schema/titleEIDR                 "10.5240/09A3-1F6E-3538-DF46-5C6F-I",
                   :schema/followedBy                {:id :wiki/Q109331}}
                  {:id                               :wiki/Q836821, :type :schema/Movie,
                   :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                   :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                   :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                   :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                      :type      :schema/Book,
                                                      :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                      :schema/isbn   "0-330-25864-8",
                                                      :schema/author {:id :wiki/Q42}}}]                                  ;; :id is a DID and will be unique per DB so exclude from comparison
                 query-res)
              "Standard bootstrap data isn't matching."))))))


(deftest ^:integration json-ld-list-order-preservation
  (testing "json-ld @container @list option"
    (let [conn   (test-utils/create-conn)
          movies (test-utils/load-movies conn)]
      (testing "define @list container in context"
        (let [db        @(fluree/stage (fluree/db movies)
                                       {:context {:id      "@id"
                                                  :ex      "http://example.org/ns#"
                                                  :ex/list {"@container" "@list"}}
                                        :id      "list-test"
                                        :ex/list [42 2 88 1]})
              query-res @(fluree/query db '{:context   ["" {:ex "http://example.org/ns#"}]
                                            :selectOne {?s [:*]},
                                            :where     [[?s :id "list-test"]]})]
          (is (= {:id      "list-test"
                  :ex/list [42 2 88 1]}
                 query-res)
              "Order of query result is different from transaction.")))
      (testing "define @list directly on subject"
        (let [db        @(fluree/stage (fluree/db movies)
                                       {:context {:id      "@id"
                                                  :ex      "http://example.org/ns#"}
                                        :id      "list-test2"
                                        :ex/list {"@list" [42 2 88 1]}})
              query-res @(fluree/query db '{:context   ["" {:ex "http://example.org/ns#"}],
                                            :selectOne {?s [:*]},
                                            :where     [[?s :id "list-test2"]]})]
          (is (= {:id      "list-test2"
                  :ex/list [42 2 88 1]}
                 query-res)
              "Order of query result is different from transaction."))))))

(deftest ^:integration simple-subject-crawl-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/simple-subject-crawl" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
                  [{:id           :ex/brian,
                    :type         :ex/User,
                    :schema/name  "Brian"
                    :ex/last      "Smith"
                    :schema/email "brian@example.org"
                    :schema/age   50
                    :ex/favColor  "Green"
                    :ex/favNums   7}
                   {:id           :ex/alice,
                    :type         :ex/User,
                    :schema/name  "Alice"
                    :ex/last      "Smith"
                    :schema/email "alice@example.org"
                    :ex/favColor  "Green"
                    :schema/age   42
                    :ex/favNums   [42, 76, 9]}
                   {:id           :ex/cam,
                    :type         :ex/User,
                    :schema/name  "Cam"
                    :ex/last      "Jones"
                    :schema/email "cam@example.org"
                    :schema/age   34
                    :ex/favColor  "Blue"
                    :ex/favNums   [5, 10]
                    :ex/friend    [:ex/brian :ex/alice]}
                   {:id           :ex/david,
                    :type         :ex/User,
                    :schema/name  "David"
                    :ex/last      "Jones"
                    :schema/email "david@example.org"
                    :schema/age   46
                    :ex/favNums   [15 70]
                    :ex/friend    [:ex/cam]}])]
    (testing "using `where`"
      (testing "id"
        ;;TODO not getting reparsed as ssc
        (is (= [{:id           :ex/brian,
                 :type     :ex/User
                 :schema/name  "Brian"
                 :ex/last      "Smith"
                 :schema/email "brian@example.org"
                 :schema/age   50
                 :ex/favColor  "Green"
                 :ex/favNums   7}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :id :ex/brian]]}))))
      ;;TODO not getting reparsed as ssc
      (testing "iri"
        (is (= [{:id           :ex/david
                 :type     :ex/User
                 :schema/name  "David"
                 :ex/last      "Jones"
                 :schema/email "david@example.org"
                 :schema/age   46
                 :ex/favNums   [15 70]
                 :ex/friend    {:id :ex/cam}}
                {:type     :ex/User
                 :schema/email "cam@example.org"
                 :ex/favNums   [5 10]
                 :schema/age   34
                 :ex/last      "Jones"
                 :schema/name  "Cam"
                 :id           :ex/cam
                 :ex/friend    [{:id :ex/brian} {:id :ex/alice}]
                 :ex/favColor  "Blue"}
                {:id           :ex/alice
                 :type     :ex/User
                 :schema/name  "Alice"
                 :ex/last      "Smith"
                 :schema/email "alice@example.org"
                 :schema/age   42
                 :ex/favNums   [9 42 76]
                 :ex/favColor  "Green"}
                {:id           :ex/brian
                 :type     :ex/User
                 :schema/name  "Brian"
                 :ex/last      "Smith"
                 :schema/email "brian@example.org"
                 :schema/age   50
                 :ex/favColor  "Green"
                 :ex/favNums   7}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :type :ex/User]]}))))
      (testing "tuple"
        (is (= [{:id           :ex/alice
                 :type     :ex/User
                 :schema/name  "Alice"
                 :ex/last      "Smith"
                 :schema/email "alice@example.org"
                 :schema/age   42
                 :ex/favNums   [9 42 76]
                 :ex/favColor  "Green"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :schema/name "Alice"]]})))
        (is (= [{:type     :ex/User
                 :schema/email "cam@example.org"
                 :ex/favNums   [5 10]
                 :schema/age   34
                 :ex/last      "Jones"
                 :schema/name  "Cam"
                 :id           :ex/cam
                 :ex/friend    [{:id :ex/brian} {:id :ex/alice}]
                 :ex/favColor  "Blue"}
                {:id           :ex/alice
                 :type     :ex/User
                 :schema/name  "Alice"
                 :ex/last      "Smith"
                 :schema/email "alice@example.org"
                 :schema/age   42
                 :ex/favNums   [9 42 76]
                 :ex/favColor  "Green"}
                {:id           :ex/brian,
                 :type     :ex/User,
                 :ex/favNums   7,
                 :ex/favColor  "Green",
                 :schema/age   50,
                 :ex/last      "Smith",
                 :schema/email "brian@example.org",
                 :schema/name  "Brian"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :ex/favColor "?color"]]})))
        (is (= [{:id           :ex/alice
                 :type     :ex/User
                 :schema/name  "Alice"
                 :ex/last      "Smith"
                 :schema/email "alice@example.org"
                 :schema/age   42
                 :ex/favNums   [9 42 76]
                 :ex/favColor  "Green"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :schema/age 42]]})))
        (is (= [{:id           :ex/alice,
                 :type     :ex/User,
                 :ex/favNums   [9 42 76],
                 :ex/favColor  "Green",
                 :schema/age   42,
                 :ex/last      "Smith",
                 :schema/email "alice@example.org",
                 :schema/name  "Alice"}]
               @(fluree/query db {:select {"?s" ["*"]}
                                  :where  [["?s" :schema/age 42]
                                           ["?s" :ex/favColor "Green"]]})))))))
