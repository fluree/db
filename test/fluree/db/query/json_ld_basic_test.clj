(ns fluree.db.query.json-ld-basic-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration json-ld-basic-query
  (testing "json-ld"
    (let [conn    (test-utils/create-conn)
          movies  (test-utils/load-movies conn)
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db      movies]
      (testing "basic wildcard single subject query"
        (let [q         {:context context
                         :select  {:wiki/Q836821 [:*]}}
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
        (let [query-res @(fluree/query db {:context context
                                           :select  {:wiki/Q836821 [:id :schema/name]}})]
          (is (= [{:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"}] query-res))))
      (testing "basic single subject query with selectOne"
        (let [query-res @(fluree/query db {:context   context
                                           :selectOne {:wiki/Q836821 [:id :schema/name]}})]
          (is (= {:id :wiki/Q836821, :schema/name "The Hitchhiker's Guide to the Galaxy"} query-res))))
      (testing "basic single subject query with graph crawl"
        (let [query-res @(fluree/query db {:context   context
                                           :selectOne {:wiki/Q836821 [:* {:schema/isBasedOn [:*]}]}})]
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
          (let [query-res @(fluree/query db {:context   context
                                             :selectOne {:wiki/Q836821 [:*]}
                                             :depth     3})]
            (is (= {:id                               :wiki/Q836821,
                    :type                             :schema/Movie,
                    :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                    :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                       :type          :schema/Book,
                                                       :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                       :schema/isbn   "0-330-25864-8",
                                                       :schema/author {:id          :wiki/Q42,
                                                                       :type        :schema/Person,
                                                                       :schema/name "Douglas Adams"}}}
                   query-res))))
        (testing "using graph sub-selection"
          (let [query-res @(fluree/query db {:context   context
                                             :selectOne {:wiki/Q836821 [:* {:schema/isBasedOn [:*]}]}
                                             :depth     3})]
            (is (= {:id                               :wiki/Q836821,
                    :type                             :schema/Movie,
                    :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                    :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                       :type          :schema/Book,
                                                       :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                       :schema/isbn   "0-330-25864-8",
                                                       :schema/author {:id          :wiki/Q42,
                                                                       :type        :schema/Person,
                                                                       :schema/name "Douglas Adams"}}}
                   query-res)))))
      (testing "expanding literal nodes"
        (testing "with wildcard"
          (let [q {:context   context
                   :selectOne {:wiki/Q836821 [:* {:schema/name [:*]}]}}]
            (is (= {:type             :schema/Movie,
                    :schema/disambiguatingDescription
                    "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/isBasedOn {:id :wiki/Q3107329},
                    :schema/name
                    {:value "The Hitchhiker's Guide to the Galaxy", :type :xsd/string},
                    :schema/titleEIDR "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :id               :wiki/Q836821}
                   @(fluree/query db q))
                "returns all defined virtual properties")))
        (testing "with specific virtual properties"
          (let [q {:context   context
                   :selectOne {:wiki/Q836821 [:* {:schema/name [:type]}]}}]
            (is (= {:type             :schema/Movie,
                    :schema/disambiguatingDescription
                    "2005 British-American comic science fiction film directed by Garth Jennings",
                    :schema/isBasedOn {:id :wiki/Q3107329},
                    :schema/name      {:type :xsd/string},
                    :schema/titleEIDR "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                    :id               :wiki/Q836821}
                   @(fluree/query db q))
                "returns only the virtual properties queried for")))))))

(deftest ^:integration json-ld-rdf-type-query
  (testing "json-ld rdf type queries"
    (let [conn    (test-utils/create-conn)
          movies  (test-utils/load-movies conn)
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db      movies]
      (testing "basic analytical RFD type query"
        (let [query-res @(fluree/query db {:context context
                                           :select  '{?s [:* {:schema/isBasedOn [:*]}]}
                                           :where   '{:id   ?s
                                                      :type :schema/Movie}})]
          (is (= [{:id                :wiki/Q109331, :type :schema/Movie,
                   :schema/name       "Back to the Future Part II",
                   :schema/titleEIDR  "10.5240/5DA5-C386-2911-7E2B-1782-L",
                   :schema/followedBy {:id :wiki/Q230552}}
                  {:id                               :wiki/Q230552,
                   :type                             :schema/Movie,
                   :schema/name                      "Back to the Future Part III",
                   :schema/disambiguatingDescription "1990 film by Robert Zemeckis",
                   :schema/titleEIDR                 "10.5240/15F9-F913-FF25-8041-E798-O"}
                  {:id                               :wiki/Q2875,
                   :type                             :schema/Movie,
                   :schema/disambiguatingDescription "1939 film by Victor Fleming",
                   :schema/isBasedOn                 {:id            :wiki/Q2870,
                                                      :type          :schema/Book,
                                                      :schema/author {:id :wiki/Q173540},
                                                      :schema/isbn   "0-582-41805-4",
                                                      :schema/name   "Gone with the Wind"},
                   :schema/name                      "Gone with the Wind",
                   :schema/titleEIDR                 "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4"}
                  {:id                               :wiki/Q836821, :type :schema/Movie,
                   :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                   :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                   :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                   :schema/isBasedOn                 {:id            :wiki/Q3107329,
                                                      :type          :schema/Book,
                                                      :schema/name   "The Hitchhiker's Guide to the Galaxy",
                                                      :schema/isbn   "0-330-25864-8",
                                                      :schema/author {:id :wiki/Q42}}}
                  {:id                               :wiki/Q91540,
                   :type                             :schema/Movie,
                   :schema/name                      "Back to the Future",
                   :schema/disambiguatingDescription "1985 film by Robert Zemeckis",
                   :schema/titleEIDR                 "10.5240/09A3-1F6E-3538-DF46-5C6F-I",
                   :schema/followedBy                {:id :wiki/Q109331}}]
                 query-res)
              "Standard bootstrap data isn't matching."))))))

(deftest ^:integration json-ld-list-order-preservation
  (testing "json-ld @container @list option"
    (let [conn    (test-utils/create-conn)
          movies  (test-utils/load-movies conn)
          context [test-utils/default-context {:ex "http://example.org/ns/"}]]
      (testing "define @list container in context"
        (let [db        @(fluree/update movies
                                        {"@context" context
                                         "insert"
                                         {:context {:id      "@id"
                                                    :ex/list {"@container" "@list"}}
                                          :id      "list-test"
                                          :ex/list [42 2 88 1]}})
              query-res @(fluree/query db {:context   context
                                           :selectOne {"list-test" [:*]}})]
          (is (= {:id      "list-test"
                  :ex/list [42 2 88 1]}
                 query-res)
              "Order of query result is different from transaction.")))
      (testing "define @list directly on subject"
        (let [db        @(fluree/update movies
                                        {"@context" context
                                         "insert"
                                         {:context {:id "@id"}
                                          :id      "list-test2"
                                          :ex/list {"@list" [42 2 88 1]}}})
              query-res @(fluree/query db {:context   context,
                                           :selectOne {"list-test2" [:*]}})]
          (is (= {:id      "list-test2"
                  :ex/list [42 2 88 1]}
                 query-res)
              "Order of query result is different from transaction."))))))

(deftest ^:integration simple-subject-crawl-test
  (let [conn    (test-utils/create-conn)
        db0     @(fluree/create conn "query/simple-subject-crawl")
        context [test-utils/default-context {:ex "http://example.org/ns/"}]
        db      @(fluree/update
                  db0
                  {"@context" context
                   "insert"
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
                     :ex/friend    [:ex/cam]}]})]
    (testing "direct id"
      ;;TODO not getting reparsed as ssc
      (is (= [{:id           :ex/brian,
               :type         :ex/User
               :schema/name  "Brian"
               :ex/last      "Smith"
               :schema/email "brian@example.org"
               :schema/age   50
               :ex/favColor  "Green"
               :ex/favNums   7}]
             @(fluree/query db {:context context
                                :select  {:ex/brian ["*"]}}))))
    ;;TODO not getting reparsed as ssc
    (testing "iri from `where`"
      (is (= [{:id           :ex/alice
               :type         :ex/User
               :schema/name  "Alice"
               :ex/last      "Smith"
               :schema/email "alice@example.org"
               :schema/age   42
               :ex/favNums   [9 42 76]
               :ex/favColor  "Green"}
              {:id           :ex/brian
               :type         :ex/User
               :schema/name  "Brian"
               :ex/last      "Smith"
               :schema/email "brian@example.org"
               :schema/age   50
               :ex/favColor  "Green"
               :ex/favNums   7}
              {:type         :ex/User
               :schema/email "cam@example.org"
               :ex/favNums   [5 10]
               :schema/age   34
               :ex/last      "Jones"
               :schema/name  "Cam"
               :id           :ex/cam
               :ex/friend    [{:id :ex/alice} {:id :ex/brian}]
               :ex/favColor  "Blue"}
              {:id           :ex/david
               :type         :ex/User
               :schema/name  "David"
               :ex/last      "Jones"
               :schema/email "david@example.org"
               :schema/age   46
               :ex/favNums   [15 70]
               :ex/friend    {:id :ex/cam}}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id   "?s"
                                          :type :ex/User}}))))
    (testing "tuple"
      (is (= [{:id           :ex/alice
               :type         :ex/User
               :schema/name  "Alice"
               :ex/last      "Smith"
               :schema/email "alice@example.org"
               :schema/age   42
               :ex/favNums   [9 42 76]
               :ex/favColor  "Green"}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id          "?s"
                                          :schema/name "Alice"}})))
      (is (= [{:id           :ex/alice
               :type         :ex/User
               :schema/name  "Alice"
               :ex/last      "Smith"
               :schema/email "alice@example.org"
               :schema/age   42
               :ex/favNums   [9 42 76]
               :ex/favColor  "Green"}
              {:id           :ex/brian,
               :type         :ex/User,
               :ex/favNums   7,
               :ex/favColor  "Green",
               :schema/age   50,
               :ex/last      "Smith",
               :schema/email "brian@example.org",
               :schema/name  "Brian"}
              {:type         :ex/User
               :schema/email "cam@example.org"
               :ex/favNums   [5 10]
               :schema/age   34
               :ex/last      "Jones"
               :schema/name  "Cam"
               :id           :ex/cam
               :ex/friend    [{:id :ex/alice} {:id :ex/brian}]
               :ex/favColor  "Blue"}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id          "?s"
                                          :ex/favColor "?color"}})))

      (is (= [{:id           :ex/alice,
               :type         :ex/User,
               :ex/favColor  "Green",
               :ex/favNums   [9 42 76],
               :ex/last      "Smith",
               :schema/age   42,
               :schema/email "alice@example.org",
               :schema/name  "Alice"}
              {:id           :ex/brian,
               :type         :ex/User,
               :ex/favColor  "Green",
               :ex/favNums   7,
               :ex/last      "Smith",
               :schema/age   50,
               :schema/email "brian@example.org",
               :schema/name  "Brian"}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id          "?s"
                                          :ex/favColor "?color"}
                                :limit   2})))

      (is (= [{:id           :ex/alice
               :type         :ex/User
               :schema/name  "Alice"
               :ex/last      "Smith"
               :schema/email "alice@example.org"
               :schema/age   42
               :ex/favNums   [9 42 76]
               :ex/favColor  "Green"}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id         "?s"
                                          :schema/age 42}})))
      (is (= [{:id           :ex/alice,
               :type         :ex/User,
               :ex/favNums   [9 42 76],
               :ex/favColor  "Green",
               :schema/age   42,
               :ex/last      "Smith",
               :schema/email "alice@example.org",
               :schema/name  "Alice"}]
             @(fluree/query db {:context context
                                :select  {"?s" ["*"]}
                                :where   {:id          "?s"
                                          :schema/age  42
                                          :ex/favColor "Green"}}))))))

(deftest ^:integration query-with-faux-compact-iri
  (testing "query with a faux compact IRI works"
    (let [conn   (test-utils/create-conn)
          alias  "faux-compact-iri-query"
          db0    @(fluree/update @(fluree/create conn alias)
                                 {"@context" test-utils/default-str-context
                                  "insert"
                                  [{"id"      "foo"
                                    "ex:name" "Foo"}
                                   {"id"      "foaf:bar"
                                    "ex:name" "Bar"}]})
          _      @(fluree/commit! conn db0)
          db1    @(fluree/load conn alias)]

      (is (= [["foo" "Foo"] ["foaf:bar" "Bar"]]
             @(fluree/query db1 {"@context" test-utils/default-str-context
                                 "select"   ["?f" "?n"]
                                 "where"    {"id"      "?f"
                                             "ex:name" "?n"}})))
      (is (= [{"id" "foo", "ex:name" "Foo"}]
             @(fluree/query db1 {"@context" test-utils/default-str-context
                                 "select"   {"foo" ["*"]}}))))))
