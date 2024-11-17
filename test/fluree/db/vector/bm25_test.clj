(ns fluree.db.vector.bm25-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

(defn full-text-search
  "Performs a full text search and returns a couple attributes joined from the db
  for use of tests below"
  [db search-term]
  @(fluree/query db {"@context" {"ex"   "http://example.org/ns/"
                                 "fidx" "https://ns.flur.ee/index#"}
                     "select"   ["?x", "?score", "?title"]
                     "where"    [["graph" "##articleSearch" {"fidx:target" search-term
                                                             "fidx:limit"  10,
                                                             "fidx:sync"   true,
                                                             "fidx:result" {"@id"        "?x"
                                                                            "fidx:score" "?score"}}]
                                 {"@id"      "?x"
                                  "ex:title" "?title"}]}))

(deftest ^:integration bm25-index-search
  (testing "Creating and using a bm25 index after inserting data"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-search")

          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:food-article"
                      "ex:author"  "Jane Smith"
                      "ex:title"   "This is one title of a document about food"
                      "ex:summary" "This is a summary of the document about food including apples and oranges"}
                     {"@id"        "ex:hobby-article"
                      "ex:author"  "John Doe"
                      "ex:title"   "This is an article about hobbies"
                      "ex:summary" "Hobbies include reading and hiking"}]})

          db-r   @(fluree/stage
                   db
                   {"insert"
                    {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                       "fvg"  "https://ns.flur.ee/virtualgraph#"
                                       "fidx" "https://ns.flur.ee/index#"
                                       "ex"   "http://example.org/"},
                     "@id"            "ex:articleSearch"
                     "@type"          ["f:VirtualGraph" "fidx:BM25"]
                     "f:virtualGraph" "articleSearch"
                     ;"fidx:b"         0.75 ;; TODO - this is same as default - test with different values and verify values are picked up
                     ;"fidx:k1"        1.2
                     ;; TODO - I think specifying the language below was updated
                     "fidx:stemmer"   {"@id" "fidx:snowballStemmer-en"}
                     "fidx:stopwords" {"@id" "fidx:stopwords-en"}
                     "f:query"        {"@type"  "@json"
                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                 "where"    [{"@id"       "?x"
                                                              "ex:author" "?author"}]
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})]

      (is (= [["ex:hobby-article" 0.741011563872269 "This is an article about hobbies"]
              ["ex:food-article" 0.6510910594922633 "This is one title of a document about food"]]
             (full-text-search db-r "Apples for snacks for John"))))))

(deftest ^:integration bm25-index-search-before-data
  (testing "Creating and using a bm25 index before inserting data"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-search2")

          db     @(fluree/stage
                   (fluree/db ledger)
                   {"insert"
                    {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                       "fvg"  "https://ns.flur.ee/virtualgraph#"
                                       "fidx" "https://ns.flur.ee/index#"
                                       "ex"   "http://example.org/"},
                     "@id"            "ex:articleSearch"
                     "@type"          ["f:VirtualGraph" "fidx:BM25"]
                     "f:virtualGraph" "articleSearch"
                     "f:query"        {"@type"  "@json"
                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                 "where"    [{"@id"       "?x"
                                                              "ex:author" "?author"}]
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

          db-r   @(fluree/stage
                   db
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:food-article"
                      "ex:author"  "Jane Smith"
                      "ex:title"   "This is one title of a document about food"
                      "ex:summary" "This is a summary of the document about food including apples and oranges"}
                     {"@id"        "ex:hobby-article"
                      "ex:author"  "John Doe"
                      "ex:title"   "This is an article about hobbies"
                      "ex:summary" "Hobbies include reading and hiking"}]})]

      (is (= [["ex:hobby-article" 0.741011563872269 "This is an article about hobbies"]
              ["ex:food-article" 0.6510910594922633 "This is one title of a document about food"]]
             (full-text-search db-r "Apples for snacks for John"))))))

(deftest ^:integration bm25-many-inserts-then-query
  (testing "Create a number of inserts, each will update off each other in the background - with pending query"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-search3")

          db     @(fluree/stage
                   (fluree/db ledger)
                   {"insert"
                    {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                       "fvg"  "https://ns.flur.ee/virtualgraph#"
                                       "fidx" "https://ns.flur.ee/index#"
                                       "ex"   "http://example.org/"},
                     "@id"            "ex:articleSearch"
                     "@type"          ["f:VirtualGraph" "fidx:BM25"]
                     "f:virtualGraph" "articleSearch"
                     "f:query"        {"@type"  "@json"
                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                 "where"    [{"@id"       "?x"
                                                              "ex:author" "?author"}]
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

          db-r   @(fluree/stage
                   db
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:food-article"
                      "ex:author"  "Jane Smith"
                      "ex:title"   "This is one title of a document about food"
                      "ex:summary" "This is a summary of the document about food including apples and oranges"}
                     {"@id"        "ex:hobby-article"
                      "ex:author"  "John Doe"
                      "ex:title"   "This is an article about hobbies"
                      "ex:summary" "Hobbies include reading and hiking"}]})
          db-r2  @(fluree/stage
                   db-r
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:tech-news"
                      "ex:author"  "Bob Oak"
                      "ex:title"   "Here is an article about the latest technology news"
                      "ex:summary" "We have some interesting information about gadgets and software"}
                     {"@id"        "ex:tech-news2"
                      "ex:author"  "Bob Oak"
                      "ex:title"   "Cryptocurrency news - bitcoin at all time high"
                      "ex:summary" "Various cryptocurrencies like bitcoin and ethereum are at all time highs"}]})
          db-r3  @(fluree/stage
                   db-r2
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:health-article"
                      "ex:author"  "Joe Janssen"
                      "ex:title"   "Microplastics are in our food"
                      "ex:summary" "Microplastics are in our food and water supply, and now account for a credit card's worth of plastic in our bodies"}
                     {"@id"        "ex:health-article2"
                      "ex:author"  "Amy Aetna"
                      "ex:title"   "Medical costs are at all time high"
                      "ex:summary" "Medical costs are at all time high, and many people are struggling to pay for their healthcare"}]})]

      (is (= [["ex:tech-news2" 2.0901192626067044 "Cryptocurrency news - bitcoin at all time high"]
              ["ex:health-article" 1.9365594800478445 "Microplastics are in our food"]]
             (full-text-search db-r3 "Bitcoin funding microplastics research"))))))

(deftest ^:integration bm25-index-update-items
  (testing "Ensuring that updates to indexed items are properly accounted for"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-item-updates")

          db     @(fluree/stage
                   (fluree/db ledger)
                   {"insert"
                    {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                       "fvg"  "https://ns.flur.ee/virtualgraph#"
                                       "fidx" "https://ns.flur.ee/index#"
                                       "ex"   "http://example.org/"},
                     "@id"            "ex:articleSearch"
                     "@type"          ["f:VirtualGraph" "fidx:BM25"]
                     "f:virtualGraph" "articleSearch"
                     "f:query"        {"@type"  "@json"
                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                 "where"    [{"@id"       "?x"
                                                              "ex:author" "?author"}]
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

          db-r   @(fluree/stage
                   db
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:food-article"
                      "ex:author"  "Jane Smith"
                      "ex:title"   "This is one title of a document about food"
                      "ex:summary" "This is a summary of the document about food including apples and oranges"}
                     {"@id"        "ex:hobby-article"
                      "ex:author"  "John Doe"
                      "ex:title"   "This is an article about hobbies"
                      "ex:summary" "Hobbies include reading and hiking"}]})

          db-r2  @(fluree/stage
                   db-r
                   {"@context" {"ex" "http://example.org/ns/"}
                    "where"    {"@id"        "ex:food-article"
                                "ex:summary" "?summary"}
                    "delete"   [{"@id"        "ex:food-article"
                                 "ex:summary" "?summary"}]
                    "insert"   [{"@id"        "ex:food-article"
                                 "ex:summary" "This summary removes the fruit but adds travel like airplanes and taxis"}]})]

      (is (= [["ex:hobby-article" 0.7549127709068711 "This is an article about hobbies"]]
             (full-text-search db-r2 "Apples for snacks for John"))
          "After updating the summary of the food article it no longer contains a reference to apples so won't show.")

      (is (= [["ex:food-article" 0.64072428455121 "This is one title of a document about food"]]
             (full-text-search db-r2 "Something about airplanes"))
          "The article now talks about airplanes and should show up for the new search"))))

(deftest ^:integration bm25-index-retractions
  (testing "Retracting data from a bm25 index"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-retract")

          db     @(fluree/stage
                   (fluree/db ledger)
                   {"insert"
                    {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                       "fvg"  "https://ns.flur.ee/virtualgraph#"
                                       "fidx" "https://ns.flur.ee/index#"
                                       "ex"   "http://example.org/"},
                     "@id"            "ex:articleSearch"
                     "@type"          ["f:VirtualGraph" "fidx:BM25"]
                     "f:virtualGraph" "articleSearch"
                     "f:query"        {"@type"  "@json"
                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                 "where"    [{"@id"       "?x"
                                                              "ex:author" "?author"}]
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

          db-r   @(fluree/stage
                   db
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"        "ex:food-article"
                      "ex:author"  "Jane Smith"
                      "ex:title"   "This is one title of a document about food"
                      "ex:summary" "This is a summary of the document about food including apples and oranges"}
                     {"@id"        "ex:hobby-article"
                      "ex:author"  "John Doe"
                      "ex:title"   "This is an article about hobbies"
                      "ex:summary" "Hobbies include reading and hiking"}]})

          db-r2  @(fluree/stage
                   db-r
                   {"@context" {"ex" "http://example.org/ns/"}
                    "where"    {"@id" "ex:food-article"
                                "?p"  "?o"}
                    "delete"   {"@id" "ex:food-article"
                                "?p"  "?o"}})]

      (is (= [["ex:hobby-article" 0.28768207245178085 "This is an article about hobbies"]]
             (full-text-search db-r2 "Apples for snacks for John")))


      (testing "Score after adding and retracting article is same as score with just one article"
        (let [ledger2 @(fluree/create conn "bm25-retract-verify-same-score")

              db2     @(fluree/stage
                        (fluree/db ledger2)
                        {"insert"
                         {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                            "fvg"  "https://ns.flur.ee/virtualgraph#"
                                            "fidx" "https://ns.flur.ee/index#"
                                            "ex"   "http://example.org/"},
                          "@id"            "ex:articleSearch"
                          "@type"          ["f:VirtualGraph" "fidx:BM25"]
                          "f:virtualGraph" "articleSearch"
                          "f:query"        {"@type"  "@json"
                                            "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                      "where"    [{"@id"       "?x"
                                                                   "ex:author" "?author"}]
                                                      "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

              db2-r   @(fluree/stage
                        db2
                        {"@context" {"ex" "http://example.org/ns/"}
                         "insert"
                         [{"@id"        "ex:hobby-article"
                           "ex:author"  "John Doe"
                           "ex:title"   "This is an article about hobbies"
                           "ex:summary" "Hobbies include reading and hiking"}]})]
          (is (= (full-text-search db2-r "Apples for snacks for John")
                 (full-text-search db-r2 "Apples for snacks for John"))))))))

(deftest ^:integration bm25-index-exceptions
  (testing "The query of bm25 index has specific formatting requirements"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "bm25-search-exceptions")]

      (testing " the query has a subgraph selector in :select"
        (let [ex-db @(fluree/stage
                      (fluree/db ledger)
                      {"insert"
                       {"@context"       {"f"    "https://ns.flur.ee/ledger#"
                                          "fvg"  "https://ns.flur.ee/virtualgraph#"
                                          "fidx" "https://ns.flur.ee/index#"
                                          "ex"   "http://example.org/"},
                        "@id"            "ex:articleSearch"
                        "@type"          ["f:VirtualGraph" "fidx:BM25"]
                        "f:virtualGraph" "articleSearch"
                        "f:query"        {"@type"  "@json"
                                          "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                    "where"    [{"@id"       "?x"
                                                                 "ex:author" "?author"}]
                                                    "select"   ["?x" "?author"]}}}})]

          (is (= "BM25 index query must be created with a subgraph selector"
                 (ex-message ex-db))))))))
