(ns fluree.db.vector.bm25-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

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
                                                 "select"   {"?x" ["@id" "ex:author" "ex:title" "ex:summary"]}}}}})

          q1     @(fluree/query db-r {"@context" {"ex"   "http://example.org/ns/"
                                                  "fidx" "https://ns.flur.ee/index#"}
                                      "select"   ["?x", "?score", "?title"]
                                      "where"    [["graph" "##articleSearch" {"fidx:target" "Apples for snacks for John"
                                                                              "fidx:limit"  10,
                                                                              "fidx:sync"   true,
                                                                              "fidx:result" {"@id"        "?x"
                                                                                             "fidx:score" "?score"}}]
                                                  {"@id"      "?x"
                                                   "ex:title" "?title"}]})]

      (is (= [["ex:hobby-article" 0.741011563872269 "This is an article about hobbies"]
              ["ex:food-article" 0.6510910594922633 "This is one title of a document about food"]]
             q1)))))

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
                      "ex:summary" "Hobbies include reading and hiking"}]})

          q1     @(fluree/query db-r {"@context" {"ex"   "http://example.org/ns/"
                                                  "fidx" "https://ns.flur.ee/index#"}
                                      "select"   ["?x", "?score", "?title"]
                                      "where"    [["graph" "##articleSearch" {"fidx:target" "Apples for snacks for John"
                                                                              "fidx:limit"  10,
                                                                              "fidx:sync"   true,
                                                                              "fidx:result" {"@id"        "?x"
                                                                                             "fidx:score" "?score"}}]
                                                  {"@id"      "?x"
                                                   "ex:title" "?title"}]})]

      (is (= [["ex:hobby-article" 0.741011563872269 "This is an article about hobbies"]
              ["ex:food-article" 0.6510910594922633 "This is one title of a document about food"]]
             q1)))))

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
                      "ex:summary" "Medical costs are at all time high, and many people are struggling to pay for their healthcare"}]})

          q1     @(fluree/query db-r3 {"@context" {"ex"   "http://example.org/ns/"
                                                   "fidx" "https://ns.flur.ee/index#"}
                                       "select"   ["?x", "?score", "?title"]
                                       "where"    [["graph" "##articleSearch" {"fidx:target" "Bitcoin funding microplastics research"
                                                                               "fidx:limit"  10,
                                                                               "fidx:sync"   true,
                                                                               "fidx:result" {"@id"        "?x"
                                                                                              "fidx:score" "?score"}}]
                                                   {"@id"      "?x"
                                                    "ex:title" "?title"}]})]

      (is (= [["ex:tech-news2" 2.0901192626067044 "Cryptocurrency news - bitcoin at all time high"]
              ["ex:health-article" 1.9365594800478445 "Microplastics are in our food"]]
             q1)))))

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
