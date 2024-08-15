(ns fluree.db.vector.search-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration vector-search-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "vector-search")
        db     @(fluree/stage
                 (fluree/db ledger)
                 {"@context" {"ex" "http://example.org/ns/"}
                  "insert"
                  [{"@id"     "ex:homer"
                    "ex:name" "Homer"
                    "ex:xVec" {"@value" [0.6, 0.5]
                               "@type"  const/iri-vector}
                    "ex:age"  36}
                   {"@id"     "ex:bart"
                    "ex:name" "Bart"
                    "ex:xVec" {"@value" [0.1, 0.9]
                               "@type"  const/iri-vector}
                    "ex:age"  "forever 10"}]})]

    (testing "Including the score and vector value in the result"
      (let [query   {"@context" {"ex" "http://example.org/ns/"}
                     "select"   ["?x" "?score" "?vec"]
                     "where"    [{"@id"     "?x"
                                  "ex:xVec" {"@value"  "?vec"
                                             "@vector" [0.7, 0.6]
                                             "@score"  "?score"
                                             "@metric" "dotproduct"}}]}
            results @(fluree/query db query)]
        (is (= [["ex:bart" 0.61 [0.1, 0.9]]
                ["ex:homer" 0.72 [0.6, 0.5]]]
               results))))

    (testing "Filter results based on another property"
      (let [query   {"@context" {"ex" "http://example.org/ns/"}
                     "select"   ["?x" "?score" "?vec"]
                     "where"    [{"@id"    "?x"
                                  "ex:age" 36}
                                 {"@id"     "?x"
                                  "ex:xVec" {"@value"  "?vec"
                                             "@vector" [0.7, 0.6]
                                             "@score"  "?score"
                                             "@metric" "dotproduct"}}]}
            results @(fluree/query db query)]
        (is (= [["ex:homer" 0.72 [0.6, 0.5]]]
               results))))

    (testing "Applying filter to score values."
      (let [query   {"@context" {"ex" "http://example.org/ns/"}
                     "select"   ["?x" "?score"]
                     "where"    [{"@id"     "?x"
                                  "ex:xVec" {"@value"  "?vec"
                                             "@vector" [0.7, 0.6]
                                             "@score"  "?score"
                                             "@metric" "dotproduct"}}
                                 ["filter" "(> ?score 0.7)"]]}
            results @(fluree/query db query)]
        (is (= [["ex:homer" 0.72]]
               results))))))

(deftest ^:integration vector-search-different-scores
  (testing "When a property has some vectors but other datatypes, filter non-vectors in scoring"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "vector-search")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:bart"
                      "ex:xVec" [{"@value" [0.1, 0.9]
                                  "@type"  const/iri-vector}
                                 {"@value" [0.2, 0.9]
                                  "@type"  const/iri-vector}]}]})]

      (testing "Including the score and vector value in the result"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" {"@value"  "?vec"
                                               "@vector" [0.7, 0.6]
                                               "@score"  "?score"
                                               "@metric" "dotproduct"}}]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:bart" 0.61 [0.1, 0.9]]
                  ["ex:bart" 0.68 [0.2, 0.9]]
                  ["ex:homer" 0.72 [0.6, 0.5]]]
                 results))))

      (testing "Including the score and vector value in the result"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" {"@value"  "?vec"
                                               "@vector" [0.7, 0.6]
                                               "@score"  "?score"
                                               "@metric" "cosine"}}]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:bart" 0.7306568260253945 [0.1 0.9]]
                  ["ex:bart" 0.8 [0.2 0.9]]
                  ["ex:homer" 0.9999035633345558 [0.6 0.5]]]
                 results))))

      (testing "Including the score and vector value in the result"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" {"@value"  "?vec"
                                               "@vector" [0.7, 0.6]
                                               "@score"  "?score"
                                               "@metric" "distance"}}]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.14142135623730956 [0.6 0.5]]
                  ["ex:bart" 0.5830951894845299 [0.2 0.9]]
                  ["ex:bart" 0.6708203932499369 [0.1 0.9]]]
                 results)))))))

(deftest ^:integration vector-search-mixed-datatype
  (testing "When a property has some vectors but other datatypes, filter non-vectors in scoring"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "vector-search")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:lucy"
                      "ex:xVec" "Not a Vector"}
                     {"@id"     "ex:bart"
                      "ex:xVec" [{"@value" [0.1, 0.9]
                                  "@type"  const/iri-vector}
                                 {"@value" [0.2, 0.9]
                                  "@type"  const/iri-vector}]}]})]

      (testing "Including the score and vector value in the result"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" {"@value"  "?vec"
                                               "@vector" [0.7, 0.6]
                                               "@score"  "?score"
                                               "@metric" "dotproduct"}}]
                       "orderBy"  [["DESC" "?score"]]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.72 [0.6, 0.5]]
                  ["ex:bart" 0.68 [0.2, 0.9]]
                  ["ex:bart" 0.61 [0.1, 0.9]]]
                 results)))))))
