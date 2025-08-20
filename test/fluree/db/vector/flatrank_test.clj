(ns fluree.db.vector.flatrank-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration ^:sci vector-search-test
  (let [conn   (test-utils/create-conn)
        db0 @(fluree/create conn "vector-score")
        db     @(fluree/update
                 db0
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
                     "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                "@type"  const/iri-vector}]]
                     "where"    [{"@id"     "?x"
                                  "ex:xVec" "?vec"}
                                 ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]}
            results @(fluree/query db query)]
        (is (= [["ex:bart" 0.61 [0.1, 0.9]]
                ["ex:homer" 0.72 [0.6, 0.5]]]
               results))))

    (testing "Filter results based on another property"
      (let [query   {"@context" {"ex" "http://example.org/ns/"}
                     "select"   ["?x" "?score" "?vec"]
                     "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                "@type"  const/iri-vector}]]
                     "where"    [{"@id"     "?x"
                                  "ex:age"  36
                                  "ex:xVec" "?vec"}
                                 ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]}
            results @(fluree/query db query)]
        (is (= [["ex:homer" 0.72 [0.6, 0.5]]]
               results))))

    (testing "Applying filter to score values."
      (let [query   {"@context" {"ex" "http://example.org/ns/"}
                     "select"   ["?x" "?score"]
                     "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                "@type"  const/iri-vector}]]
                     "where"    [{"@id"     "?x"
                                  "ex:xVec" "?vec"}
                                 ["bind" "?score" "(dotProduct ?vec ?targetVec)"]
                                 ["filter" "(> ?score 0.7)"]]}
            results @(fluree/query db query)]
        (is (= [["ex:homer" 0.72]]
               results))))))

(deftest ^:integration ^:sci vector-search-different-scores
  (testing "Multi-cardinality vector values work as expected"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "vector-score-multi-card")
          db     @(fluree/update
                   db0
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
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:bart" 0.61 [0.1, 0.9]]
                  ["ex:bart" 0.68 [0.2, 0.9]]
                  ["ex:homer" 0.72 [0.6, 0.5]]]
                 results))))

      (testing "Using a cosine-similiarity metric"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(cosineSimilarity ?vec ?targetVec)"]]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:bart" 0.7306568260253945 [0.1 0.9]]
                  ["ex:bart" 0.8 [0.2 0.9]]
                  ["ex:homer" 0.9999035633345558 [0.6 0.5]]]
                 results))))

      (testing "Usine a euclidianDistance metric"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(euclidianDistance ?vec ?targetVec)"]]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.14142135623730956 [0.6 0.5]]
                  ["ex:bart" 0.5830951894845299 [0.2 0.9]]
                  ["ex:bart" 0.6708203932499369 [0.1 0.9]]]
                 results)))))))

(deftest ^:integration ^:sci vector-search-mixed-datatype
  (testing "When a property has some vectors but other datatypes, filter non-vectors in scoring"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "vector-score-mixed-dt")
          db     @(fluree/update
                   db0
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:lucy"
                      "ex:xVec" "Not a Vector"} ;; <- a string value for ex:xVec
                     {"@id"     "ex:bart"
                      "ex:xVec" [{"@value" [0.1, 0.9]
                                  "@type"  const/iri-vector}
                                 {"@value" [0.2, 0.9]
                                  "@type"  const/iri-vector}]}]})]

      (testing "Including the score and vector value in the result"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?score" "?vec"]
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]
                       "orderBy"  "?score"}
              results @(fluree/query db query)]
          (is (= [["ex:lucy" nil "Not a Vector"]
                  ["ex:bart" 0.61 [0.1, 0.9]]
                  ["ex:bart" 0.68 [0.2, 0.9]]
                  ["ex:homer" 0.72 [0.6, 0.5]]]
                 results)))))))

(deftest ^:integration vector-search-with-limit
  (testing "Vector search with sorting and limit"
    (let [conn   (test-utils/create-conn)
          db0    @(fluree/create conn "vector-search-limit")
          db     @(fluree/update
                   db0
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:name" "Homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:marge"
                      "ex:name" "Marge"
                      "ex:xVec" {"@value" [0.9, 0.8]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:lisa"
                      "ex:name" "Lisa"
                      "ex:xVec" {"@value" [0.7, 0.7]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:bart"
                      "ex:name" "Bart"
                      "ex:xVec" {"@value" [0.1, 0.9]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:maggie"
                      "ex:name" "Maggie"
                      "ex:xVec" {"@value" [0.2, 0.3]
                                 "@type"  const/iri-vector}}]})]

      (testing "Top 3 results by score"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?name" "?score"]
                       "values"   ["?targetVec" [{"@value" [0.8, 0.7]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:name" "?name"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]
                       "orderBy"  "(desc ?score)"
                       "limit"    3}
              results @(fluree/query db query)]
          (is (= 3 (count results)) "Should return exactly 3 results")
          (is (= [["ex:marge" "Marge" 1.28]
                  ["ex:lisa" "Lisa" 1.0499999999999998]
                  ["ex:homer" "Homer" 0.83]]
                 results)
              "Should return top 3 scores in descending order"))))))

(deftest ^:integration vector-search-multi-targets
  (testing "Vector search with multiple target vectors using values"
    (let [conn   (test-utils/create-conn)
          db0    @(fluree/create conn "vector-search-multi-targets")
          db     @(fluree/update
                   db0
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:bart"
                      "ex:xVec" {"@value" [0.1, 0.9]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:lisa"
                      "ex:xVec" {"@value" [0.3, 0.1]
                                 "@type"  const/iri-vector}}]})]

      (testing "Multiple target vectors produce multiple searches"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?x" "?targetVec" "?score" "?vec"]
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}
                                                 {"@value" [0.1, 0.8]
                                                  "@type"  const/iri-vector}]]
                       "where"    [{"@id"     "?x"
                                    "ex:xVec" "?vec"}
                                   ["bind" "?score" "(dotProduct ?vec ?targetVec)"]]
                       "orderBy"  ["?targetVec" "(desc ?score)"]}
              results @(fluree/query db query)]
          (is (= 6 (count results)) "Should return 3 subjects × 2 target vectors = 6 results")
          (is (= [["ex:bart" [0.1 0.8] 0.7300000000000001 [0.1 0.9]]
                  ["ex:homer" [0.1 0.8] 0.46 [0.6 0.5]]
                  ["ex:lisa" [0.1 0.8] 0.11000000000000001 [0.3 0.1]]
                  ["ex:homer" [0.7 0.6] 0.72 [0.6 0.5]]
                  ["ex:bart" [0.7 0.6] 0.61 [0.1 0.9]]
                  ["ex:lisa" [0.7 0.6] 0.27 [0.3 0.1]]]
                 results)
              "Results grouped by target vector, then sorted by score desc")))

      (testing "Cross-comparison of vectors from the dataset"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"   ["?sourceId" "?targetId" "?score"]
                       "where"    [{"@id"     "?sourceId"
                                    "ex:xVec" "?sourceVec"}
                                   {"@id"     "?targetId"
                                    "ex:xVec" "?targetVec"}
                                   ["bind" "?score" "(cosineSimilarity ?sourceVec ?targetVec)"]
                                   ["filter" "(not= ?sourceId ?targetId)"]]
                       "orderBy"  ["?sourceId" "(desc ?score)"]}
              results @(fluree/query db query)]
          (is (= 6 (count results)) "Each of 3 subjects compared to 2 others")
          (is (every? #(not= (first %) (second %)) results)
              "No self-comparisons due to filter"))))))
