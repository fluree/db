(ns fluree.db.vector.index-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration vector-index-search
  (testing "Some vectors on a property can be flat-rank scored"
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
                      "ex:xVec" "Not a Vector"} ;; <- a string value for ex:xVec
                     {"@id"     "ex:bart"
                      "ex:xVec" [{"@value" [0.1, 0.9]
                                  "@type"  const/iri-vector}
                                 {"@value" [0.2, 0.9]
                                  "@type"  const/iri-vector}]}]})]

      (testing "dot product scoring"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns}
                       "select"   ["?x", "?score", "?vec"]
                       "where"    [["graph"
                                    "##Flatrank-DotProduct"
                                    {"fidx:search"   {"@value" [0.7, 0.6]
                                                      "@type"  const/iri-vector}
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10,
                                     "fidx:result"   {"@id"         "?x"
                                                      "fidx:score"  "?score"
                                                      "fidx:vector" "?vec"}}]]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.72 [0.6, 0.5]]
                  ["ex:bart" 0.68 [0.2, 0.9]]
                  ["ex:bart" 0.61 [0.1, 0.9]]]
                 results))))

      (testing "cosine similarity scoring"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns},
                       "select"   ["?x", "?score", "?vec"],
                       "where"    [["graph"
                                    "##Flatrank-Cosine"
                                    {"fidx:search"   {"@value" [0.7, 0.6]
                                                      "@type"  const/iri-vector}
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10,
                                     "fidx:result"   {"@id"         "?x"
                                                      "fidx:score"  "?score",
                                                      "fidx:vector" "?vec"}}]]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.9999035633345558 [0.6 0.5]]
                  ["ex:bart" 0.8 [0.2 0.9]]
                  ["ex:bart" 0.7306568260253945 [0.1 0.9]]]
                 results))))

      (testing "euclidean distance similarity scoring"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns},
                       "select"   ["?x", "?score", "?vec"],
                       "where"    [["graph"
                                    "##Flatrank-Distance"
                                    {"fidx:search"   {"@value" [0.7, 0.6]
                                                      "@type"  const/iri-vector}
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10,
                                     "fidx:result"   {"@id"         "?x"
                                                      "fidx:score"  "?score",
                                                      "fidx:vector" "?vec"}}]]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" 0.14142135623730956 [0.6 0.5]]
                  ["ex:bart" 0.5830951894845299 [0.2 0.9]]
                  ["ex:bart" 0.6708203932499369 [0.1 0.9]]]
                 results)))))))


(deftest ^:integration vector-index-search-extra
  (testing "Vector results can join with additional properties"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "vector-search-add-props")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"      "ex:homer"
                      "ex:title" "Homer Title"
                      "ex:xVec"  {"@value" [0.6, 0.5]
                                  "@type"  const/iri-vector}}
                     {"@id"      "ex:bart"
                      "ex:title" "Bart Title"
                      "ex:xVec"  [{"@value" [0.1, 0.9]
                                   "@type"  const/iri-vector}
                                  {"@value" [0.2, 0.9]
                                   "@type"  const/iri-vector}]}]})]

      (testing "dot product scoring"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns}
                       "select"   ["?x", "?title", "?score", "?vec"]
                       "where"    [["graph"
                                    "##Flatrank-DotProduct"
                                    {"fidx:search"   {"@value" [0.7, 0.6]
                                                      "@type"  const/iri-vector}
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10,
                                     "fidx:result"   {"@id"         "?x"
                                                      "fidx:score"  "?score"
                                                      "fidx:vector" "?vec"}}]
                                   {"@id"      "?x"
                                    "ex:title" "?title"}]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" "Homer Title" 0.72 [0.6, 0.5]]
                  ["ex:bart" "Bart Title" 0.68 [0.2, 0.9]]
                  ["ex:bart" "Bart Title" 0.61 [0.1, 0.9]]]
                 results)))))))


(deftest ^:integration vector-index-multi-bindings
  (testing "Initial 'solutions' before the search call will produce multiple search results"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "vector-search-bindings")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:xVec" {"@value" [0.6, 0.5]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:bart"
                      "ex:xVec" {"@value" [0.1, 0.9]
                                 "@type"  const/iri-vector}}
                     {"@id"     "ex:lucy"
                      "ex:xVec" {"@value" [0.3, 0.1]
                                 "@type"  const/iri-vector}}]})]

      (testing "multiple values bindings for target vectors"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns}
                       "select"   ["?x", "?targetVec", "?score", "?vec"]
                       "where"    [["graph"
                                    "##Flatrank-DotProduct"
                                    {"fidx:search"   "?targetVec"
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10,
                                     "fidx:result"   {"@id"         "?x"
                                                      "fidx:score"  "?score"
                                                      "fidx:vector" "?vec"}}]]
                       "values"   ["?targetVec" [{"@value" [0.7, 0.6]
                                                  "@type"  const/iri-vector}
                                                 {"@value" [0.1, 0.8]
                                                  "@type"  const/iri-vector}]]}
              results @(fluree/query db query)]
          (is (= [["ex:homer" [0.7, 0.6], 0.72, [0.6, 0.5]]
                  ["ex:bart" [0.7, 0.6], 0.61, [0.1, 0.9]]
                  ["ex:lucy" [0.7 0.6], 0.27, [0.3 0.1]]
                  ["ex:bart" [0.1 0.8], 0.7300000000000001, [0.1 0.9]]
                  ["ex:homer" [0.1 0.8], 0.46, [0.6 0.5]]
                  ["ex:lucy" [0.1 0.8], 0.11000000000000001, [0.3 0.1]]]
                 results)
              "results repeated for each vector, but with different scores/order")))

      (testing "comparison vector pulled from result set"
        (let [query   {"@context" {"ex"   "http://example.org/ns/"
                                   "fidx" iri/f-idx-ns}
                       "select"   ["?targetSubj" "?x", "?score"]
                       "where"    [{"@id"     "?targetSubj"
                                    "ex:xVec" "?targetVec"}
                                   ["graph"
                                    "##Flatrank-Cosine"
                                    {"fidx:search"   "?targetVec"
                                     "fidx:property" {"@id" "ex:xVec"}
                                     "fidx:limit"    10
                                     "fidx:result"   {"@id"        "?x"
                                                      "fidx:score" "?score"}}]
                                   ["filter" "(not= ?targetSubj ?x)"]]}
              results @(fluree/query db query)]
          (is (= [["ex:bart" "ex:homer" 0.7211047102874315]
                  ["ex:bart" "ex:lucy" 0.41905817746174695]
                  ["ex:lucy" "ex:homer" 0.9312427797057533]
                  ["ex:lucy" "ex:bart" 0.41905817746174695]
                  ["ex:homer" "ex:lucy" 0.9312427797057533]
                  ["ex:homer" "ex:bart" 0.7211047102874315]]
                 results)
              "comparing every person to every other person with a ranked score"))))))
