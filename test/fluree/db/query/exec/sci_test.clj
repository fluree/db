(ns fluree.db.query.exec.sci-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.test-utils :as test-utils]
            [fluree.json-ld :as json-ld]))

(deftest ^:sci sci-end-to-end-filter-functions
  (testing "filters and functions in real queries (SCI compile->eval path)"
    (let [ctx [test-utils/default-context {"ex" "http://example.org/ns/"}]
          conn @(fluree/connect-memory)
          ;; create returns the initial db
          db @(fluree/create conn "test/sci")
          db1 @(fluree/update db {"@context" ctx
                                  "insert"   [{"@id" "ex:cam"  "schema:name" "Cam"  "ex:age" 28}
                                              {"@id" "ex:alex" "schema:name" "Alex" "ex:age" 42}]})]
      ;; numeric comparison in filter
      (is (= ["Cam"]
             @(fluree/query db1 {"@context" ctx
                                 "select"   "?name"
                                 "where"    [{"@id" "?p" "schema:name" "?name" "ex:age" "?age"}
                                             ["filter" "(< ?age 30)"]]})))
      ;; regex in filter
      (is (= ["Cam"]
             @(fluree/query db1 {"@context" ctx
                                 "select"   "?name"
                                 "where"    [{"@id" "?p" "schema:name" "?name"}
                                             ["filter" "(regex ?name \"^C\")"]]})))
      ;; bind + function + filter
      (is (= ["Cam"]
             @(fluree/query db1 {"@context" ctx
                                 "select"   "?name"
                                 "where"    [{"@id" "?p" "schema:name" "?name"}
                                             ["bind" "?len" "(strLen ?name)"]
                                             ["filter" "(= ?len 3)"]]})))
      ;; iri inside filter
      (is (= [["ex:cam" "ex:age"]]
             @(fluree/query db1 {"@context" ctx
                                 "select"   ["?s" "?p"]
                                 "where"    [{"@id" "?s" "?p" "?o"}
                                             ["filter" "(and (= ?p (iri \"ex:age\")) (= ?s (iri \"ex:cam\")))"]]}))))))

(deftest ^:sci compile-and-eval-iri-sci
  (testing "compile -> eval path expands iri with context under SCI"
    (let [raw-ctx {"ex" "http://example.org/"}
          parsed-ctx (json-ld/parse-context raw-ctx)
          compiled-fn (eval/compile '(iri "ex:name") parsed-ctx)
          result (compiled-fn {})]
      (is (= "http://example.org/name" (:value result)))
      (is (= "@id" (:datatype-iri result))))))