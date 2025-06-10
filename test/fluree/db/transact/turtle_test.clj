(ns fluree.db.transact.turtle-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(def turtle-sample
  "@prefix ex: <http://example.org/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
   @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
   # --- Named Node ---
   ex:foo ex:name \"Foo's Name\" ;
          ex:age  \"42\"^^xsd:integer .
   # --- Blank Node related to other blank node ---
   _:b1 a ex:Person ;
        ex:name \"Blank Node\" ;
        ex:age  \"41\"^^xsd:integer ;
        ex:friend _:b1 .
   # --- Numeric datatype without ---
   _:b2 rdf:type ex:Person ;
        ex:name \"Blank 2\" ;
        ex:age 33 .
   ")

(deftest ^:integration turtle-insert
  (testing "Successfully inserts Turtle data into a ledger"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "tx/turtle-insert")
          db        @(fluree/insert (fluree/db ledger) turtle-sample {:format :turtle})

          query       {"@context" {"ex"  "http://example.org/"
                                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
                       "select"   {"?s" ["*"]}
                       "where"    {"@id"    "?s"
                                   "ex:age" {"@value" 42
                                             "@type"  "xsd:integer"}}}
          query2      {"@context" {"ex"  "http://example.org/"
                                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
                       "select"   {"?s" ["*"]}
                       "where"    {"@id"    "?s"
                                   "ex:age" {"@value" 41
                                             "@type"  "xsd:integer"}}}
          query3      {"@context" {"ex"  "http://example.org/"
                                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
                       "select"   {"?s" ["*"]}
                       "where"    {"@id"    "?s"
                                   "ex:age" 33}}]

      (is (= [{"@id"     "ex:foo"
               "ex:name" "Foo's Name"
               "ex:age"  42}]
             @(fluree/query db query))
          "Turtle data with explicit IRI @id values resolution")

      (is (= [{"@id"       "_:b1"
               "@type"     "ex:Person"
               "ex:name"   "Blank Node"
               "ex:age"    41
               "ex:friend" {"@id" "_:b1"}}]
             @(fluree/query db query2))
          "Blank nodes, rdf:type alias 'a' and blank node refs carry through")

      (is (= [{"@id"     "_:b2"
               "@type"   "ex:Person"
               "ex:name" "Blank 2"
               "ex:age"  33}]
             @(fluree/query db query3))
          "When no datatype is specified in TTL, it isn't needed in query either to retrieve data."))))

(deftest ^:integration turtle-upsert
  (testing "Successfully inserts Turtle data into a ledger"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "tx/turtle-upsert")
          db        @(fluree/insert (fluree/db ledger) turtle-sample {:format :turtle})
          db2       @(fluree/upsert db "@prefix ex: <http://example.org/> .
                                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                                        ex:foo ex:name \"UPDATED Name\" ;
                                               ex:age  \"33\"^^xsd:integer ."
                                    {:format :turtle})]

      (is (= [{"@id"     "ex:foo"
               "ex:name" "UPDATED Name"
               "ex:age"  33}]
             @(fluree/query db2 {"@context" {"ex"  "http://example.org/"}
                                 "select"   {"ex:foo" ["*"]}}))
          "Turtle data with explicit IRI @id values resolution"))))
