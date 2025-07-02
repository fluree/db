(ns fluree.db.transact.upsert-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.test-utils :as test-utils]))

(def sample-insert-txn {"@context" {"ex" "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "@graph"   [{"@id"         "ex:alice",
                                     "@type"        "ex:User",
                                     "schema:name" "Alice"
                                     "schema:age"  42}
                                    {"@id"         "ex:bob",
                                     "@type"        "ex:User",
                                     "schema:name" "Bob"
                                     "schema:age"  22}]})

(def sample-upsert-txn {"@context" {"ex" "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "@graph"   [{"@id"         "ex:alice"
                                     "schema:name" "Alice2"}
                                    {"@id"         "ex:bob"
                                     "schema:name" "Bob2"}
                                    {"@id"         "ex:jane"
                                     "schema:name" "Jane2"}]})

(def sample-update-txn {"@context" (get sample-upsert-txn "@context")
                        "where" [["optional" {"@id" "ex:alice"
                                              "schema:name" "?f0"}]
                                 ["optional" {"@id" "ex:bob"
                                              "schema:name" "?f1"}]
                                 ["optional" {"@id" "ex:jane"
                                              "schema:name" "?f2"}]]
                        "delete" [{"@id" "ex:alice"
                                   "schema:name" "?f0"}
                                  {"@id" "ex:bob"
                                   "schema:name" "?f1"}
                                  {"@id" "ex:jane"
                                   "schema:name" "?f2"}]
                        "insert" (get sample-upsert-txn "@graph")})

(deftest upsert-parsing
  (testing "Parsed upsert txn is identical to long-form update txn"
    (is (= (parse/parse-upsert-txn sample-upsert-txn {})
           (parse/parse-update-txn sample-update-txn {})))))

(deftest ^:integration upsert-data
  (testing "Upserting data into a ledger is identitcal to long-form update txn"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "tx/upsert-test")
          db        @(fluree/insert (fluree/db ledger) sample-insert-txn)
          db+upsert @(fluree/upsert db sample-upsert-txn)]

      (is (= [{"@id"         "ex:alice",
               "@type"       "ex:User",
               "schema:age"  42,
               "schema:name" "Alice2"}
              {"@id"         "ex:bob",
               "schema:age"  22,
               "schema:name" "Bob2",
               "@type"       "ex:User"}
              {"@id"         "ex:jane",
               "schema:name" "Jane2"}]
             @(fluree/query db+upsert
                            {"@context" {"ex"     "http://example.org/ns/"
                                         "schema" "http://schema.org/"}
                             "select"   {"?id" ["*"]}
                             "where"    {"@id"         "?id"
                                         "schema:name" "?name"}}))
          "Upsert data is inconsistent"))))

(deftest ^:integration upsert-no-changes
  (testing "Upserting identical data to existing does not change ledger"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/upsert2")

          db     @(fluree/insert (fluree/db ledger) sample-insert-txn)

          db2    @(fluree/upsert db sample-insert-txn)

          db3    @(fluree/upsert db sample-insert-txn)

          query  {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"}
                  "select"   {"?id" ["*"]}
                  "where"    {"@id"         "?id"
                              "schema:name" "?name"}}]

      (is (= (get sample-insert-txn "@graph")
             @(fluree/query db query)
             @(fluree/query db2 query)
             @(fluree/query db3 query))
          "Resulting data should be identical to original insert"))))
