(ns fluree.db.transact.delete-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration deleting-data
  (testing "Deletions of entire subjects."
    (let [conn             (test-utils/create-conn)
          ledger           @(fluree/create conn "tx/delete"
                                           {"defaults"
                                            {"@context"
                                             ["" {"ex" "http://example.org/ns/"}]}})
          db               @(fluree/stage
                             (fluree/db ledger)
                             {"@graph" [{"id"           "ex:alice",
                                         "type"         "ex:User",
                                         "schema:name"  "Alice"
                                         "schema:email" "alice@flur.ee"
                                         "schema:age"   42}
                                        {"id"          "ex:bob",
                                         "type"        "ex:User",
                                         "schema:name" "Bob"
                                         "schema:age"  22}
                                        {"id"           "ex:jane",
                                         "type"         "ex:User",
                                         "schema:name"  "Jane"
                                         "schema:email" "jane@flur.ee"
                                         "schema:age"   30}]})

          ;; delete everything for "ex:alice"
          db-subj-delete   @(fluree/stage db
                                          '{"delete" ["ex:alice" ?p ?o]
                                            "where"  [["ex:alice" ?p ?o]]})

          ;; delete any "schema:age" values for "ex:bob"
          db-subj-pred-del @(fluree/stage db
                                          '{"delete" ["ex:bob" "schema:age" ?o]
                                            "where"  [["ex:bob" "schema:age" ?o]]})

          ;; delete all subjects with a "schema:email" predicate
          db-all-preds     @(fluree/stage db
                                          '{"delete" [?s ?p ?o]
                                            "where"  [[?s "schema:email" ?x]
                                                      [?s ?p ?o]]})

          ;; delete all subjects where "schema:age" = 30
          db-age-delete    @(fluree/stage db
                                          '{"delete" [?s ?p ?o]
                                            "where"  [[?s "schema:age" 30]
                                                      [?s ?p ?o]]})]

      (is (= ["Jane" "Bob"]
             @(fluree/query db-subj-delete
                            '{"select" ?name
                              "where"  [[?s "schema:name" ?name]]}))

          "Only Jane and Bob should be left in the db.")

      (is (= {"id"          "ex:bob",
              "rdf:type"    ["ex:User"],
              "schema:name" "Bob"}
             @(fluree/query db-subj-pred-del
                            '{"selectOne" {?s ["*"]}
                              "where" [[?s "id" "ex:bob"]]}))

          "Bob should no longer have an age property.")

      (is (= ["Bob"]
             @(fluree/query db-all-preds
                            '{"select" ?name
                              "where"  [[?s "schema:name" ?name]]}))

          "Only Bob should be left, as he is the only one without an email.")

      (is (= ["Bob" "Alice"]
             @(fluree/query db-age-delete
                            '{"select" ?name
                              "where"  [[?s "schema:name" ?name]]}))

          "Only Bob and Alice should be left in the db."))))
