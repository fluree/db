(ns graalvm-test
  "Comprehensive test suite for Fluree DB GraalVM native image functionality.
   Run this test to verify all operations work correctly in native image."
  (:require [fluree.db.api :as fluree]
            [clojure.core.async :refer [<!!]])
  (:gen-class))

(defn test-memory-connection []
  (println "\n=== Testing Memory Connection ===")
  (try
    (let [conn @(fluree/connect-memory)]
      (println "✓ Memory connection created")
      (println "  Connection type:" (type conn))
      (println "  Connection id:" (:id conn))
      conn)
    (catch Exception e
      (println "✗ Memory connection failed:" (.getMessage e))
      (throw e))))

(defn test-file-connection []
  (println "\n=== Testing File Connection ===")
  (try
    (let [conn @(fluree/connect-file {:storage-path "./dev/data/graalvm-test"})]
      (println "✓ File connection created")
      (println "  Connection type:" (type conn))
      conn)
    (catch Exception e
      (println "✗ File connection failed:" (.getMessage e))
      (throw e))))

(defn test-ledger-operations [conn ledger-alias]
  (println "\n=== Testing Ledger Operations ===")
  
  ;; Create ledger
  (println "\nCreating ledger...")
  (let [ledger (try
                 @(fluree/create conn ledger-alias)
                 (catch Exception e
                   (if (re-find #"already exists" (.getMessage e))
                     (do
                       (println "  Ledger already exists, loading...")
                       @(fluree/load conn ledger-alias))
                     (throw e))))]
    (println "✓ Ledger ready")
    
    ;; Get database
    (println "\nGetting database...")
    (let [db (fluree/db ledger)]
      (println "✓ Database retrieved")
      (println "  DB type:" (type db))
      (println "  DB t value:" (:t db))
      
      ;; Test exists?
      (println "\nTesting exists?...")
      (let [exists? @(fluree/exists? conn ledger-alias)]
        (println "✓ Ledger exists check:" exists?))
      
      {:ledger ledger :db db})))

(defn test-insert-operations [db]
  (println "\n=== Testing Insert Operations ===")
  
  ;; Simple insert
  (println "\nTesting simple insert...")
  (let [insert-data [{"@context" {"ex" "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "@id" "ex:alice"
                      "@type" "ex:User"
                      "schema:name" "Alice"
                      "schema:age" 30}]
        new-db @(fluree/insert db insert-data)]
    (println "✓ Simple insert successful")
    (println "  New DB t value:" (:t new-db))
    
    ;; Batch insert with @graph
    (println "\nTesting batch insert with @graph...")
    (let [batch-data {"@context" {"ex" "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "@graph" [{"@id" "ex:bob"
                                 "@type" "ex:User"
                                 "schema:name" "Bob"
                                 "schema:age" 25
                                 "ex:department" "Engineering"}
                                {"@id" "ex:charlie"
                                 "@type" "ex:User"
                                 "schema:name" "Charlie"
                                 "schema:age" 35
                                 "ex:department" "Sales"}
                                {"@id" "ex:david"
                                 "@type" "ex:User"
                                 "schema:name" "David"
                                 "schema:age" 40
                                 "ex:department" "Engineering"}]}
          batch-db @(fluree/insert new-db batch-data)]
      (println "✓ Batch insert successful")
      (println "  Batch DB t value:" (:t batch-db))
      batch-db)))

(defn test-update-operations [ledger db]
  (println "\n=== Testing Update Operations ===")
  
  ;; Update operation
  (println "\nTesting update...")
  (let [update-data {"@context" {"ex" "http://example.org/ns/"
                                 "schema" "http://schema.org/"}
                     "insert" [{"@id" "ex:alice"
                                "schema:age" 31}]}
        updated-db @(fluree/update db update-data)]
    (println "✓ Update successful")
    (println "  Updated DB t value:" (:t updated-db))
    
    ;; Upsert operation
    (println "\nTesting upsert...")
    (let [upsert-data [{"@context" {"ex" "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "@id" "ex:eve"
                        "@type" "ex:User"
                        "schema:name" "Eve"
                        "schema:age" 28
                        "ex:department" "Marketing"}]
          upserted-db @(fluree/upsert updated-db upsert-data)]
      (println "✓ Upsert successful")
      (println "  Upserted DB t value:" (:t upserted-db))
      
      ;; Test commit
      (println "\nTesting commit...")
      (let [committed-db @(fluree/commit! ledger upserted-db)]
        (println "✓ Commit successful")
        (println "  Committed DB t value:" (:t committed-db))
        committed-db))))

(defn test-query-operations [db]
  (println "\n=== Testing Query Operations ===")
  
  ;; Simple query
  (println "\nTesting simple query...")
  (let [simple-query {"@context" {"ex" "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "select" ["?name" "?age"]
                      "where" {"@type" "ex:User"
                               "schema:name" "?name"
                               "schema:age" "?age"}}
        results @(fluree/query db simple-query)]
    (println "✓ Simple query successful")
    (println "  Results count:" (count results))
    (println "  Sample results:" (take 2 results)))
  
  ;; Query with filter
  (println "\nTesting query with filter...")
  (let [filter-query {"@context" {"ex" "http://example.org/ns/"
                                   "schema" "http://schema.org/"}
                      "select" ["?name"]
                      "where" {"@type" "ex:User"
                               "schema:name" "?name"
                               "schema:age" "?age"
                               "$filter" "(> ?age 30)"}}
        filter-results @(fluree/query db filter-query)]
    (println "✓ Filter query successful")
    (println "  Results:" filter-results))
  
  ;; Aggregation query
  (println "\nTesting aggregation query...")
  (let [agg-query {"@context" {"ex" "http://example.org/ns/"
                               "schema" "http://schema.org/"}
                   "select" ["(as (avg ?age) ?avgAge)" "(as (count ?person) ?count)"]
                   "where" {"@id" "?person"
                            "@type" "ex:User"
                            "schema:age" "?age"}}
        agg-results @(fluree/query db agg-query)]
    (println "✓ Aggregation query successful")
    (println "  Results:" agg-results))
  
  ;; Grouping query
  (println "\nTesting grouping query...")
  (let [group-query {"@context" {"ex" "http://example.org/ns/"
                                 "schema" "http://schema.org/"}
                     "select" ["?dept" "(count ?person)"]
                     "where" {"@id" "?person"
                              "@type" "ex:User"
                              "ex:department" "?dept"}
                     "group-by" "?dept"}
        group-results @(fluree/query db group-query)]
    (println "✓ Grouping query successful")
    (println "  Department counts:" group-results))
  
  ;; SPARQL query
  (println "\nTesting SPARQL query...")
  (let [sparql "SELECT ?name ?age ?dept
                WHERE {
                  ?person a <http://example.org/ns/User> ;
                          <http://schema.org/name> ?name ;
                          <http://schema.org/age> ?age ;
                          <http://example.org/ns/department> ?dept .
                }
                ORDER BY ?dept ?name"
        sparql-results @(fluree/query db sparql {:format :sparql})]
    (println "✓ SPARQL query successful")
    (println "  Results count:" (count sparql-results))
    (println "  Sample results:" (take 3 sparql-results))))

(defn test-advanced-operations [conn ledger db]
  (println "\n=== Testing Advanced Operations ===")
  
  ;; Test dataset
  (println "\nTesting dataset creation...")
  (let [dataset (fluree/dataset {"main" db})]
    (println "✓ Dataset created")
    (println "  Dataset type:" (type dataset)))
  
  ;; Test range queries
  (println "\nTesting range query...")
  (let [range-results @(fluree/range db :spot = ["ex:alice"])]
    (println "✓ Range query successful")
    (println "  Results count:" (count range-results)))
  
  ;; Test history
  (println "\nTesting history query...")
  (let [history-query {"@context" {"ex" "http://example.org/ns/"}
                       "history" "ex:alice"
                       "t" {"from" 1}}
        history-results @(fluree/history ledger history-query)]
    (println "✓ History query successful")
    (println "  History entries:" (count history-results))))

(defn run-all-tests []
  (println "=== Fluree DB GraalVM Native Image Test Suite ===")
  (println "Testing comprehensive functionality...\n")
  
  (try
    ;; Test memory connection
    (let [mem-conn (test-memory-connection)
          {:keys [ledger db]} (test-ledger-operations mem-conn "test/graalvm-mem")
          insert-db (test-insert-operations db)
          update-db (test-update-operations ledger insert-db)]
      (test-query-operations update-db)
      (test-advanced-operations mem-conn ledger update-db))
    
    ;; Test file connection
    (let [file-conn (test-file-connection)
          {:keys [ledger db]} (test-ledger-operations file-conn "test/graalvm-file")
          insert-db (test-insert-operations db)]
      (test-query-operations insert-db))
    
    (println "\n✅ ALL TESTS PASSED! ✅")
    (println "\nFluree DB is fully functional in GraalVM native image!")
    true
    
    (catch Exception e
      (println "\n❌ TEST FAILED ❌")
      (println "Error:" (.getMessage e))
      (.printStackTrace e)
      false)))

(defn -main
  "Main entry point for GraalVM test"
  [& args]
  (let [success? (run-all-tests)]
    (System/exit (if success? 0 1))))