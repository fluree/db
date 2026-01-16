(ns iceberg-graalvm-test
  "GraalVM native image integration test for Iceberg Virtual Graphs.

   This test verifies that Fluree DB with Iceberg VG support works correctly
   in a GraalVM native image, using a REST catalog (no Hadoop dependencies).

   Environment Variables:
   - ICEBERG_REST_URI: REST catalog endpoint (default: http://localhost:8181)
   - ICEBERG_REST_S3_ENDPOINT: S3-compatible endpoint (default: http://localhost:9000)
   - ICEBERG_REST_BUCKET: S3 bucket name (default: warehouse)
   - AWS_ACCESS_KEY_ID: S3 access key (default: admin)
   - AWS_SECRET_ACCESS_KEY: S3 secret key (default: password)
   - ICEBERG_REST_TABLE: Table to test (default: openflights.airlines)
   - ICEBERG_REST_NAMESPACE: Namespace to test (default: openflights)"
  (:require [fluree.db.api :as fluree]
            [fluree.db.storage.s3 :as s3]
            ;; Use REST-specific namespaces directly to avoid Hadoop deps
            [fluree.db.tabular.iceberg.rest :as iceberg-rest]
            [fluree.db.tabular.protocol :as tabular])
  (:gen-class))

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

(defn- get-env
  "Get environment variable with default value."
  [name default]
  (or (System/getenv name) default))

(defn- rest-catalog-config
  "Build REST catalog configuration from environment."
  []
  {:uri        (get-env "ICEBERG_REST_URI" "http://localhost:8181")
   :s3-endpoint (get-env "ICEBERG_REST_S3_ENDPOINT" "http://localhost:9000")
   :bucket     (get-env "ICEBERG_REST_BUCKET" "warehouse")
   :access-key (get-env "AWS_ACCESS_KEY_ID" "admin")
   :secret-key (get-env "AWS_SECRET_ACCESS_KEY" "password")
   :table      (get-env "ICEBERG_REST_TABLE" "openflights.airlines")
   :namespace  (get-env "ICEBERG_REST_NAMESPACE" "openflights")})

;;; ---------------------------------------------------------------------------
;;; Test Functions
;;; ---------------------------------------------------------------------------

(defn test-s3-store
  "Test S3 store creation for REST catalog."
  [config]
  (println "\n=== Testing S3 Store Creation ===")
  (try
    (let [store (s3/open nil (:bucket config) "" (:s3-endpoint config))]
      (println "  S3 store created")
      (println "  Bucket:" (:bucket config))
      (println "  Endpoint:" (:s3-endpoint config))
      store)
    (catch Exception e
      (println "  S3 store creation failed:" (.getMessage e))
      (throw e))))

(defn test-rest-catalog-connection
  "Test REST catalog connectivity."
  [config store]
  (println "\n=== Testing REST Catalog Connection ===")
  (try
    (let [source (iceberg-rest/create-rest-iceberg-source
                   {:uri   (:uri config)
                    :store store})]
      (println "  REST catalog connection established")
      (println "  URI:" (:uri config))
      source)
    (catch Exception e
      (println "  REST catalog connection failed:" (.getMessage e))
      (throw e))))

(defn test-list-namespaces
  "Test listing namespaces from catalog."
  [source]
  (println "\n=== Testing List Namespaces ===")
  (try
    (let [namespaces (tabular/list-namespaces source)]
      (println "  Found" (count namespaces) "namespaces")
      (doseq [ns namespaces]
        (println "    -" ns))
      namespaces)
    (catch Exception e
      (println "  List namespaces failed:" (.getMessage e))
      (throw e))))

(defn test-list-tables
  "Test listing tables in a namespace."
  [source namespace]
  (println "\n=== Testing List Tables ===")
  (try
    (let [tables (tabular/list-tables source namespace)]
      (println "  Found" (count tables) "tables in" namespace)
      (doseq [t tables]
        (println "    -" t))
      tables)
    (catch Exception e
      (println "  List tables failed:" (.getMessage e))
      (throw e))))

(defn test-get-schema
  "Test getting table schema."
  [source table]
  (println "\n=== Testing Get Schema ===")
  (try
    (let [schema (tabular/get-schema source table {})]
      (println "  Schema for" table)
      (println "  Columns:" (count (:columns schema)))
      (doseq [col (:columns schema)]
        (println "    -" (:name col) ":" (:type col)))
      schema)
    (catch Exception e
      (println "  Get schema failed:" (.getMessage e))
      (throw e))))

(defn test-scan-rows
  "Test scanning rows from table."
  [source table]
  (println "\n=== Testing Scan Rows ===")
  (try
    (let [rows (tabular/scan-rows source table {:limit 5})]
      (println "  Scanned" (count rows) "rows from" table)
      (doseq [row (take 3 rows)]
        (println "    Sample:" (pr-str (select-keys row ["id" "name" "country"]))))
      rows)
    (catch Exception e
      (println "  Scan rows failed:" (.getMessage e))
      (throw e))))

(defn test-get-statistics
  "Test getting table statistics."
  [source table]
  (println "\n=== Testing Get Statistics ===")
  (try
    (let [stats (tabular/get-statistics source table {})]
      (println "  Statistics for" table)
      (println "  Row count:" (:row-count stats))
      (println "  File count:" (:file-count stats))
      stats)
    (catch Exception e
      (println "  Get statistics failed:" (.getMessage e))
      (throw e))))

(defn test-filtered-scan
  "Test filtered scan with predicate pushdown."
  [source table]
  (println "\n=== Testing Filtered Scan (Predicate Pushdown) ===")
  (try
    (let [rows (tabular/scan-rows source table
                                  {:columns ["id" "name" "country"]
                                   :predicates [{:column "country"
                                                 :op :eq
                                                 :value "United States"}]
                                   :limit 10})]
      (println "  Scanned" (count rows) "US airlines")
      (doseq [row (take 5 rows)]
        (println "    -" (get row "name") "(" (get row "id") ")"))
      rows)
    (catch Exception e
      (println "  Filtered scan failed:" (.getMessage e))
      (throw e))))

(defn test-fluree-memory-connection
  "Test basic Fluree memory connection (without Iceberg VG)."
  []
  (println "\n=== Testing Fluree Memory Connection ===")
  (try
    (let [conn @(fluree/connect-memory)]
      (println "  Memory connection created")
      (println "  Connection type:" (type conn))

      ;; Create a simple ledger
      (let [ledger-alias "test/graalvm-basic"]
        @(fluree/create conn ledger-alias)
        (println "  Ledger created:" ledger-alias)

        ;; Insert some data
        (let [db @(fluree/db conn ledger-alias)
              insert-data [{"@context" {"ex" "http://example.org/"}
                            "@id" "ex:test1"
                            "@type" "ex:TestEntity"
                            "ex:name" "Test Entity 1"}]
              new-db @(fluree/insert db insert-data)]
          (println "  Data inserted, new t:" (:t new-db))

          ;; Query the data
          (let [query {"@context" {"ex" "http://example.org/"}
                       "select" ["?name"]
                       "where" {"@type" "ex:TestEntity"
                                "ex:name" "?name"}}
                results @(fluree/query new-db query)]
            (println "  Query results:" results)
            @(fluree/disconnect conn)
            results))))
    (catch Exception e
      (println "  Fluree memory connection failed:" (.getMessage e))
      (throw e))))

;;; ---------------------------------------------------------------------------
;;; Main Test Runner
;;; ---------------------------------------------------------------------------

(defn run-fluree-core-tests
  "Run basic Fluree tests to verify core functionality."
  []
  (println "\n" (apply str (repeat 60 "=")) "\n")
  (println "FLUREE CORE TESTS")
  (println "\n" (apply str (repeat 60 "=")) "\n")

  (test-fluree-memory-connection))

(defn run-iceberg-rest-tests
  "Run Iceberg REST catalog tests."
  [config]
  (println "\n" (apply str (repeat 60 "=")) "\n")
  (println "ICEBERG REST CATALOG TESTS")
  (println "\n" (apply str (repeat 60 "=")) "\n")

  (let [store (test-s3-store config)
        source (test-rest-catalog-connection config store)]
    (try
      (test-list-namespaces source)
      (test-list-tables source (:namespace config))
      (test-get-schema source (:table config))
      (test-scan-rows source (:table config))
      (test-get-statistics source (:table config))
      (test-filtered-scan source (:table config))
      (finally
        (tabular/close source)))))

(defn run-all-tests
  "Run all Iceberg GraalVM tests."
  []
  (println "\n" (apply str (repeat 60 "=")) "\n")
  (println "FLUREE DB ICEBERG GRAALVM NATIVE IMAGE TEST SUITE")
  (println "\n" (apply str (repeat 60 "=")) "\n")

  (let [config (rest-catalog-config)]
    (println "Configuration:")
    (println "  REST URI:" (:uri config))
    (println "  S3 Endpoint:" (:s3-endpoint config))
    (println "  Bucket:" (:bucket config))
    (println "  Test Table:" (:table config))
    (println "  Test Namespace:" (:namespace config))

    (try
      ;; Phase 1: Core Fluree tests
      (run-fluree-core-tests)

      ;; Phase 2: Iceberg REST catalog tests
      (run-iceberg-rest-tests config)

      (println "\n" (apply str (repeat 60 "=")) "\n")
      (println "ALL ICEBERG GRAALVM TESTS PASSED!")
      (println "\n" (apply str (repeat 60 "=")) "\n")
      true

      (catch Exception e
        (println "\n" (apply str (repeat 60 "=")) "\n")
        (println "ICEBERG GRAALVM TESTS FAILED!")
        (println "Error:" (.getMessage e))
        (.printStackTrace e)
        (println "\n" (apply str (repeat 60 "=")) "\n")
        false))))

(defn -main
  "Main entry point for GraalVM Iceberg test."
  [& _args]
  (let [success? (run-all-tests)]
    (System/exit (if success? 0 1))))
