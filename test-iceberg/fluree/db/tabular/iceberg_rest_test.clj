(ns ^:iceberg fluree.db.tabular.iceberg-rest-test
  "Tests for REST catalog IcebergSource.

   Requires a running Iceberg REST catalog server and configured storage.
   Run with: clojure -M:dev:iceberg:cljtest '{:kaocha.filter/focus-meta [:iceberg-rest]}'

   For local testing, you can use docker-compose in dev-resources/iceberg-rest/

   Required environment variables for integration tests:
   - ICEBERG_REST_URI: REST catalog endpoint (default: http://localhost:8181)
   - ICEBERG_REST_S3_ENDPOINT: S3-compatible endpoint (default: http://localhost:9000)
   - ICEBERG_REST_BUCKET: S3 bucket name (default: warehouse)
   - ICEBERG_REST_ACCESS_KEY: S3 access key (default: admin)
   - ICEBERG_REST_SECRET_KEY: S3 secret key (default: password)"
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.storage.s3 :as s3]
            [fluree.db.tabular.iceberg.rest :as rest]
            [fluree.db.tabular.protocol :as proto]))

;;; ---------------------------------------------------------------------------
;;; Configuration (from environment or defaults)
;;; ---------------------------------------------------------------------------

(defn- create-test-store
  "Create an S3 store for testing, returns nil if not configured.
   Note: s3/open uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars for credentials."
  []
  (let [endpoint (or (System/getenv "ICEBERG_REST_S3_ENDPOINT") "http://localhost:9000")
        bucket   (or (System/getenv "ICEBERG_REST_BUCKET") "warehouse")]
    ;; s3/open signature: [identifier bucket prefix endpoint-override]
    ;; It reads credentials from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars
    (try
      (s3/open nil bucket "" endpoint)
      (catch Exception _
        nil))))

(defn- test-config
  "REST catalog config - creates fresh store each time."
  []
  {:uri        (or (System/getenv "ICEBERG_REST_URI") "http://localhost:8181")
   :store      (create-test-store)
   :auth-token (System/getenv "ICEBERG_REST_TOKEN")})

(def ^:private source (atom nil))

(defn- catalog-reachable?
  "Check if REST catalog is reachable and store is configured."
  []
  (let [config (test-config)]
    (when (:store config)
      (try
        ;; Attempt to create source - will fail if catalog unavailable
        (let [s (rest/create-rest-iceberg-source config)]
          (proto/close s)
          true)
        (catch Exception _
          false)))))

(defn rest-source-fixture [f]
  (let [config (test-config)]
    (if (and (:store config) (catalog-reachable?))
      (do
        (reset! source (rest/create-rest-iceberg-source config))
        (try
          (f)
          (finally
            (when @source
              (proto/close @source)
              (reset! source nil)))))
      (do
        (println "SKIP: REST catalog not reachable at" (:uri config) "or store not configured")
        (f)))))  ;; Still run unit tests even if catalog not available

(use-fixtures :once rest-source-fixture)

;;; ---------------------------------------------------------------------------
;;; Unit Tests (don't require running catalog)
;;; ---------------------------------------------------------------------------

(deftest create-source-requires-uri-and-store
  (testing "Throws on missing uri"
    (is (thrown? AssertionError
                 (rest/create-rest-iceberg-source {:store (reify)}))))

  (testing "Throws on missing store"
    (is (thrown? AssertionError
                 (rest/create-rest-iceberg-source {:uri "http://localhost"})))))

;;; ---------------------------------------------------------------------------
;;; Integration Tests (require running REST catalog)
;;; ---------------------------------------------------------------------------

(deftest ^:iceberg-rest rest-scan-test
  (when @source
    (testing "Scan returns rows"
      ;; This test assumes a test table exists in the catalog
      ;; Configure ICEBERG_REST_TABLE env var to specify
      (when-let [table-name (System/getenv "ICEBERG_REST_TABLE")]
        (let [rows (proto/scan-rows @source table-name {:limit 5})]
          (is (seq rows) "Should return some rows")
          (is (<= (count rows) 5) "Should respect limit"))))))

(deftest ^:iceberg-rest rest-schema-test
  (when @source
    (testing "Get schema returns column info"
      (when-let [table-name (System/getenv "ICEBERG_REST_TABLE")]
        (let [schema (proto/get-schema @source table-name {})]
          (is (map? schema))
          (is (seq (:columns schema)) "Should have columns"))))))

(deftest ^:iceberg-rest rest-statistics-test
  (when @source
    (testing "Get statistics returns row count"
      (when-let [table-name (System/getenv "ICEBERG_REST_TABLE")]
        (let [stats (proto/get-statistics @source table-name {})]
          (is (map? stats))
          (is (number? (:row-count stats)) "Should have row count"))))))

;;; ---------------------------------------------------------------------------
;;; Catalog Discovery Integration Tests
;;; ---------------------------------------------------------------------------

(deftest ^:iceberg-rest list-namespaces-test
  (when @source
    (testing "List namespaces returns seq of strings"
      (let [namespaces (proto/list-namespaces @source)]
        (is (sequential? namespaces))
        (is (every? string? namespaces))))))

(deftest ^:iceberg-rest list-tables-test
  (when @source
    (testing "List tables in namespace"
      (when-let [namespace (System/getenv "ICEBERG_REST_NAMESPACE")]
        (let [tables (proto/list-tables @source namespace)]
          (is (sequential? tables))
          (is (every? string? tables))
          (is (every? #(str/starts-with? % namespace) tables)))))))

(deftest ^:iceberg-rest discover-catalog-test
  (when @source
    (testing "Discover catalog returns namespace->tables map"
      (let [catalog-info (rest/discover-catalog @source {:include-schema? false
                                                         :include-statistics? false})]
        (is (map? catalog-info))
        (doseq [[ns tables] catalog-info]
          (is (string? ns))
          (is (sequential? tables))
          (is (every? :name tables)))))))

;;; ---------------------------------------------------------------------------
;;; REPL Helpers
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.tabular.iceberg-rest-test))
