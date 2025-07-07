(ns fluree.db.storage.s3-unit-test
  "Unit tests for S3 storage that don't require external dependencies"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.storage :as storage]
            [fluree.db.storage.s3 :as s3-storage]))

(deftest s3-storage-creation-test
  (testing "S3 storage can be created with valid parameters"
    (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
      (is (some? store) "S3Store should be created")
      (is (= "test-s3" (:identifier store)) "Identifier should match")
      (is (= "test-bucket" (:bucket store)) "Bucket should match")
      (is (= "test-prefix" (:prefix store)) "Prefix should match"))))

(deftest s3-storage-protocols-test
  (testing "S3 storage implements all required protocols"
    (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
      (is (satisfies? storage/Addressable store) "Should implement Addressable")
      (is (satisfies? storage/Identifiable store) "Should implement Identifiable")
      (is (satisfies? storage/JsonArchive store) "Should implement JsonArchive")
      (is (satisfies? storage/ContentAddressedStore store) "Should implement ContentAddressedStore")
      (is (satisfies? storage/ByteStore store) "Should implement ByteStore"))))

(deftest s3-storage-identifiers-test
  (testing "S3 storage returns correct identifiers"
    (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
      (is (= #{"test-s3"} (storage/identifiers store)) "Should return identifier set"))))

(deftest s3-storage-location-test
  (testing "S3 storage generates correct location URI"
    (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
      (is (= "fluree:test-s3:s3:test-bucket:test-prefix" (storage/location store))
          "Should generate correct fluree location URI"))))

(deftest s3-endpoint-configuration-test
  (testing "S3 endpoint configuration variants"
    (testing "with custom endpoint"
      (let [store (s3-storage/open "test" "bucket" "prefix" "http://localhost:9000")]
        (is (some? store) "Should create store with custom endpoint")))

    (testing "without custom endpoint (default AWS)"
      (let [store (s3-storage/open "test" "bucket" "prefix")]
        (is (some? store) "Should create store with default endpoint")))

    (testing "with nil endpoint"
      (let [store (s3-storage/open "test" "bucket" "prefix" nil)]
        (is (some? store) "Should create store with nil endpoint")))))

(deftest connect-s3-validation-test
  (testing "connect-s3 API function validates required parameters"
    (testing "should require s3-bucket parameter"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"S3 bucket name is required"
           (fluree/connect-s3 {}))
          "Should throw error when s3-bucket is missing"))

    (testing "should require s3-endpoint parameter"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"S3 endpoint is required"
           (fluree/connect-s3 {:s3-bucket "test-bucket"}))
          "Should throw error when s3-endpoint is missing"))

    (testing "should accept valid parameters without throwing validation errors"
      ;; This should pass validation (but may fail later on actual S3 connection)
      (try
        @(fluree/connect-s3 {:s3-bucket "test-bucket"
                             :s3-endpoint "http://localhost:4566"})
        (catch clojure.lang.ExceptionInfo e
          ;; If it's a validation error about missing bucket/endpoint, that's a test failure
          (when (or (re-find #"S3 bucket name is required" (.getMessage e))
                    (re-find #"S3 endpoint is required" (.getMessage e)))
            (throw e)))
        (catch Exception _
          ;; Other exceptions (like connection failures) are expected and OK
          :connection-failed))
      ;; If we get here without validation errors, the test passes
      (is true "Should pass parameter validation"))))