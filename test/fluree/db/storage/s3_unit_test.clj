(ns fluree.db.storage.s3-unit-test
  "Unit tests for S3 storage that don't require external dependencies"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.storage :as storage]
            [fluree.db.storage.s3 :as s3-storage]))

(deftest s3-storage-creation-test
  (testing "S3 storage can be created with valid parameters"
    ;; Set mock credentials for test
    (with-redefs [s3-storage/get-credentials (fn [] {:access-key "test-key" :secret-key "test-secret"})]
      (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
        (is (some? store) "S3Store should be created")
        (is (= "test-s3" (:identifier store)) "Identifier should match")
        (is (= "test-bucket" (:bucket store)) "Bucket should match")
        (is (= "test-prefix/" (:prefix store)) "Prefix should match normalized with trailing slash")))))

(deftest s3-storage-identifiers-test
  (testing "S3 storage returns correct identifiers"
    ;; Set mock credentials for test
    (with-redefs [s3-storage/get-credentials (fn [] {:access-key "test-key" :secret-key "test-secret"})]
      (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
        (is (= #{"test-s3"} (storage/identifiers store)) "Should return identifier set")))))

(deftest s3-storage-location-test
  (testing "S3 storage generates correct location URI"
    ;; Set mock credentials for test
    (with-redefs [s3-storage/get-credentials (fn [] {:access-key "test-key" :secret-key "test-secret"})]
      (let [store (s3-storage/open "test-s3" "test-bucket" "test-prefix")]
        (is (= "fluree:test-s3:s3" (storage/location store))
            "Should generate correct fluree location URI"))))

  (testing "S3 storage normalizes prefix to end with /"
    (with-redefs [s3-storage/get-credentials (fn [] {:access-key "test-key" :secret-key "test-secret"})]
      (let [store1 (s3-storage/open nil "bucket" "prefix")
            store2 (s3-storage/open nil "bucket" "prefix/")
            store3 (s3-storage/open nil "bucket" "")
            store4 (s3-storage/open nil "bucket" nil)]
        (is (= "prefix/" (:prefix store1)) "Should add trailing slash")
        (is (= "prefix/" (:prefix store2)) "Should keep existing trailing slash")
        (is (nil? (:prefix store3)) "Should convert empty string to nil")
        (is (nil? (:prefix store4)) "Should keep nil as nil")))))

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
          "Should throw error when s3-endpoint is missing"))))

(deftest s3-address-format-test
  (testing "S3 address without identifier (nil)"
    (let [path "some-dir/file.json"
          address (s3-storage/s3-address nil path)]
      (is (= "fluree:s3://some-dir/file.json" address)
          "Should generate address without identifier when nil")))

  (testing "S3 address with identifier"
    (let [identifier "test-s3"
          path "some-dir/file.json"
          address (s3-storage/s3-address identifier path)]
      (is (= "fluree:test-s3:s3://some-dir/file.json" address)
          "Should include identifier in address")))

  (testing "S3 address with prefix and file path"
    ;; This simulates how paths are built in practice
    ;; prefix comes from S3Store, path is the file location
    (let [identifier nil
          prefix "test"
          file-path "ledger1/commit/abc123.json"
          full-path (str prefix "/" file-path)
          address (s3-storage/s3-address identifier full-path)]
      (is (= "fluree:s3://test/ledger1/commit/abc123.json" address)
          "Should include prefix in the path"))))