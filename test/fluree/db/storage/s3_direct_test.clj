(ns fluree.db.storage.s3-direct-test
  "Test for direct S3 implementation"
  (:require [alphabase.core :as alphabase]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.storage.s3 :as s3]))

(deftest hmac-sha256-test
  (testing "HMAC-SHA256 implementation"
    ;; Test vector from AWS documentation
    (let [key (.getBytes "AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" "UTF-8")
          data "20150830"
          result (s3/hmac-sha256 key data)
          hex-result (alphabase/base-to-base result :bytes :hex)]
      ;; This is just a basic test to ensure HMAC works
      (is (= 64 (count hex-result)))
      (is (string? hex-result)))))

(deftest signature-v4-test
  (testing "AWS Signature V4 components"
    (testing "URL encoding"
      (is (= "hello%20world" (s3/url-encode "hello world")))
      (is (= "test%2Fpath" (s3/url-encode "test/path")))
      (is (= "special%2A~chars" (s3/url-encode "special*~chars"))))

    (testing "Canonical URI"
      (is (= "/" (s3/canonical-uri "")))
      (is (= "/test/path" (s3/canonical-uri "test/path"))))

    (testing "SHA256 hex"
      (let [result (s3/sha256-hex "test")]
        (is (= 64 (count result)))
        (is (= "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" result))))))

(deftest s3-url-test
  (testing "S3 URL building"
    (is (= "https://my-bucket.s3.us-east-1.amazonaws.com/test/path.json"
           (s3/build-s3-url "my-bucket" "us-east-1" "test/path.json")))
    (is (= "https://bucket.s3.eu-west-1.amazonaws.com/file.txt"
           (s3/build-s3-url "bucket" "eu-west-1" "file.txt")))))