(ns fluree.db.storage.s3-express-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.storage.s3-express :as s3-express]))

(deftest express-one-bucket-detection-test
  (testing "Correctly identifies S3 Express One Zone buckets"
    (is (true? (s3-express/express-one-bucket? "my-bucket--use1-az1--x-s3"))
        "Should detect standard Express One bucket")
    (is (true? (s3-express/express-one-bucket? "test-data--usw2-az2--x-s3"))
        "Should detect Express One bucket in different region/AZ")
    (is (true? (s3-express/express-one-bucket? "prod-index--euw1-az3--x-s3"))
        "Should detect Express One bucket with eu region"))

  (testing "Correctly rejects standard S3 buckets"
    (is (false? (s3-express/express-one-bucket? "my-regular-bucket"))
        "Should reject standard bucket")
    (is (false? (s3-express/express-one-bucket? "bucket-with-dashes"))
        "Should reject bucket with dashes that don't match pattern")
    (is (false? (s3-express/express-one-bucket? "bucket--x-s3"))
        "Should reject bucket without AZ identifier")
    (is (false? (s3-express/express-one-bucket? "my-bucket--use1-az1"))
        "Should reject bucket without --x-s3 suffix")
    (is (false? (s3-express/express-one-bucket? nil))
        "Should handle nil bucket name")
    (is (false? (s3-express/express-one-bucket? ""))
        "Should handle empty bucket name")))

(deftest get-credentials-for-bucket-test
  (testing "Returns base credentials for standard S3 buckets"
    (let [base-creds {:access-key "AKIAIOSFODNN7EXAMPLE"
                      :secret-key "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
          result (s3-express/get-credentials-for-bucket
                  "my-regular-bucket"
                  "us-east-1"
                  base-creds)]
      (is (= (:access-key result) (:access-key base-creds))
          "Should return same access key")
      (is (= (:secret-key result) (:secret-key base-creds))
          "Should return same secret key")
      (is (nil? (:session-token result))
          "Should not have session token for standard buckets"))))

(deftest session-cache-test
  (testing "Cache operations work correctly"
    ;; Clear cache before test
    (s3-express/clear-session-cache!)

    (is (some? (s3-express/clear-session-cache!))
        "Clearing cache should complete successfully")))

(comment
  ;; Integration tests for S3 Express One Zone
  ;; These require actual AWS credentials and an Express One Zone bucket
  ;; Run manually with appropriate AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION

  (deftest integration-get-session-credentials-test
    (testing "Can create session credentials for Express One bucket"
      (let [base-creds {:access-key (System/getenv "AWS_ACCESS_KEY_ID")
                        :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
            bucket "your-test-bucket--use1-az1--x-s3"
            region "us-east-1"
            session-creds (s3-express/get-session-credentials bucket region base-creds)]
        (is (some? (:access-key session-creds))
            "Should have access key")
        (is (some? (:secret-key session-creds))
            "Should have secret key")
        (is (some? (:session-token session-creds))
            "Should have session token"))))

  (deftest integration-session-caching-test
    (testing "Session credentials are cached and reused"
      (let [base-creds {:access-key (System/getenv "AWS_ACCESS_KEY_ID")
                        :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
            bucket "your-test-bucket--use1-az1--x-s3"
            region "us-east-1"]
        ;; Clear cache
        (s3-express/clear-session-cache!)

        ;; First call should create new session
        (let [creds1 (s3-express/get-session-credentials bucket region base-creds)
              ;; Second call should use cached session
              creds2 (s3-express/get-session-credentials bucket region base-creds)]
          (is (= (:session-token creds1) (:session-token creds2))
              "Should reuse same session token from cache"))))))
