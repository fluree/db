(ns fluree.db.storage.s3-test
  "S3 storage integration tests"
  (:require [clojure.test :refer [deftest is testing]]
            [cognitect.aws.client.api :as aws]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

(defn setup-s3-test-env []
  (System/setProperty "aws.accessKeyId" "test")
  (System/setProperty "aws.secretAccessKey" "test")
  (System/setProperty "aws.region" "us-east-1"))

(deftest ^:integration s3-basic-integration-test
  (testing "Basic S3 integration (requires LocalStack)"
    (if-not (test-utils/s3-available?)
      (log/info "⏭️  Skipping S3 integration test - LocalStack not available at localhost:4566")
      (do
        (setup-s3-test-env)
        (let [bucket "fluree-test"
              prefix "integration"
              test-id (str "test-" (System/currentTimeMillis))]

          ;; Create bucket and wait for it
          (let [client (aws/client {:api :s3
                                    :endpoint-override {:protocol :http
                                                        :hostname "localhost"
                                                        :port 4566}})]
            (try
              (aws/invoke client {:op :CreateBucket :request {:Bucket bucket}})
              (catch Exception _ nil))
            ;; Wait a moment for bucket creation
            (Thread/sleep 100))

          ;; Test connection creation using connect-s3
          (let [conn @(fluree/connect-s3 {:s3-bucket bucket
                                          :s3-prefix prefix
                                          :s3-endpoint "http://localhost:4566"
                                          :cache-max-mb 50
                                          :parallelism 1})]

            (try
              (is (some? conn) "Connection should be created")

              ;; Test ledger creation
              (let [ledger @(fluree/create conn test-id)]
                (is (some? ledger) "Ledger should be created"))

              (finally
                @(fluree/disconnect conn)))))))))

(deftest ^:integration s3-full-workflow-test
  (testing "Complete S3 workflow: stage → commit → reload → query"
    (if-not (test-utils/s3-available?)
      (log/info "⏭️  Skipping S3 workflow test - LocalStack not available at localhost:4566")
      (do
        (setup-s3-test-env)
        (let [bucket "fluree-test"
              prefix "workflow"
              ledger-name (str "workflow-" (System/currentTimeMillis))
              context {"@vocab" "https://ns.flur.ee/ledger#"
                       "ex" "http://example.org/ns/"}
              test-data [{"@id" "ex:alice" "ex:name" "Alice"}]]

          ;; Create bucket and wait for it
          (let [client (aws/client {:api :s3
                                    :endpoint-override {:protocol :http
                                                        :hostname "localhost"
                                                        :port 4566}})]
            (try
              (aws/invoke client {:op :CreateBucket :request {:Bucket bucket}})
              (catch Exception _ nil))
            ;; Wait a moment for bucket creation
            (Thread/sleep 100))

          ;; Phase 1: Create and commit
          (let [conn1 @(fluree/connect-s3 {:s3-bucket bucket
                                           :s3-prefix prefix
                                           :s3-endpoint "http://localhost:4566"
                                           :cache-max-mb 50
                                           :parallelism 1})
                ledger @(fluree/create conn1 ledger-name)
                db0 (fluree/db ledger)
                db1 @(fluree/stage db0 {"@context" context "insert" test-data})
                committed @(fluree/commit! ledger db1)]

            (is (some? committed) "Data should commit to S3")
            @(fluree/disconnect conn1))

          ;; Brief pause for S3 consistency
          (Thread/sleep 1000)

          ;; Phase 2: Reload and verify
          (let [conn2 @(fluree/connect-s3 {:s3-bucket bucket
                                           :s3-prefix prefix
                                           :s3-endpoint "http://localhost:4566"
                                           :cache-max-mb 50
                                           :parallelism 1})]
            (try
              (let [loaded-ledger @(fluree/load conn2 ledger-name)
                    db (fluree/db loaded-ledger)
                    result @(fluree/query db {"@context" context
                                              "select" ["?name"]
                                              "where" [{"ex:name" "?name"}]})]

                (is (= [["Alice"]] result) "Data should persist through S3")
                (log/info "✅ S3 workflow test successful! Found:" result))
              (catch Exception e
                (log/error "ERROR in phase 2:" (.getMessage e))
                (throw e))
              (finally
                @(fluree/disconnect conn2)))))))))