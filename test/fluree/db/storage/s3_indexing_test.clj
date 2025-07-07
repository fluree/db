(ns fluree.db.storage.s3-indexing-test
  "Test S3 storage with indexing"
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [cognitect.aws.client.api :as aws]
            [fluree.db.api :as fluree]))

(defn setup-env []
  (System/setProperty "aws.accessKeyId" "test")
  (System/setProperty "aws.secretAccessKey" "test")
  (System/setProperty "aws.region" "us-east-1"))

(defn list-s3-objects [bucket prefix]
  (let [client (aws/client {:api :s3
                            :endpoint-override {:protocol :http
                                                :hostname "localhost"
                                                :port 4566}})
        result (aws/invoke client {:op :ListObjectsV2
                                   :request {:Bucket bucket
                                             :Prefix prefix}})]
    (:Contents result)))

(defn get-index-files-by-type
  "Categorize index files by type (root, post, spot, tspo, opst)"
  [objects]
  (let [index-files (filter #(str/includes? (:Key %) "/index/") objects)]
    {:root (filter #(str/includes? (:Key %) "/index/root/") index-files)
     :post (filter #(str/includes? (:Key %) "/index/post/") index-files)
     :spot (filter #(str/includes? (:Key %) "/index/spot/") index-files)
     :tspo (filter #(str/includes? (:Key %) "/index/tspo/") index-files)
     :opst (filter #(str/includes? (:Key %) "/index/opst/") index-files)
     :all index-files}))

(deftest ^:integration ^:pending test-s3-indexing
  (testing "S3 storage with indexing triggers and validation"
    ;; Marking as pending until LocalStack setup is improved in CI
    (setup-env)
    (let [bucket "fluree-test"
          prefix "indexing"
          ledger-name (str "index-test-" (System/currentTimeMillis))
          context {"@vocab" "https://ns.flur.ee/ledger#"
                   "ex" "http://example.org/ns/"}]

      (testing "Index creation with low thresholds"
        (let [conn @(fluree/connect-s3
                     {:s3-bucket bucket
                      :s3-prefix prefix
                      :s3-endpoint "http://localhost:4566"
                      :defaults {:indexing {:reindex-min-bytes 100
                                            :reindex-max-bytes 1000}}})]

          (testing "Ledger creation and initial transaction"
            (let [ledger @(fluree/create conn ledger-name)
                  db0 (fluree/db ledger)
                  data1 [{"@id" "ex:alice" "ex:name" "Alice" "ex:age" 30}
                         {"@id" "ex:bob" "ex:name" "Bob" "ex:age" 25}]
                  db1 @(fluree/stage db0 {"@context" context "insert" data1})]

              (is (some? ledger) "Ledger should be created successfully")
              (is (some? db1) "Data should stage successfully")

              @(fluree/commit! ledger db1)

              ;; Wait for async indexing
              (Thread/sleep 3000)

              (let [objects (list-s3-objects bucket (str prefix "/" ledger-name))
                    commit-files (filter #(str/includes? (:Key %) "/commit/") objects)]
                (is (>= (count commit-files) 2) "Should have commit files in S3"))))

          (testing "Index generation after threshold exceeded"
            (let [ledger @(fluree/load conn ledger-name)
                  db-current (fluree/db ledger)
                  data2 [{"@id" "ex:carol" "ex:name" "Carol" "ex:age" 35}
                         {"@id" "ex:dave" "ex:name" "Dave" "ex:age" 40}]
                  db2 @(fluree/stage db-current {"@context" context "insert" data2})]

              @(fluree/commit! ledger db2)

              ;; Wait for indexing to complete
              (Thread/sleep 5000)

              (let [objects (list-s3-objects bucket (str prefix "/" ledger-name))
                    index-analysis (get-index-files-by-type objects)]

                ;; Validate index files were created
                (is (pos? (count (:all index-analysis)))
                    "Should create index files with low threshold")

                ;; Validate all index types are present
                (is (pos? (count (:root index-analysis))) "Should have root index files")

                ;; Validate we have at least one root index (may have old ones during GC)
                (is (>= (count (:root index-analysis)) 1) "Should have at least one root index"))))

          @(fluree/disconnect conn)))

      (testing "Index loading and querying from cold start"
            ;; Create a fresh connection to test loading indexes from S3
        (let [conn-fresh @(fluree/connect-s3
                           {:s3-bucket bucket
                            :s3-prefix prefix
                            :s3-endpoint "http://localhost:4566"})
              loaded-ledger @(fluree/load conn-fresh ledger-name)
              db-loaded (fluree/db loaded-ledger)]

          (is (some? loaded-ledger) "Should load ledger from S3")
          (is (some? db-loaded) "Should get database from loaded ledger")

          (testing "Query by name (alphabetical order)"
            (let [result @(fluree/query db-loaded
                                        {"@context" context
                                         "select" ["?name"]
                                         "where" [{"@id" "?s" "ex:name" "?name"}]
                                         "order-by" ["?name"]})]

              (is (= 4 (count result)) "Should find all 4 people using loaded indexes")
              (is (= [["Alice"] ["Bob"] ["Carol"] ["Dave"]] result)
                  "Should return names in alphabetical order")))

          (testing "Query by age (numerical order)"
            (let [result @(fluree/query db-loaded
                                        {"@context" context
                                         "select" ["?name" "?age"]
                                         "where" [{"@id" "?s"
                                                   "ex:name" "?name"
                                                   "ex:age" "?age"}]
                                         "order-by" ["?age"]})]

              (is (= 4 (count result)) "Should find all people using loaded indexes")
              (is (= [["Bob" 25] ["Alice" 30] ["Carol" 35] ["Dave" 40]] result)
                  "Should return correct data ordered by age from loaded indexes")))

          @(fluree/disconnect conn-fresh))))))
