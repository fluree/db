(ns fluree.db.storage.s3-testcontainers-test
  "S3 storage integration tests using testcontainers and LocalStack
   
   These tests require Docker to be installed and running. They are tagged with
   ^:docker meta tag and are excluded from regular CI/CD runs.
   
   To run these tests:
   - All docker tests: clojure -M:docker-tests
   - Specific test: clojure -M:cljtest -m kaocha.runner --focus fluree.db.storage.s3-testcontainers-test
   
   These tests can be included in a weekly CI/CD job using the :docker-tests alias."
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.storage.s3 :as s3]
            [fluree.db.util.xhttp :as xhttp])
  (:import [org.testcontainers.containers.localstack LocalStackContainer]
           [org.testcontainers.containers.localstack LocalStackContainer$Service]
           [org.testcontainers.utility DockerImageName]))

(def ^:dynamic *localstack-container* nil)
(def ^:dynamic *s3-endpoint* nil)

(defn start-localstack-container []
  (let [docker-image (DockerImageName/parse "localstack/localstack:3.0.2")
        ^"[Lorg.testcontainers.containers.localstack.LocalStackContainer$Service;"
        services (into-array LocalStackContainer$Service [LocalStackContainer$Service/S3])
        ^LocalStackContainer container (doto (LocalStackContainer. docker-image)
                                         (.withServices services))]
    (.start container)
    {:container container
     :endpoint (.getEndpointOverride container LocalStackContainer$Service/S3)}))

(defn stop-localstack-container [container-info]
  (when-let [^LocalStackContainer container (:container container-info)]
    (.stop container)))

(defn create-s3-bucket
  "Create a bucket in LocalStack S3"
  [bucket-name endpoint]
  (.println System/out (str "Attempting to create bucket: " bucket-name " at endpoint: " endpoint))
  (try
    ;; For LocalStack, we need to use the direct endpoint without the bucket in hostname
    (let [url (str endpoint "/" bucket-name)
          response @(xhttp/put url "" {:headers {"Content-Type" "application/xml"}})]
      (.println System/out (str "Created bucket " bucket-name " Response status: " (:status response))))
    (catch Exception e
      (.println System/out (str "Failed to create bucket " bucket-name ": " (.getMessage e) " - " (pr-str e))))))

(defn localstack-fixture [f]
  ;; Set up AWS credentials as system properties
  (System/setProperty "aws.accessKeyId" "test")
  (System/setProperty "aws.secretKey" "test")
  (System/setProperty "aws.region" "us-east-1")

  (let [container-info (start-localstack-container)]
    (try
      (binding [*localstack-container* (:container container-info)
                *s3-endpoint* (str (:endpoint container-info))]
        (.println System/out (str "LocalStack endpoint: " *s3-endpoint*))
        ;; Wait a moment for LocalStack to be fully ready
        (Thread/sleep 2000)
        ;; Create test buckets
        (.println System/out "Creating buckets...")
        (create-s3-bucket "fluree-test" *s3-endpoint*)
        (create-s3-bucket "fluree-indexing-test" *s3-endpoint*)
        (.println System/out "Buckets created, running tests...")
        (f))
      (finally
        (stop-localstack-container container-info)))))

(use-fixtures :once localstack-fixture)

(deftest ^:integration ^:docker s3-testcontainers-basic-test
  (testing "Basic S3 operations with testcontainers"
    (let [bucket "fluree-test"]

      ;; Test Fluree connection with LocalStack endpoint override
      (with-redefs [s3/build-s3-url
                    (fn [bucket _region path]
                      ;; Override to use LocalStack endpoint instead of AWS
                      (str *s3-endpoint* "/" bucket "/" path))]
        (let [conn @(fluree/connect-s3 {:s3-bucket bucket
                                        :s3-prefix "test"
                                        :s3-endpoint *s3-endpoint*
                                        :cache-max-mb 50
                                        :parallelism 1})]
          (try
            (is (some? conn) "Connection should be created")

            ;; Test ledger creation
            (let [ledger-id "testcontainers-test"
                  ledger @(fluree/create conn ledger-id)]
              (is (some? ledger) "Ledger should be created")

              ;; Test basic operations
              (let [db @(fluree/update (fluree/db ledger)
                                       {"@context" {"ex" "http://example.org/ns/"}
                                        "insert" [{"@id" "ex:alice"
                                                   "@type" "ex:Person"
                                                   "ex:name" "Alice"}
                                                  {"@id" "ex:bob"
                                                   "@type" "ex:Person"
                                                   "ex:name" "Bob"}]})

                    ;; Commit the data
                    committed-db @(fluree/commit! ledger db)
                    ;; Query the data
                    results @(fluree/query committed-db
                                           {"@context" {"ex" "http://example.org/ns/"}
                                            "select" ["?s" "?name"]
                                            "where" {"@id" "?s"
                                                     "@type" "ex:Person"
                                                     "ex:name" "?name"}})]

                (is (= 2 (count results)) "Should have 2 results")
                (is (= #{["ex:alice" "Alice"] ["ex:bob" "Bob"]} (set results))
                    "Should return correct data")
                (is (some? committed-db) "Commit should succeed")

                ;; Test reload with fresh connection to verify data persistence
                (let [fresh-conn @(fluree/connect-s3 {:s3-bucket bucket
                                                      :s3-prefix "test"
                                                      :s3-endpoint *s3-endpoint*
                                                      :cache-max-mb 50
                                                      :parallelism 1})
                      reloaded @(fluree/load fresh-conn ledger-id)
                      reloaded-db (fluree/db reloaded)
                      reload-results @(fluree/query reloaded-db
                                                    {"@context" {"ex" "http://example.org/ns/"}
                                                     "select" ["?s" "?name"]
                                                     "where" {"@id" "?s"
                                                              "@type" "ex:Person"
                                                              "ex:name" "?name"}})]
                  (is (= results reload-results) "Reloaded data should match")
                  @(fluree/disconnect fresh-conn))))

            (finally
              @(fluree/disconnect conn))))))))

(deftest ^:integration ^:docker s3-testcontainers-indexing-test
  (testing "S3 storage with indexing using testcontainers"
    (let [bucket "fluree-indexing-test"]

      ;; Connect with indexing configuration using LocalStack endpoint override
      (with-redefs [s3/build-s3-url
                    (fn [bucket _region path]
                      ;; Override to use LocalStack endpoint instead of AWS
                      (str *s3-endpoint* "/" bucket "/" path))]
        (let [conn @(fluree/connect-s3 {:s3-bucket bucket
                                        :s3-prefix "indexing"
                                        :s3-endpoint *s3-endpoint*
                                        :cache-max-mb 50
                                        :parallelism 1
                                        :defaults {:indexing {:reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})]
          (try
            (let [ledger-id "indexing-test"
                  ledger @(fluree/create conn ledger-id)

                  ;; Add enough data to trigger indexing
                  db1 @(fluree/update (fluree/db ledger)
                                      {"@context" {"ex" "http://example.org/ns/"}
                                       "insert" (for [i (range 50)]
                                                  {"@id" (str "ex:person" i)
                                                   "@type" "ex:Person"
                                                   "ex:name" (str "Person " i)
                                                   "ex:age" i})})

                  ;; Commit to trigger indexing and query to verify data
                  db2 @(fluree/commit! ledger db1)
                  count-result @(fluree/query db2
                                              {"@context" {"ex" "http://example.org/ns/"}
                                               "select" "(count ?s)"
                                               "where" {"@id" "?s"
                                                        "@type" "ex:Person"}})
                  ;; Verify index files were created in S3 using our s3-list
                  store (s3/->S3Store nil (s3/get-credentials) bucket "us-east-1" "indexing")
                  list-ch (s3/s3-list store "")
                  objects-resp (<!! list-ch)
                  object-keys (map :key (:contents objects-resp))]

              (is (= [50] count-result) "Should have 50 persons")
              (is (pos? (count object-keys)) "Should have objects in S3")
              (is (some #(re-find #"index" %) object-keys)
                  "Should have index files"))

            (finally
              @(fluree/disconnect conn))))))))