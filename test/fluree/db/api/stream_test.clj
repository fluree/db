(ns fluree.db.api.stream-test
  (:require [clojure.core.async :as async :refer [go-loop <!]]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils])
  (:import (java.io StringReader)))

(defn str->reader
  "Creates a StringReader from a string for testing."
  [s]
  (StringReader. s))

;;; ---------------------------------------------------------------------------
;;; stream-insert tests (staging without commit)
;;; ---------------------------------------------------------------------------

(deftest ^:integration stream-insert-basic-test
  (testing "Stages NDJSON documents into database"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "stream/insert-basic")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert db0 (str->reader ndjson)
                                        {:context {"ex" "http://example.org/"}})]
      (is (= 2 (get-in result [:stats :lines-staged])))
      (is (some? (:db result)))

      ;; Query the staged database
      (let [names @(fluree/query (:db result)
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?name"
                                  :where   {"ex:name" "?name"}})]
        (is (= #{"Alice" "Bob"} (set names)))))))

(deftest ^:integration stream-insert-with-shared-context-test
  (testing "First line as context-only is extracted and applied"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "stream/shared-context")
          ;; First line is context-only
          ndjson "{\"@context\": {\"ex\": \"http://example.org/\"}}\n{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}"
          result @(fluree/stream-insert db0 (str->reader ndjson))]
      (is (= 1 (get-in result [:stats :lines-staged])))

      (let [names @(fluree/query (:db result)
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?name"
                                  :where   {"ex:name" "?name"}})]
        (is (= ["Alice"] names))))))

(deftest ^:integration stream-insert-error-modes-test
  (testing "Error mode :fail stops on first error"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "stream/error-fail")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\nnot json\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert db0 (str->reader ndjson)
                                        {:context {"ex" "http://example.org/"}
                                         :error-mode :fail})]
      (is (instance? Throwable result))))

  (testing "Error mode :skip continues past errors"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "stream/error-skip")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\nnot json\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert db0 (str->reader ndjson)
                                        {:context {"ex" "http://example.org/"}
                                         :error-mode :skip})]
      (is (= 2 (get-in result [:stats :lines-staged])))))

  (testing "Error mode :collect gathers all errors"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "stream/error-collect")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\nnot json\nalso not json\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert db0 (str->reader ndjson)
                                        {:context {"ex" "http://example.org/"}
                                         :error-mode :collect})]
      (is (= 2 (get-in result [:stats :lines-staged])))
      (is (= 2 (count (:errors result)))))))

;;; ---------------------------------------------------------------------------
;;; stream-insert! tests (with commit)
;;; ---------------------------------------------------------------------------

(deftest ^:integration stream-insert-commit-basic-test
  (testing "Streams and commits NDJSON documents"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/insert-commit")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert! conn "stream/insert-commit" (str->reader ndjson)
                                         {:context {"ex" "http://example.org/"}})]
      (is (= :success (:status result)))
      (is (= 2 (:lines-processed result)))
      (is (pos? (:batches-committed result)))
      (is (some? (:final-t result)))

      ;; Verify data is committed by loading fresh
      (let [db    @(fluree/db conn "stream/insert-commit")
            names @(fluree/query db
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?name"
                                  :where   {"ex:name" "?name"}})]
        (is (= #{"Alice" "Bob"} (set names)))))))

(deftest ^:integration stream-insert-commit-batching-test
  (testing "Commits in batches based on max-batch-lines"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/batching")
          ;; Generate 25 documents, with batch size of 10
          docs (for [i (range 25)]
                 (str "{\"@id\": \"ex:item" i "\", \"ex:value\": " i "}"))
          ndjson (str/join "\n" docs)
          progress-events (atom [])
          progress-ch (async/chan 100)]

      ;; Collect progress events
      (go-loop []
        (when-let [event (<! progress-ch)]
          (swap! progress-events conj event)
          (recur)))

      (let [result @(fluree/stream-insert! conn "stream/batching" (str->reader ndjson)
                                           {:context {"ex" "http://example.org/"}
                                            :max-batch-lines 10
                                            :progress-ch progress-ch})]
        ;; Allow progress events to be collected
        (Thread/sleep 100)
        (async/close! progress-ch)

        (is (= :success (:status result)))
        (is (= 25 (:lines-processed result)))
        ;; Should have committed at least 3 batches (10, 10, 5)
        (is (>= (:batches-committed result) 3))

        ;; Should have batch-committed events
        (let [batch-events (filter #(= :batch-committed (:type %)) @progress-events)]
          (is (>= (count batch-events) 3)))))))

(deftest ^:integration stream-insert-commit-empty-input-test
  (testing "Handles empty input gracefully"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/empty")
          result @(fluree/stream-insert! conn "stream/empty" (str->reader "")
                                         {:context {"ex" "http://example.org/"}})]
      (is (= :success (:status result)))
      (is (= 0 (:lines-processed result)))
      (is (= 0 (:batches-committed result))))))

(deftest ^:integration stream-insert-commit-error-handling-test
  (testing "Error mode :fail returns failed status"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/error-fail-commit")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\nnot json"
          result @(fluree/stream-insert! conn "stream/error-fail-commit" (str->reader ndjson)
                                         {:context {"ex" "http://example.org/"}
                                          :error-mode :fail})]
      (is (= :failed (:status result)))
      (is (some? (:error result)))))

  (testing "Error mode :collect returns partial status with errors"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/error-collect-commit")
          ndjson "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}\nnot json\n{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}"
          result @(fluree/stream-insert! conn "stream/error-collect-commit" (str->reader ndjson)
                                         {:context {"ex" "http://example.org/"}
                                          :error-mode :collect})]
      (is (= 2 (:lines-processed result)))
      (is (= 1 (count (:errors result)))))))

;;; ---------------------------------------------------------------------------
;;; Complex document tests
;;; ---------------------------------------------------------------------------

(deftest ^:integration stream-insert-nested-documents-test
  (testing "Handles nested JSON-LD documents"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/nested")
          ndjson (str "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\", \"ex:knows\": {\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}}\n"
                      "{\"@id\": \"ex:charlie\", \"ex:name\": \"Charlie\"}")
          result @(fluree/stream-insert! conn "stream/nested" (str->reader ndjson)
                                         {:context {"ex" "http://example.org/"}})]
      (is (= :success (:status result)))
      (is (= 2 (:lines-processed result)))

      ;; Query to verify nested data
      (let [db @(fluree/db conn "stream/nested")
            names @(fluree/query db
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?name"
                                  :where   {"ex:name" "?name"}})]
        ;; Should have Alice, Bob (nested), and Charlie
        (is (= #{"Alice" "Bob" "Charlie"} (set names)))))))

(deftest ^:integration stream-insert-graph-test
  (testing "Handles @graph containers"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "stream/graph")
          ndjson "{\"@graph\": [{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}, {\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}]}"
          result @(fluree/stream-insert! conn "stream/graph" (str->reader ndjson)
                                         {:context {"ex" "http://example.org/"}})]
      (is (= :success (:status result)))
      (is (= 1 (:lines-processed result)))

      (let [db @(fluree/db conn "stream/graph")
            names @(fluree/query db
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?name"
                                  :where   {"ex:name" "?name"}})]
        (is (= #{"Alice" "Bob"} (set names)))))))
