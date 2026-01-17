(ns stream-explore
  "Development namespace for exploring and testing the streaming insert API.

   Run the various comment forms to test:
   1. Data integrity across batch boundaries
   2. Novelty backpressure handling
   3. Context handling (shared context, inline context, opts context)

   Usage: Load this namespace in a REPL and evaluate the comment forms."
  (:require [clojure.core.async :as async :refer [go-loop <!]]
            [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json])
  (:import (java.io StringReader File)))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; NDJSON Generation Utilities
;;; ---------------------------------------------------------------------------

(defn random-name
  "Generate a random name for test data."
  []
  (let [first-names ["Alex" "Jordan" "Taylor" "Morgan" "Casey" "Riley" "Quinn" "Avery"
                     "Skylar" "Dakota" "Reese" "Finley" "Sage" "River" "Phoenix" "Blake"]
        last-names ["Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis"
                    "Rodriguez" "Martinez" "Hernandez" "Lopez" "Gonzalez" "Wilson" "Anderson"]]
    (str (rand-nth first-names) " " (rand-nth last-names))))

(defn random-age
  "Generate a random age between 18 and 80."
  []
  (+ 18 (rand-int 63)))

(defn generate-person-json
  "Generate a JSON string for a single person document."
  [id & {:keys [name age friend-ids]}]
  (let [doc (cond-> {"@id" (str "ex:" id)}
              name       (assoc "ex:name" name)
              age        (assoc "ex:age" age)
              friend-ids (assoc "ex:friend" (mapv #(hash-map "@id" (str "ex:" %)) friend-ids)))]
    (json/stringify doc)))

(defn generate-ndjson-stream
  "Generate an NDJSON string with:
   - First line: andrew who is friends with chase
   - Middle lines: n random people
   - Last line: chase with a name

   This allows testing data integrity across batch boundaries."
  [n & {:keys [include-context-line?]}]
  (let [lines (cond-> []
                ;; Optional shared context line
                include-context-line?
                (conj (json/stringify {"@context" {"ex" "http://example.org/ns/"
                                                   "@vocab" "http://example.org/ns/"}}))

                ;; First data line: andrew knows chase
                true
                (conj (generate-person-json "andrew"
                                            :name "Andrew"
                                            :age 35
                                            :friend-ids ["chase"]))

                ;; Middle lines: random people
                true
                (into (for [i (range n)]
                        (generate-person-json (str "person-" i)
                                              :name (random-name)
                                              :age (random-age))))

                ;; Last line: chase with name
                true
                (conj (generate-person-json "chase" :name "Chase")))]
    (str/join "\n" lines)))

(defn generate-ndjson-file
  "Generate a temporary NDJSON file with the specified number of documents.
   Returns the File object."
  [n & {:keys [include-context-line?] :as opts}]
  (let [file (File/createTempFile "stream-test-" ".ndjson")]
    (.deleteOnExit file)
    (spit file (generate-ndjson-stream n opts))
    (println "Generated" (+ n 2 (if include-context-line? 1 0)) "lines to" (.getAbsolutePath file))
    file))

(defn str->reader
  "Create a StringReader from a string."
  [s]
  (StringReader. s))

;;; ---------------------------------------------------------------------------
;;; Progress Monitoring
;;; ---------------------------------------------------------------------------

(defn start-progress-monitor
  "Start a go-loop that prints progress events. Returns the progress channel."
  []
  (let [progress-ch (async/chan 100)]
    (go-loop []
      (when-let [event (<! progress-ch)]
        (case (:type event)
          :batch-committed
          (println (format "  [Batch %d] Committed %d lines, t=%d"
                           (:batch-num event)
                           (:lines-in-batch event)
                           (:t event)))

          :backpressure
          (println (format "  [Backpressure] %s - novelty ratio: %.2f, waited: %dms"
                           (name (:action event))
                           (double (:novelty-ratio event))
                           (:waited-ms event)))

          :progress
          (when (= :backpressure (:state event))
            (println (format "  [State] %s - lines read: %d, staged: %d"
                             (name (:state event))
                             (:lines-read event)
                             (:lines-staged event))))

          ;; Default: print event type
          (println "  [Event]" (:type event) (dissoc event :type)))
        (recur)))
    progress-ch))

;;; ---------------------------------------------------------------------------
;;; Test Helpers
;;; ---------------------------------------------------------------------------

(defn verify-andrew-knows-chase
  "Query to verify andrew's friend (chase) has the expected name.
   Returns the friend's name or nil if not found."
  [db]
  (let [result @(fluree/query db
                              {:context {"ex" "http://example.org/ns/"}
                               :select "?friendName"
                               :where {"@id" "ex:andrew"
                                       "ex:friend" {"ex:name" "?friendName"}}})]
    (first result)))

(defn count-people
  "Count total people in the database."
  [db]
  (let [result @(fluree/query db
                              {:context {"ex" "http://example.org/ns/"}
                               :select "(count ?p)"
                               :where {"@id" "?p"
                                       "ex:name" "?name"}})]
    (first result)))

;;; ---------------------------------------------------------------------------
;;; Test 1: Data Integrity Across Batch Boundaries
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; TEST 1: Data Integrity Across Batch Boundaries
  ;; ==========================================================================
  ;;
  ;; This test verifies that data inserted at the beginning of a stream
  ;; (andrew who knows chase) can correctly reference data inserted at the
  ;; end of the stream (chase with name), even when multiple batch commits
  ;; occur in between.

  ;; Create a fresh connection and ledger
  (def conn @(fluree/connect-memory))
  (def ledger @(fluree/create conn "test/stream-integrity"))

  ;; Generate 5000 random documents between andrew and chase
  ;; With max-batch-lines of 100, this should create ~50 batches
  (def ndjson-data (generate-ndjson-stream 5000))

  ;; Check the first and last few lines
  (let [lines (str/split-lines ndjson-data)]
    (println "Total lines:" (count lines))
    (println "\nFirst 3 lines:")
    (doseq [line (take 3 lines)]
      (println " " line))
    (println "\nLast 3 lines:")
    (doseq [line (take-last 3 lines)]
      (println " " line)))

  ;; Stream insert with progress monitoring
  (def progress-ch (start-progress-monitor))

  (println "\nStarting stream insert...")
  (def result
    @(fluree/stream-insert! conn "test/stream-integrity"
                            (str->reader ndjson-data)
                            {:context {"ex" "http://example.org/ns/"}
                             :max-batch-lines 100  ; Small batches to force many commits
                             :progress-ch progress-ch}))

  (async/close! progress-ch)
  (Thread/sleep 100) ; Let final events print

  ;; Check the result
  (println "\n=== Result ===")
  (println "Status:" (:status result))
  (println "Lines processed:" (:lines-processed result))
  (println "Batches committed:" (:batches-committed result))
  (println "Final t:" (:final-t result))

  ;; Verify data integrity - can we traverse from andrew to chase?
  (def db @(fluree/db conn "test/stream-integrity"))

  (println "\n=== Data Integrity Check ===")
  (let [chase-name (verify-andrew-knows-chase db)]
    (println "Andrew's friend's name:" chase-name)
    (if (= "Chase" chase-name)
      (println "✓ SUCCESS: Data integrity maintained across" (:batches-committed result) "batch commits!")
      (println "✗ FAILURE: Expected 'Chase', got" chase-name)))

  (println "\nTotal people with names:" (count-people db))

  ;; Query to see andrew's full data
  @(fluree/query db {:context {"ex" "http://example.org/ns/"}
                     :select {"ex:andrew" ["*" {"ex:friend" ["*"]}]}})

  )

;;; ---------------------------------------------------------------------------
;;; Test 2: Novelty Backpressure
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; TEST 2: Novelty Backpressure
  ;; ==========================================================================
  ;;
  ;; This test forces backpressure by using a very small reindex-max-bytes
  ;; setting, ensuring the stream pauses and resumes correctly.

  ;; Create connection with very small max novelty to force backpressure
  (def bp-conn @(fluree/connect-memory {:defaults {:reindex-max-bytes 50000}})) ; 50KB max
  (def bp-ledger @(fluree/create bp-conn "test/stream-backpressure"))

  ;; Generate enough data to exceed 50KB novelty multiple times
  ;; Each person doc is roughly 100-200 bytes, so 500 should exceed it
  (def bp-ndjson (generate-ndjson-stream 500))

  (def bp-progress-ch (start-progress-monitor))

  (println "\n=== Testing Backpressure (max novelty: 50KB) ===")
  (println "This should trigger backpressure events...\n")

  (def bp-result
    @(fluree/stream-insert! bp-conn "test/stream-backpressure"
                            (str->reader bp-ndjson)
                            {:context {"ex" "http://example.org/ns/"}
                             :max-batch-lines 50   ; Commit every 50 lines
                             :batch-threshold 0.5  ; Commit at 50% novelty
                             :progress-ch bp-progress-ch}))

  (async/close! bp-progress-ch)
  (Thread/sleep 100)

  (println "\n=== Backpressure Test Result ===")
  (println "Status:" (:status bp-result))
  (println "Lines processed:" (:lines-processed bp-result))
  (println "Batches committed:" (:batches-committed bp-result))

  ;; Verify data integrity despite backpressure
  (def bp-db @(fluree/db bp-conn "test/stream-backpressure"))
  (let [chase-name (verify-andrew-knows-chase bp-db)]
    (if (= "Chase" chase-name)
      (println "✓ SUCCESS: Stream stayed intact through backpressure!")
      (println "✗ FAILURE: Data corruption after backpressure")))

  )

;;; ---------------------------------------------------------------------------
;;; Test 3: Context Handling Variations
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; TEST 3A: Context via Options
  ;; ==========================================================================

  (def ctx-conn @(fluree/connect-memory))
  (def ctx-ledger-a @(fluree/create ctx-conn "test/context-via-opts"))

  ;; NDJSON without any context - relies on opts
  (def ndjson-no-ctx
    "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\", \"ex:friend\": {\"@id\": \"ex:bob\"}}
{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}")

  (println "\n=== Test 3A: Context via Options ===")
  (def result-a
    @(fluree/stream-insert! ctx-conn "test/context-via-opts"
                            (str->reader ndjson-no-ctx)
                            {:context {"ex" "http://example.org/ns/"}}))

  (println "Status:" (:status result-a))

  (def db-a @(fluree/db ctx-conn "test/context-via-opts"))
  @(fluree/query db-a {:context {"ex" "http://example.org/ns/"}
                       :select {"ex:alice" ["*" {"ex:friend" ["*"]}]}})

  ;; ==========================================================================
  ;; TEST 3B: Context as First Line (Context-Only Line)
  ;; ==========================================================================

  (def ctx-ledger-b @(fluree/create ctx-conn "test/context-first-line"))

  ;; First line is context-only (no data, just @context)
  (def ndjson-ctx-first
    "{\"@context\": {\"ex\": \"http://example.org/ns/\"}}
{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\", \"ex:friend\": {\"@id\": \"ex:bob\"}}
{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}")

  (println "\n=== Test 3B: Context as First Line ===")
  (def result-b
    @(fluree/stream-insert! ctx-conn "test/context-first-line"
                            (str->reader ndjson-ctx-first)
                            {})) ; No context in opts!

  (println "Status:" (:status result-b))
  (println "Lines processed:" (:lines-processed result-b) "(should be 2, context line not counted)")

  (def db-b @(fluree/db ctx-conn "test/context-first-line"))
  @(fluree/query db-b {:context {"ex" "http://example.org/ns/"}
                       :select {"ex:alice" ["*" {"ex:friend" ["*"]}]}})

  ;; ==========================================================================
  ;; TEST 3C: Inline Context Per Document
  ;; ==========================================================================

  (def ctx-ledger-c @(fluree/create ctx-conn "test/context-inline"))

  ;; Each document has its own @context
  (def ndjson-ctx-inline
    "{\"@context\": {\"ex\": \"http://example.org/ns/\"}, \"@id\": \"ex:alice\", \"ex:name\": \"Alice\", \"ex:friend\": {\"@id\": \"ex:bob\"}}
{\"@context\": {\"ex\": \"http://example.org/ns/\"}, \"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}")

  (println "\n=== Test 3C: Inline Context Per Document ===")
  (def result-c
    @(fluree/stream-insert! ctx-conn "test/context-inline"
                            (str->reader ndjson-ctx-inline)
                            {}))

  (println "Status:" (:status result-c))

  (def db-c @(fluree/db ctx-conn "test/context-inline"))
  @(fluree/query db-c {:context {"ex" "http://example.org/ns/"}
                       :select {"ex:alice" ["*" {"ex:friend" ["*"]}]}})

  ;; ==========================================================================
  ;; TEST 3D: Mixed - Shared Context with Per-Document Overrides
  ;; ==========================================================================

  (def ctx-ledger-d @(fluree/create ctx-conn "test/context-mixed"))

  ;; First line shared context, but second doc overrides with additional context
  (def ndjson-ctx-mixed
    "{\"@context\": {\"ex\": \"http://example.org/ns/\"}}
{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}
{\"@context\": {\"schema\": \"http://schema.org/\"}, \"@id\": \"ex:bob\", \"ex:name\": \"Bob\", \"schema:email\": \"bob@example.org\"}")

  (println "\n=== Test 3D: Mixed Context (Shared + Override) ===")
  (def result-d
    @(fluree/stream-insert! ctx-conn "test/context-mixed"
                            (str->reader ndjson-ctx-mixed)
                            {}))

  (println "Status:" (:status result-d))

  (def db-d @(fluree/db ctx-conn "test/context-mixed"))

  ;; Query alice (uses shared context)
  (println "\nAlice (shared context):")
  @(fluree/query db-d {:context {"ex" "http://example.org/ns/"}
                       :select {"ex:alice" ["*"]}})

  ;; Query bob (has additional schema context)
  (println "\nBob (shared + inline context):")
  @(fluree/query db-d {:context {"ex" "http://example.org/ns/"
                                 "schema" "http://schema.org/"}
                       :select {"ex:bob" ["*"]}})

  )

;;; ---------------------------------------------------------------------------
;;; Test 4: Large File Performance
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; TEST 4: Large File Stream Insert
  ;; ==========================================================================
  ;;
  ;; Generate a large file and stream it in to test real-world performance.

  ;; Generate 100K documents
  (println "Generating 100K document NDJSON file...")
  (def large-file (generate-ndjson-file 100000))
  (println "File size:" (/ (.length large-file) 1024.0 1024.0) "MB")

  (def perf-conn @(fluree/connect-memory))
  (def perf-ledger @(fluree/create perf-conn "test/stream-performance"))

  (def perf-progress-ch (start-progress-monitor))

  (println "\n=== Starting Large File Stream Insert ===")
  (def start-time (System/currentTimeMillis))

  (def perf-result
    @(fluree/stream-insert! perf-conn "test/stream-performance"
                            (.getAbsolutePath large-file) ; File path string
                            {:context {"ex" "http://example.org/ns/"}
                             :max-batch-lines 1000
                             :progress-ch perf-progress-ch}))

  (def elapsed (- (System/currentTimeMillis) start-time))

  (async/close! perf-progress-ch)
  (Thread/sleep 100)

  (println "\n=== Performance Results ===")
  (println "Status:" (:status perf-result))
  (println "Lines processed:" (:lines-processed perf-result))
  (println "Batches committed:" (:batches-committed perf-result))
  (println "Total time:" (/ elapsed 1000.0) "seconds")
  (println "Throughput:" (/ (:lines-processed perf-result) (/ elapsed 1000.0)) "docs/sec")

  ;; Verify data integrity
  (def perf-db @(fluree/db perf-conn "test/stream-performance"))
  (let [chase-name (verify-andrew-knows-chase perf-db)]
    (if (= "Chase" chase-name)
      (println "✓ Data integrity verified!")
      (println "✗ Data integrity failed!")))

  ;; Clean up
  (.delete large-file)

  )

;;; ---------------------------------------------------------------------------
;;; Test 5: Error Handling
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; TEST 5: Error Handling Modes
  ;; ==========================================================================

  (def err-conn @(fluree/connect-memory))

  ;; NDJSON with some invalid lines
  (def ndjson-with-errors
    "{\"@id\": \"ex:alice\", \"ex:name\": \"Alice\"}
not valid json
{\"@id\": \"ex:bob\", \"ex:name\": \"Bob\"}
also {invalid json
{\"@id\": \"ex:charlie\", \"ex:name\": \"Charlie\"}")

  ;; Test :fail mode (default)
  (println "\n=== Test 5A: Error Mode :fail ===")
  (def err-ledger-a @(fluree/create err-conn "test/error-fail"))
  (def err-result-a
    @(fluree/stream-insert! err-conn "test/error-fail"
                            (str->reader ndjson-with-errors)
                            {:context {"ex" "http://example.org/ns/"}
                             :error-mode :fail}))

  (println "Status:" (:status err-result-a))
  (when (:error err-result-a)
    (println "Error:" (ex-message (:error err-result-a))))

  ;; Test :skip mode
  (println "\n=== Test 5B: Error Mode :skip ===")
  (def err-ledger-b @(fluree/create err-conn "test/error-skip"))
  (def err-result-b
    @(fluree/stream-insert! err-conn "test/error-skip"
                            (str->reader ndjson-with-errors)
                            {:context {"ex" "http://example.org/ns/"}
                             :error-mode :skip}))

  (println "Status:" (:status err-result-b))
  (println "Lines processed:" (:lines-processed err-result-b))

  ;; Test :collect mode
  (println "\n=== Test 5C: Error Mode :collect ===")
  (def err-ledger-c @(fluree/create err-conn "test/error-collect"))
  (def err-result-c
    @(fluree/stream-insert! err-conn "test/error-collect"
                            (str->reader ndjson-with-errors)
                            {:context {"ex" "http://example.org/ns/"}
                             :error-mode :collect}))

  (println "Status:" (:status err-result-c))
  (println "Lines processed:" (:lines-processed err-result-c))
  (println "Errors collected:" (count (:errors err-result-c)))
  (doseq [err (:errors err-result-c)]
    (println "  Line" (:line-num err) "-" (:error err)))

  )

;;; ---------------------------------------------------------------------------
;;; Quick Smoke Test
;;; ---------------------------------------------------------------------------

(comment
  ;; ==========================================================================
  ;; QUICK SMOKE TEST - Run this to verify basic functionality
  ;; ==========================================================================

  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "STREAM INSERT API - QUICK SMOKE TEST")
  (println "\n" (str/join (repeat 60 "=")) "\n")

  (def smoke-conn @(fluree/connect-memory))
  (def smoke-ledger @(fluree/create smoke-conn "test/smoke"))

  ;; Small test: 50 documents with batches of 10
  (def smoke-ndjson (generate-ndjson-stream 50))

  (def smoke-progress (start-progress-monitor))

  (def smoke-result
    @(fluree/stream-insert! smoke-conn "test/smoke"
                            (str->reader smoke-ndjson)
                            {:context {"ex" "http://example.org/ns/"}
                             :max-batch-lines 10
                             :progress-ch smoke-progress}))

  (async/close! smoke-progress)
  (Thread/sleep 100)

  (println "\n--- Results ---")
  (println "Status:" (:status smoke-result))
  (println "Lines:" (:lines-processed smoke-result))
  (println "Batches:" (:batches-committed smoke-result))

  (def smoke-db @(fluree/db smoke-conn "test/smoke"))

  (let [chase-name (verify-andrew-knows-chase smoke-db)
        people-count (count-people smoke-db)]
    (println "\n--- Verification ---")
    (println "Andrew's friend:" chase-name (if (= "Chase" chase-name) "✓" "✗"))
    (println "Total people:" people-count (if (= 52 people-count) "✓" "✗"))
    (println)
    (if (and (= "Chase" chase-name) (= 52 people-count) (= :success (:status smoke-result)))
      (println "✓✓✓ SMOKE TEST PASSED ✓✓✓")
      (println "✗✗✗ SMOKE TEST FAILED ✗✗✗")))

  )
