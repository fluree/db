(ns ^:iceberg fluree.db.tabular.seekable-stream-test
  "Tests for the block-caching SeekableInputStream implementation.

   Validates:
   - Cache hit/miss behavior
   - Block boundary alignment
   - Shared cache across multiple streams
   - Correct data reading with seeks"
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.storage :as storage]
            [fluree.db.tabular.seekable-stream :as seekable]))

;;; ---------------------------------------------------------------------------
;;; Mock Store Implementation
;;; ---------------------------------------------------------------------------

(defrecord MockRangeStore [data call-log]
  storage/RangeReadableStore
  (read-bytes-range [_ path offset length]
    (async/go
      (swap! call-log update :range-calls (fnil conj [])
             {:path path :offset offset :length length})
      (let [data-bytes ^bytes @data
            end (min (+ offset length) (alength data-bytes))
            actual-len (- end offset)]
        (when (pos? actual-len)
          (let [result (byte-array actual-len)]
            (System/arraycopy data-bytes offset result 0 actual-len)
            result)))))

  storage/StatStore
  (stat [_ path]
    (async/go
      (swap! call-log update :stat-calls (fnil conj []) {:path path})
      {:size (alength ^bytes @data)}))

  storage/ByteStore
  (read-bytes [_ path]
    (async/go
      (swap! call-log update :full-reads (fnil conj []) {:path path})
      @data))
  (write-bytes [_ _path _bytes]
    (throw (ex-info "MockRangeStore is read-only" {})))
  (swap-bytes [_ _path _f]
    (throw (ex-info "MockRangeStore is read-only" {}))))

(defn- create-mock-store
  "Create a mock store with deterministic data.
   Returns [store call-log-atom]."
  [size]
  (let [;; Create deterministic data: each byte is (mod position 256)
        data (byte-array (for [i (range size)] (unchecked-byte (mod i 256))))
        call-log (atom {})]
    [(->MockRangeStore (atom data) call-log) call-log]))

;;; ---------------------------------------------------------------------------
;;; Unit Tests: Cache Hit Behavior
;;; ---------------------------------------------------------------------------

(deftest cache-hit-test
  (testing "Second stream reuses cached blocks"
    (let [file-size (* 16 1024)  ;; 16KB file
          block-size (* 4 1024)   ;; 4KB blocks
          [store call-log] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/file.parquet"

          ;; Create first stream and read some data
          stream1 (seekable/create-seekable-input-stream
                   store path file-size {:block-size block-size :cache cache})
          buf1 (byte-array 100)]

      ;; Read from first stream (should fetch block 0)
      (.read stream1 buf1)
      (.close stream1)

      (is (= 1 (count (:range-calls @call-log)))
          "First stream should make 1 range call for block 0")

      ;; Create second stream with SAME cache
      (let [stream2 (seekable/create-seekable-input-stream
                     store path file-size {:block-size block-size :cache cache})
            buf2 (byte-array 100)]

        ;; Read from same offset (should hit cache)
        (.read stream2 buf2)
        (.close stream2)

        (is (= 1 (count (:range-calls @call-log)))
            "Second stream should NOT make additional range calls (cache hit)")

        ;; Verify both reads got the same data
        (is (java.util.Arrays/equals buf1 buf2)
            "Both streams should return identical data")))))

(deftest cache-miss-different-blocks-test
  (testing "Reading different blocks causes cache misses"
    (let [file-size (* 16 1024)  ;; 16KB file
          block-size (* 4 1024)   ;; 4KB blocks (4 blocks total)
          [store call-log] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/file.parquet"

          stream (seekable/create-seekable-input-stream
                  store path file-size {:block-size block-size :cache cache})
          buf (byte-array 100)]

      ;; Read from block 0
      (.read stream buf)
      (is (= 1 (count (:range-calls @call-log))))

      ;; Seek to block 2 and read
      (.seek stream (* 2 block-size))
      (.read stream buf)
      (is (= 2 (count (:range-calls @call-log)))
          "Reading from different block should cause cache miss")

      ;; Seek back to block 0 (should be cached)
      (.seek stream 0)
      (.read stream buf)
      (is (= 2 (count (:range-calls @call-log)))
          "Re-reading block 0 should hit cache")

      (.close stream))))

(deftest block-boundary-alignment-test
  (testing "Range reads align to block boundaries"
    (let [file-size (* 16 1024)
          block-size (* 4 1024)
          [store call-log] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/file.parquet"

          stream (seekable/create-seekable-input-stream
                  store path file-size {:block-size block-size :cache cache})
          buf (byte-array 100)]

      ;; Seek to middle of block 1 and read
      (.seek stream (+ block-size 1000))
      (.read stream buf)

      ;; Should fetch entire block 1 (offset 4096, length 4096)
      (let [range-call (first (:range-calls @call-log))]
        (is (= block-size (:offset range-call))
            "Range read should start at block boundary")
        (is (= block-size (:length range-call))
            "Range read should request full block"))

      (.close stream))))

(deftest cross-block-read-test
  (testing "Reads spanning multiple blocks fetch all needed blocks"
    (let [file-size (* 16 1024)
          block-size (* 4 1024)
          [store call-log] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/file.parquet"

          stream (seekable/create-seekable-input-stream
                  store path file-size {:block-size block-size :cache cache})
          ;; Buffer large enough to span 2 blocks
          buf (byte-array (* 2 block-size))]

      ;; Read from start (will need blocks 0 and 1)
      (let [bytes-read (.read stream buf)]
        (is (= (* 2 block-size) bytes-read)
            "Should read full buffer"))

      (is (= 2 (count (:range-calls @call-log)))
          "Should fetch 2 blocks for cross-block read")

      (.close stream))))

(deftest shared-cache-multiple-paths-test
  (testing "Shared cache isolates data by path"
    (let [file-size (* 8 1024)
          block-size (* 4 1024)
          [store call-log] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          buf (byte-array 100)

          stream1 (seekable/create-seekable-input-stream
                   store "path/a.parquet" file-size {:block-size block-size :cache cache})
          stream2 (seekable/create-seekable-input-stream
                   store "path/b.parquet" file-size {:block-size block-size :cache cache})]

      ;; Read from both streams
      (.read stream1 buf)
      (.read stream2 buf)

      ;; Each path should cause its own cache miss
      (is (= 2 (count (:range-calls @call-log)))
          "Different paths should cause separate cache misses")

      ;; Verify the calls were for different paths
      (let [paths (set (map :path (:range-calls @call-log)))]
        (is (= #{"path/a.parquet" "path/b.parquet"} paths)))

      (.close stream1)
      (.close stream2))))

(deftest data-integrity-test
  (testing "Cached data matches original"
    (let [file-size 1000
          block-size 256
          [store _] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/data.bin"

          stream (seekable/create-seekable-input-stream
                  store path file-size {:block-size block-size :cache cache})]

      ;; Read all data
      (let [buf (byte-array file-size)
            bytes-read (.read stream buf)]
        (is (= file-size bytes-read))

        ;; Verify each byte matches expected pattern
        (doseq [i (range file-size)]
          (is (= (unchecked-byte (mod i 256)) (aget buf i))
              (str "Byte at position " i " should match"))))

      (.close stream))))

(deftest seek-and-read-test
  (testing "Seek positions correctly after various operations"
    (let [file-size 1000
          block-size 256
          [store _] (create-mock-store file-size)
          cache (seekable/create-cache {:max-bytes (* 64 1024)})
          path "test/seek.bin"

          stream (seekable/create-seekable-input-stream
                  store path file-size {:block-size block-size :cache cache})]

      ;; Initial position
      (is (= 0 (.getPos stream)))

      ;; Seek to middle
      (.seek stream 500)
      (is (= 500 (.getPos stream)))

      ;; Read single byte
      (let [b (.read stream)]
        (is (= (mod 500 256) b))
        (is (= 501 (.getPos stream))))

      ;; Skip some bytes
      (let [skipped (.skip stream 100)]
        (is (= 100 skipped))
        (is (= 601 (.getPos stream))))

      ;; Seek back
      (.seek stream 0)
      (is (= 0 (.getPos stream)))

      (.close stream))))

;;; ---------------------------------------------------------------------------
;;; Cache Configuration Tests
;;; ---------------------------------------------------------------------------

(deftest cache-creation-test
  (testing "create-cache returns valid Caffeine cache"
    (let [cache (seekable/create-cache {:max-bytes (* 10 1024 1024)
                                        :ttl-minutes 10})]
      (is (instance? com.github.benmanes.caffeine.cache.Cache cache))

      ;; Can put and get values
      (.put cache "test-key" (byte-array 100))
      (is (some? (.getIfPresent cache "test-key"))))))

(deftest cache-eviction-test
  (testing "Cache evicts entries when over capacity"
    (let [;; Very small cache: 1KB max
          cache (seekable/create-cache {:max-bytes 1024})
          block-size 512]

      ;; Add two 512-byte blocks (fills cache)
      (.put cache ["path" block-size 0] (byte-array 512))
      (.put cache ["path" block-size 1] (byte-array 512))

      ;; Force cleanup
      (.cleanUp cache)

      ;; Add a third block - should trigger eviction
      (.put cache ["path" block-size 2] (byte-array 512))

      ;; Force cleanup again
      (.cleanUp cache)

      ;; Cache should have evicted at least one entry
      ;; (Caffeine's eviction is async, so we check estimated size)
      (is (<= (.estimatedSize cache) 2)
          "Cache should evict to stay under capacity"))))
