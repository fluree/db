(ns ^:iceberg fluree.db.tabular.file-io-cache-test
  "Tests for FileIO stat caching and range read behavior.

   Validates:
   - Stat results are cached (no repeated HEAD requests)
   - Range reads are used instead of full file downloads
   - Shared cache instance is passed through correctly
   - Backward compatibility with no-opts call"
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.storage :as storage]
            [fluree.db.tabular.file-io :as file-io]
            [fluree.db.tabular.seekable-stream :as seekable])
  (:import [org.apache.iceberg.io FileIO InputFile]))

;;; ---------------------------------------------------------------------------
;;; Mock Store with Counters
;;; ---------------------------------------------------------------------------

(defrecord CountingStore [data stat-calls range-calls full-reads]
  storage/StatStore
  (stat [_ path]
    (async/go
      (swap! stat-calls inc)
      {:size (alength ^bytes @data)
       :path path}))

  storage/RangeReadableStore
  (read-bytes-range [_ _path offset length]
    (async/go
      (swap! range-calls inc)
      (let [data-bytes ^bytes @data
            end (min (+ offset length) (alength data-bytes))
            actual-len (- end offset)]
        (when (pos? actual-len)
          (let [result (byte-array actual-len)]
            (System/arraycopy data-bytes offset result 0 actual-len)
            result)))))

  storage/ByteStore
  (read-bytes [_ _path]
    (async/go
      (swap! full-reads inc)
      @data))
  (write-bytes [_ _path _bytes]
    (throw (ex-info "CountingStore is read-only" {})))
  (swap-bytes [_ _path _f]
    (throw (ex-info "CountingStore is read-only" {}))))

(defn- create-counting-store
  "Create a store with counters for stat, range, and full reads.
   Returns [store {:stat-calls :range-calls :full-reads}]."
  [size]
  (let [data (byte-array (for [i (range size)] (unchecked-byte (mod i 256))))
        stat-calls (atom 0)
        range-calls (atom 0)
        full-reads (atom 0)]
    [(->CountingStore (atom data) stat-calls range-calls full-reads)
     {:stat-calls stat-calls
      :range-calls range-calls
      :full-reads full-reads}]))

;;; ---------------------------------------------------------------------------
;;; Simple Store (no stat/range support - fallback path)
;;; ---------------------------------------------------------------------------

(defrecord SimpleStore [data full-reads]
  storage/ByteStore
  (read-bytes [_ _path]
    (async/go
      (swap! full-reads inc)
      @data))
  (write-bytes [_ _path _bytes]
    (throw (ex-info "SimpleStore is read-only" {})))
  (swap-bytes [_ _path _f]
    (throw (ex-info "SimpleStore is read-only" {}))))

(defn- create-simple-store
  "Create a store without stat/range support (fallback path)."
  [size]
  (let [data (byte-array (for [i (range size)] (unchecked-byte (mod i 256))))
        full-reads (atom 0)]
    [(->SimpleStore (atom data) full-reads)
     {:full-reads full-reads}]))

;;; ---------------------------------------------------------------------------
;;; FileIO Stat Caching Tests
;;; ---------------------------------------------------------------------------

(deftest stat-caching-test
  (testing "getLength caches stat result"
    (let [file-size 10000
          [store counters] (create-counting-store file-size)
          file-io (file-io/create-fluree-file-io store {})
          input-file (.newInputFile file-io "test/file.parquet")]

      ;; First call to getLength
      (let [len1 (.getLength input-file)]
        (is (= file-size len1))
        (is (= 1 @(:stat-calls counters))
            "First getLength should call stat"))

      ;; Second call should use cache
      (let [len2 (.getLength input-file)]
        (is (= file-size len2))
        (is (= 1 @(:stat-calls counters))
            "Second getLength should NOT call stat (cached)"))

      ;; Third call still cached
      (.getLength input-file)
      (is (= 1 @(:stat-calls counters))
          "Stat should only be called once per InputFile"))))

(deftest exists-caches-stat-test
  (testing "exists() result caches stat"
    (let [file-size 1000
          [store counters] (create-counting-store file-size)
          file-io (file-io/create-fluree-file-io store {})
          input-file (.newInputFile file-io "test/file.parquet")]

      ;; exists() should stat the file
      (is (true? (.exists input-file)))
      (is (= 1 @(:stat-calls counters)))

      ;; Subsequent getLength should reuse cached stat
      (is (= file-size (.getLength input-file)))
      (is (= 1 @(:stat-calls counters))
          "getLength after exists should use cached stat"))))

;;; ---------------------------------------------------------------------------
;;; Range Read Tests (No Full File Downloads)
;;; ---------------------------------------------------------------------------

(deftest uses-range-reads-test
  (testing "newStream uses range reads, not full file downloads"
    (let [file-size 100000  ;; 100KB file
          block-size 4096   ;; 4KB blocks
          [store counters] (create-counting-store file-size)
          cache (seekable/create-cache {:max-bytes (* 1024 1024)})
          file-io (file-io/create-fluree-file-io store {:cache-instance cache
                                                        :block-size block-size})
          input-file (.newInputFile file-io "test/large.parquet")]

      ;; Open stream and read some data
      (with-open [stream (.newStream input-file)]
        (let [buf (byte-array 100)]
          (.read stream buf)))

      ;; Should use range reads, NOT full reads
      (is (= 0 @(:full-reads counters))
          "Should NOT download full file")
      (is (pos? @(:range-calls counters))
          "Should use range reads"))))

(deftest footer-read-pattern-test
  (testing "Parquet footer read pattern uses range reads"
    ;; Parquet files are typically read footer-first (seek to end, read footer)
    (let [file-size 100000
          block-size 4096
          [store counters] (create-counting-store file-size)
          cache (seekable/create-cache {:max-bytes (* 1024 1024)})
          file-io (file-io/create-fluree-file-io store {:cache-instance cache
                                                        :block-size block-size})
          input-file (.newInputFile file-io "test/data.parquet")]

      (with-open [stream (.newStream input-file)]
        ;; Simulate Parquet footer read: seek to near end, read
        (.seek stream (- file-size 1000))
        (let [buf (byte-array 1000)]
          (.read stream buf)))

      ;; Should be range reads, not full file
      (is (= 0 @(:full-reads counters)))
      (is (pos? @(:range-calls counters)))))

  (testing "Multiple seeks use cached blocks"
    (let [file-size 100000
          block-size 4096
          [store counters] (create-counting-store file-size)
          cache (seekable/create-cache {:max-bytes (* 1024 1024)})
          file-io (file-io/create-fluree-file-io store {:cache-instance cache
                                                        :block-size block-size})
          input-file (.newInputFile file-io "test/data.parquet")]

      (with-open [stream (.newStream input-file)]
        (let [buf (byte-array 100)]
          ;; Read from beginning
          (.read stream buf)
          (let [calls-after-first @(:range-calls counters)]

            ;; Seek somewhere else
            (.seek stream 50000)
            (.read stream buf)

            ;; Seek back to beginning (should be cached)
            (.seek stream 0)
            (.read stream buf)

            ;; The third read should use cache, so range calls shouldn't increase much
            ;; (Only the middle read should add calls)
            (is (< (- @(:range-calls counters) calls-after-first) 3)
                "Re-reading cached blocks should not cause new range calls")))))))

;;; ---------------------------------------------------------------------------
;;; Shared Cache Tests
;;; ---------------------------------------------------------------------------

(deftest shared-cache-across-files-test
  (testing "Cache instance is shared across InputFiles"
    (let [file-size 10000
          block-size 4096
          [store counters] (create-counting-store file-size)
          cache (seekable/create-cache {:max-bytes (* 1024 1024)})
          file-io (file-io/create-fluree-file-io store {:cache-instance cache
                                                        :block-size block-size})]

      ;; Create two InputFiles for the same path
      (let [input1 (.newInputFile file-io "test/shared.parquet")
            input2 (.newInputFile file-io "test/shared.parquet")]

        ;; Read from first
        (with-open [stream1 (.newStream input1)]
          (.read stream1 (byte-array 100)))

        (let [calls-after-first @(:range-calls counters)]
          ;; Read same data from second InputFile (same path)
          (with-open [stream2 (.newStream input2)]
            (.read stream2 (byte-array 100)))

          ;; Should hit cache - no new range calls
          (is (= calls-after-first @(:range-calls counters))
              "Second InputFile should hit shared cache"))))))

;;; ---------------------------------------------------------------------------
;;; Fallback Path Tests
;;; ---------------------------------------------------------------------------

(deftest fallback-full-read-test
  (testing "Falls back to full read when store lacks stat/range"
    (let [file-size 1000
          [store counters] (create-simple-store file-size)
          file-io (file-io/create-fluree-file-io store {})
          input-file (.newInputFile file-io "test/simple.dat")]

      ;; newStream should fall back to full read
      (with-open [stream (.newStream input-file)]
        (.read stream (byte-array 100)))

      (is (pos? @(:full-reads counters))
          "Should use full reads when range reads not supported"))))

;;; ---------------------------------------------------------------------------
;;; Backward Compatibility Tests
;;; ---------------------------------------------------------------------------

(deftest backward-compat-no-opts-test
  (testing "create-fluree-file-io works without options (backward compat)"
    (let [file-size 1000
          [store counters] (create-counting-store file-size)
          ;; Call with single arg (no opts) - this is the old API
          file-io (file-io/create-fluree-file-io store)
          input-file (.newInputFile file-io "test/compat.dat")]

      ;; Should still work
      (is (= file-size (.getLength input-file)))
      (is (true? (.exists input-file)))

      ;; And stream should work
      (with-open [stream (.newStream input-file)]
        (is (pos? (.read stream (byte-array 10))))))))

(deftest options-passed-correctly-test
  (testing "Block size option is respected"
    (let [file-size 100000
          small-block 1024  ;; 1KB blocks
          [store counters] (create-counting-store file-size)
          cache (seekable/create-cache {:max-bytes (* 1024 1024)})
          file-io (file-io/create-fluree-file-io store {:cache-instance cache
                                                        :block-size small-block})
          input-file (.newInputFile file-io "test/blocks.dat")]

      (with-open [stream (.newStream input-file)]
        ;; Read 5KB - should need 5 blocks with 1KB block size
        (.read stream (byte-array 5000)))

      ;; With 1KB blocks, reading 5KB should require ~5 range calls
      (is (>= @(:range-calls counters) 5)
          "Small block size should result in more range calls"))))
