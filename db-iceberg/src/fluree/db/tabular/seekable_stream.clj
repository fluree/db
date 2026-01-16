(ns fluree.db.tabular.seekable-stream
  "Block-caching SeekableInputStream for efficient Iceberg file access.

   Iceberg requires SeekableInputStream for reading Parquet files, which
   involves many seek/read operations. This implementation fetches data
   in configurable blocks (default 4MB) and caches them using Caffeine LRU.

   Key features:
   - Fetches 4MB blocks on demand via RangeReadableStore protocol
   - Global LRU cache bounded by total bytes (default 256MB)
   - 5-minute TTL for cache entries
   - Thread-safe block fetching"
  (:require [clojure.core.async :as async]
            [fluree.db.storage :as storage])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine Weigher)
           (java.io InputStream)
           (java.time Duration)
           (org.apache.iceberg.io SeekableInputStream)))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

(def ^:private default-block-size
  "Default block size for range reads: 4MB.
   Balances S3 latency (~50-100ms per request) vs memory footprint."
  (* 4 1024 1024))

(def ^:private default-max-cache-bytes
  "Default maximum cache size: 256MB"
  (* 256 1024 1024))

(def ^:private default-expire-minutes
  "Default TTL for cache entries: 5 minutes"
  5)

;;; ---------------------------------------------------------------------------
;;; Block Cache
;;; ---------------------------------------------------------------------------

(defn create-cache
  "Create a Caffeine cache with specified settings.
   Returns a Cache instance.

   This function is typically called ONCE at publisher init time to create
   a shared cache instance that is reused across all VGs under that publisher.

   Options:
   - :max-bytes - Maximum cache size in bytes (default 256MB)
   - :ttl-minutes - Time-to-live in minutes (default 5)"
  ^Cache [{:keys [max-bytes ttl-minutes]
           :or {max-bytes default-max-cache-bytes
                ttl-minutes default-expire-minutes}}]
  (-> (Caffeine/newBuilder)
      (.maximumWeight (long max-bytes))
      (.weigher (reify Weigher
                  (weigh [_ _k v]
                    (alength ^bytes v))))
      (.expireAfterAccess (Duration/ofMinutes ttl-minutes))
      (.build)))

;; Global cache for file blocks, keyed by [path block-size block-index].
;; Bounded by total bytes with LRU eviction.
;; Used as fallback when no per-publisher cache is configured.
(defonce ^:private global-block-cache
  (delay (create-cache {})))

(defn get-block-cache
  "Get the global block cache instance.
   Used as fallback when no per-publisher cache is configured."
  ^Cache []
  @global-block-cache)

(defn- cache-key
  "Create a cache key for a block.
   Includes block-size to prevent cache corruption if different callers use different sizes."
  [path block-size block-index]
  [path block-size block-index])

(defn- fetch-block
  "Fetch a block from storage using range reads.
   Returns the byte array for the block."
  [store path block-index block-size file-size]
  (let [offset     (* block-index block-size)
        ;; Don't request beyond EOF
        remaining  (- file-size offset)
        length     (min block-size remaining)]
    (when (pos? length)
      (let [result (async/<!! (storage/read-bytes-range store path offset length))]
        (if (instance? Throwable result)
          (throw result)
          result)))))

(defn- get-or-fetch-block
  "Get a block from cache, fetching from storage if not present."
  [^Cache cache store path block-index block-size file-size]
  (let [key (cache-key path block-size block-index)]
    (or (.getIfPresent cache key)
        (when-let [block (fetch-block store path block-index block-size file-size)]
          (.put cache key block)
          block))))

;;; ---------------------------------------------------------------------------
;;; SeekableInputStream Implementation
;;; ---------------------------------------------------------------------------

(defn create-seekable-input-stream
  "Creates a SeekableInputStream backed by block-cached range reads.

   Parameters:
   - store: Storage implementing RangeReadableStore protocol
   - path: Path to the file in storage
   - file-size: Total size of the file in bytes
   - opts: Optional configuration map:
     - :block-size - Size of blocks to fetch (default 4MB)
     - :cache - Custom Cache instance (default: global cache)

   Returns a SeekableInputStream suitable for Iceberg/Parquet reading."
  ^SeekableInputStream [store path file-size {:keys [block-size cache]
                                              :or   {block-size default-block-size}}]
  (let [cache      (or cache (get-block-cache))
        pos        (atom 0)
        file-size  (long file-size)
        block-size (long block-size)]

    (proxy [SeekableInputStream] []
      (getPos []
        @pos)

      (seek [new-pos]
        (when (or (neg? new-pos) (> new-pos file-size))
          (throw (java.io.IOException. (str "Seek position out of bounds: " new-pos
                                            " (file size: " file-size ")"))))
        (reset! pos new-pos))

      (read
        ([]
         (if (>= @pos file-size)
           -1
           (let [current-pos   @pos
                 block-index   (quot current-pos block-size)
                 block-offset  (rem current-pos block-size)
                 block         (get-or-fetch-block cache store path block-index block-size file-size)]
             (if (and block (< block-offset (alength ^bytes block)))
               (let [b (aget ^bytes block block-offset)]
                 (swap! pos inc)
                 (bit-and b 0xff))
               -1))))

        ([^bytes buf]
         (.read ^InputStream this buf 0 (alength buf)))

        ([^bytes buf off len]
         (if (>= @pos file-size)
           -1
           (let [current-pos @pos
                 ;; Calculate how many bytes we can actually read
                 available   (- file-size current-pos)
                 to-read     (min len available)]
             (if (zero? to-read)
               -1
               ;; Read may span multiple blocks
               ;; Use explicit int types to avoid primitive boxing issues
               (let [bytes-read
                     (loop [bytes-read (int 0)
                            buf-offset (int off)]
                       (if (>= bytes-read to-read)
                         bytes-read
                         (let [current      (+ current-pos bytes-read)
                               block-index  (quot current block-size)
                               block-offset (rem current block-size)
                               block        (get-or-fetch-block cache store path block-index block-size file-size)]
                           (if-not block
                             bytes-read
                             (let [block-len       (alength ^bytes block)
                                   block-remaining (- block-len block-offset)
                                   copy-len        (int (min block-remaining (- to-read bytes-read)))]
                               (System/arraycopy block (int block-offset) buf buf-offset copy-len)
                               (recur (int (+ bytes-read copy-len))
                                      (int (+ buf-offset copy-len))))))))]
                 ;; Update position after successful read
                 (if (pos? bytes-read)
                   (do (swap! pos + bytes-read)
                       bytes-read)
                   -1)))))))

      (skip [n]
        (let [current   @pos
              available (- file-size current)
              to-skip   (min n available)]
          (swap! pos + to-skip)
          to-skip))

      (available []
        (- file-size @pos))

      (close []
        ;; No resources to release - cache is global
        nil))))

(defn invalidate-path
  "Invalidate all cached blocks for a given path.
   Use when a file is known to have been modified."
  [path]
  (let [^Cache cache (get-block-cache)]
    ;; Caffeine doesn't support prefix invalidation, so we iterate
    ;; This is O(n) but should be rare (only for version-hint.text)
    (doseq [key (.asMap cache)]
      (when (= path (first key))
        (.invalidate cache key)))))
