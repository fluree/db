(ns fluree.db.tabular.file-io
  "Implements Iceberg FileIO backed by Fluree storage protocols.

   This allows Iceberg to read table metadata and data files using
   Fluree's existing storage infrastructure (S3, local file, etc.)
   without requiring Hadoop dependencies.

   When the store implements StatStore and RangeReadableStore, this uses
   efficient HEAD requests and byte-range reads with block caching.
   Otherwise, falls back to reading entire files into memory.

   Usage:
     (create-fluree-file-io store)
     ;; Returns a FileIO that can be used with StaticTableOperations"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.tabular.seekable-stream :as seekable]
            [fluree.db.util.log :as log])
  (:import [java.io ByteArrayOutputStream InputStream]
           [org.apache.iceberg.io FileIO InputFile OutputFile PositionOutputStream SeekableInputStream]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Path Parsing
;;; ---------------------------------------------------------------------------

(defn- parse-storage-path
  "Parse an Iceberg path into structured components.

   Iceberg provides paths like:
   - s3://bucket/path/to/file
   - s3a://bucket/path/to/file
   - file:///path/to/file
   - /path/to/file

   Returns a map with:
   - :original  - the original path as provided
   - :scheme    - \"s3\", \"file\", or nil
   - :bucket    - bucket name for S3, nil otherwise
   - :path      - path without scheme/bucket prefix"
  [^String path]
  (cond
    ;; S3 URL: s3://bucket/path or s3a://bucket/path
    (or (str/starts-with? path "s3://")
        (str/starts-with? path "s3a://"))
    (let [without-scheme (str/replace-first path #"^s3a?://" "")
          slash-idx (str/index-of without-scheme "/")]
      (if slash-idx
        {:original path
         :scheme   "s3"
         :bucket   (subs without-scheme 0 slash-idx)
         :path     (subs without-scheme (inc slash-idx))}
        {:original path
         :scheme   "s3"
         :bucket   without-scheme
         :path     ""}))

    ;; File URL: file:///path
    (str/starts-with? path "file://")
    {:original path
     :scheme   "file"
     :bucket   nil
     :path     (str/replace-first path #"^file://" "")}

    ;; Already a plain path
    :else
    {:original path
     :scheme   nil
     :bucket   nil
     :path     path}))

(defn- get-effective-path
  "Get the effective storage path for a store.

   For stores that implement FullURIStore (like VendedCredentialsStore), returns the original path.
   For single-bucket stores (like S3Store), returns just the key path."
  [store parsed-path]
  (if (and (satisfies? storage/FullURIStore store)
           (storage/expects-full-uri? store))
    ;; Store expects full URIs like s3://bucket/path
    (:original parsed-path)
    ;; Standard store expects just the key path (bucket configured at store level)
    (:path parsed-path)))

;;; ---------------------------------------------------------------------------
;;; SeekableInputStream Implementation
;;; ---------------------------------------------------------------------------

(defn- create-seekable-input-stream
  "Creates a SeekableInputStream backed by a byte array.
   Iceberg requires seekable streams for efficient Parquet reading."
  ^SeekableInputStream [^bytes data]
  (let [pos (atom 0)
        length (alength data)]
    (proxy [SeekableInputStream] []
      (getPos [] @pos)
      (seek [new-pos]
        (when (or (neg? new-pos) (> new-pos length))
          (throw (java.io.IOException. (str "Seek position out of bounds: " new-pos))))
        (reset! pos new-pos))
      (read
        ([]
         (if (>= @pos length)
           -1
           (let [b (aget data @pos)]
             (swap! pos inc)
             (bit-and b 0xff))))
        ([^bytes buf]
         (.read ^InputStream this buf 0 (alength buf)))
        ([^bytes buf off len]
         (if (>= @pos length)
           -1
           (let [available (- length @pos)
                 to-read (min len available)]
             (System/arraycopy data @pos buf off to-read)
             (swap! pos + to-read)
             to-read))))
      (skip [n]
        (let [available (- length @pos)
              to-skip (min n available)]
          (swap! pos + to-skip)
          to-skip))
      (available []
        (- length @pos))
      (close []
        ;; No resources to release for byte array
        nil))))

;;; ---------------------------------------------------------------------------
;;; PositionOutputStream Implementation
;;; ---------------------------------------------------------------------------

(defn- create-position-output-stream
  "Creates a PositionOutputStream that buffers writes.
   Returns [stream promise] where promise will contain the final bytes."
  []
  (let [baos (ByteArrayOutputStream.)
        result-promise (promise)]
    [(proxy [PositionOutputStream] []
       (getPos [] (.size baos))
       (write
         ([b]
          (if (instance? Integer b)
            (.write baos ^int b)
            (.write baos ^bytes b)))
         ([^bytes buf off len]
          (.write baos buf off len)))
       (flush [] (.flush baos))
       (close []
         (.close baos)
         (deliver result-promise (.toByteArray baos))))
     result-promise]))

;;; ---------------------------------------------------------------------------
;;; InputFile Implementation
;;; ---------------------------------------------------------------------------

(defn- supports-optimized-io?
  "Check if the store supports efficient stat and range reads."
  [store]
  (and (satisfies? storage/StatStore store)
       (satisfies? storage/RangeReadableStore store)))

(defn- create-input-file-optimized
  "Creates an InputFile using efficient stat and range reads with block caching.

   opts may include:
   - :cache-instance - Shared Caffeine cache for block caching
   - :block-size - Block size in bytes for range reads"
  [store ^String path storage-path opts]
  (let [;; Cache the stat result to avoid multiple HEAD requests
        stat-cache (atom nil)
        ;; Extract seekable stream options
        stream-opts (cond-> {}
                      (:cache-instance opts) (assoc :cache (:cache-instance opts))
                      (:block-size opts) (assoc :block-size (:block-size opts)))]
    (reify InputFile
      (location [_] path)

      (exists [_]
        (try
          (let [stat-result (async/<!! (storage/stat store storage-path))]
            (if (instance? Throwable stat-result)
              false
              (do
                (when stat-result (reset! stat-cache stat-result))
                (some? stat-result))))
          (catch Exception _
            false)))

      (getLength [_]
        (if-let [cached @stat-cache]
          (:size cached)
          (let [stat-result (async/<!! (storage/stat store storage-path))]
            (if (instance? Throwable stat-result)
              (throw stat-result)
              (if stat-result
                (do
                  (reset! stat-cache stat-result)
                  (:size stat-result))
                (throw (java.io.FileNotFoundException. path)))))))

      (newStream [this]
        (log/debug "FlureeFileIO: Opening stream (optimized)" path "as storage path:" storage-path)
        (let [size (.getLength this)]
          (seekable/create-seekable-input-stream store storage-path size stream-opts))))))

(defn- create-input-file-fallback
  "Creates an InputFile using full-file reads (fallback for stores without stat/range)."
  [store ^String path storage-path]
  (reify InputFile
    (location [_] path)

    (exists [_]
      (try
        (let [result (async/<!! (storage/read-bytes store storage-path))]
          (some? result))
        (catch Exception _
          false)))

    (getLength [_]
      (let [data (async/<!! (storage/read-bytes store storage-path))]
        (if data
          (alength ^bytes data)
          (throw (java.io.FileNotFoundException. path)))))

    (newStream [_]
      (log/debug "FlureeFileIO: Reading (fallback)" path "as storage path:" storage-path)
      (let [data (async/<!! (storage/read-bytes store storage-path))]
        (if data
          (create-seekable-input-stream data)
          (throw (java.io.FileNotFoundException. path)))))))

(defn- create-input-file
  "Creates an Iceberg InputFile backed by Fluree storage.
   Uses efficient stat/range reads if supported, otherwise falls back to full-file reads.

   opts may include:
   - :cache-instance - Shared Caffeine cache for block caching
   - :block-size - Block size in bytes for range reads"
  [store ^String path opts]
  (let [parsed-path  (parse-storage-path path)
        storage-path (get-effective-path store parsed-path)]
    (if (supports-optimized-io? store)
      (create-input-file-optimized store path storage-path opts)
      (create-input-file-fallback store path storage-path))))

;;; ---------------------------------------------------------------------------
;;; OutputFile Implementation
;;; ---------------------------------------------------------------------------

(defn- create-output-file
  "Creates an Iceberg OutputFile backed by Fluree storage."
  [store ^String path opts]
  (let [parsed-path  (parse-storage-path path)
        storage-path (get-effective-path store parsed-path)]
    (reify OutputFile
      (location [_] path)

      (create [_]
        (log/debug "FlureeFileIO: Creating" path "as storage path:" storage-path)
        (let [[stream result-promise] (create-position-output-stream)]
          ;; Return a wrapped stream that writes to store on close
          (proxy [PositionOutputStream] []
            (getPos [] (.getPos ^PositionOutputStream stream))
            (write
              ([b]
               (if (instance? Integer b)
                 (.write ^PositionOutputStream stream ^int b)
                 (.write ^PositionOutputStream stream ^bytes b)))
              ([^bytes buf off len] (.write ^PositionOutputStream stream buf (int off) (int len))))
            (flush [] (.flush ^PositionOutputStream stream))
            (close []
              (.close ^PositionOutputStream stream)
              (let [data @result-promise]
                (async/<!! (storage/write-bytes store storage-path data)))))))

      (createOrOverwrite [this]
        (.create this))

      (toInputFile [_]
        (create-input-file store path opts)))))

;;; ---------------------------------------------------------------------------
;;; FlureeFileIO - Main FileIO Implementation
;;; ---------------------------------------------------------------------------

(defn create-fluree-file-io
  "Creates an Iceberg FileIO backed by a Fluree storage store.

   The store must implement the ByteStore protocol (read-bytes, write-bytes).
   This includes FileStore, S3Store, and MemoryStore.

   opts may include:
   - :cache-instance - Shared Caffeine cache for block caching
   - :block-size - Block size in bytes for range reads

   Example:
     (def file-io (create-fluree-file-io my-s3-store {}))
     (def table-ops (StaticTableOperations. metadata-location file-io))
     (def table (BaseTable. table-ops table-id))"
  (^FileIO [store]
   (create-fluree-file-io store {}))
  (^FileIO [store opts]
   (reify FileIO
     (^InputFile newInputFile [_ ^String path]
       (create-input-file store path opts))

     (^OutputFile newOutputFile [_ ^String path]
       (create-output-file store path opts))

     (^void deleteFile [_ ^String path]
       (let [parsed-path  (parse-storage-path path)
             storage-path (get-effective-path store parsed-path)]
         (log/debug "FlureeFileIO: Deleting" path "as storage path:" storage-path)
         ;; Note: Not all stores support delete - this may be a no-op
         (when (satisfies? storage/EraseableStore store)
           (async/<!! (storage/delete store storage-path)))
         nil))

     (initialize [_ _properties]
       ;; No-op - store is already initialized
       nil)

     (properties [_]
       {})

     (close [_]
       ;; No-op - store lifecycle managed externally
       nil))))
