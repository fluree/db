(ns fluree.db.tabular.file-io
  "Implements Iceberg FileIO backed by Fluree storage protocols.

   This allows Iceberg to read table metadata and data files using
   Fluree's existing storage infrastructure (S3, local file, etc.)
   without requiring Hadoop dependencies.

   Usage:
     (create-fluree-file-io store)
     ;; Returns a FileIO that can be used with StaticTableOperations"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.util.log :as log])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStream OutputStream]
           [org.apache.iceberg.io FileIO InputFile OutputFile PositionOutputStream SeekableInputStream]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Path Parsing
;;; ---------------------------------------------------------------------------

(defn- parse-storage-path
  "Parse an Iceberg path (which may be an S3 URL) into a path suitable for Fluree storage.

   Iceberg provides paths like:
   - s3://bucket/path/to/file
   - s3a://bucket/path/to/file
   - file:///path/to/file
   - /path/to/file

   For S3 URLs, strips the s3://bucket/ prefix since the S3Store already knows the bucket.
   For file URLs, strips the file:// prefix.
   For other paths, returns as-is."
  [^String path]
  (cond
    ;; S3 URL: s3://bucket/path or s3a://bucket/path
    (or (str/starts-with? path "s3://")
        (str/starts-with? path "s3a://"))
    (let [without-scheme (str/replace-first path #"^s3a?://" "")
          ;; Skip bucket name (everything before first /)
          slash-idx (str/index-of without-scheme "/")]
      (if slash-idx
        (subs without-scheme (inc slash-idx))
        ""))

    ;; File URL: file:///path
    (str/starts-with? path "file://")
    (str/replace-first path #"^file://" "")

    ;; Already a plain path
    :else path))

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

(defn- create-input-file
  "Creates an Iceberg InputFile backed by Fluree storage."
  [store ^String path]
  (let [storage-path (parse-storage-path path)]
    (reify InputFile
      (location [_] path)

      (exists [_]
        ;; Try to read - if nil or exception, doesn't exist
        (try
          (let [result (async/<!! (storage/read-bytes store storage-path))]
            (some? result))
          (catch Exception _
            false)))

      (getLength [this]
        ;; Read and get length - cached on first access would be better
        (let [data (async/<!! (storage/read-bytes store storage-path))]
          (if data
            (alength ^bytes data)
            (throw (java.io.FileNotFoundException. path)))))

      (newStream [_]
        (log/debug "FlureeFileIO: Reading" path "as storage path:" storage-path)
        (let [data (async/<!! (storage/read-bytes store storage-path))]
          (if data
            (create-seekable-input-stream data)
            (throw (java.io.FileNotFoundException. path))))))))

;;; ---------------------------------------------------------------------------
;;; OutputFile Implementation
;;; ---------------------------------------------------------------------------

(defn- create-output-file
  "Creates an Iceberg OutputFile backed by Fluree storage."
  [store ^String path]
  (let [storage-path (parse-storage-path path)]
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
        (create-input-file store path)))))

;;; ---------------------------------------------------------------------------
;;; FlureeFileIO - Main FileIO Implementation
;;; ---------------------------------------------------------------------------

(defn create-fluree-file-io
  "Creates an Iceberg FileIO backed by a Fluree storage store.

   The store must implement the ByteStore protocol (read-bytes, write-bytes).
   This includes FileStore, S3Store, and MemoryStore.

   Example:
     (def file-io (create-fluree-file-io my-s3-store))
     (def table-ops (StaticTableOperations. metadata-location file-io))
     (def table (BaseTable. table-ops table-id))"
  ^FileIO [store]
  (reify FileIO
    (^InputFile newInputFile [_ ^String path]
      (create-input-file store path))

    (^OutputFile newOutputFile [_ ^String path]
      (create-output-file store path))

    (^void deleteFile [_ ^String path]
      (let [storage-path (parse-storage-path path)]
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
      nil)))
