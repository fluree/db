(ns fluree.db.tabular.file-io
  "Implements Iceberg FileIO backed by Fluree storage protocols.

   This allows Iceberg to read table metadata and data files using
   Fluree's existing storage infrastructure (S3, local file, etc.)
   without requiring Hadoop dependencies.

   Usage:
     (create-fluree-file-io store)
     ;; Returns a FileIO that can be used with StaticTableOperations"
  (:require [clojure.core.async :as async]
            [fluree.db.storage :as storage]
            [fluree.db.util.log :as log])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStream OutputStream]
           [org.apache.iceberg.io FileIO InputFile OutputFile PositionOutputStream SeekableInputStream]))

(set! *warn-on-reflection* true)

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
  (reify InputFile
    (location [_] path)

    (exists [_]
      ;; Try to read - if nil or exception, doesn't exist
      (try
        (let [result (async/<!! (storage/read-bytes store path))]
          (some? result))
        (catch Exception _
          false)))

    (getLength [this]
      ;; Read and get length - cached on first access would be better
      (let [data (async/<!! (storage/read-bytes store path))]
        (if data
          (alength ^bytes data)
          (throw (java.io.FileNotFoundException. path)))))

    (newStream [_]
      (log/debug "FlureeFileIO: Reading" path)
      (let [data (async/<!! (storage/read-bytes store path))]
        (if data
          (create-seekable-input-stream data)
          (throw (java.io.FileNotFoundException. path)))))))

;;; ---------------------------------------------------------------------------
;;; OutputFile Implementation
;;; ---------------------------------------------------------------------------

(defn- create-output-file
  "Creates an Iceberg OutputFile backed by Fluree storage."
  [store ^String path]
  (reify OutputFile
    (location [_] path)

    (create [_]
      (log/debug "FlureeFileIO: Creating" path)
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
              (async/<!! (storage/write-bytes store path data)))))))

    (createOrOverwrite [this]
      (.create this))

    (toInputFile [_]
      (create-input-file store path))))

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
      (log/debug "FlureeFileIO: Deleting" path)
      ;; Note: Not all stores support delete - this may be a no-op
      (when (satisfies? storage/EraseableStore store)
        (async/<!! (storage/delete store path)))
      nil)

    (initialize [_ _properties]
      ;; No-op - store is already initialized
      nil)

    (properties [_]
      {})

    (close [_]
      ;; No-op - store lifecycle managed externally
      nil)))
