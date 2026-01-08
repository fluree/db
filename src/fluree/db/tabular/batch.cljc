(ns fluree.db.tabular.batch
  "Batch abstraction for tabular data.

   Provides a unified interface for processing tabular data in batches,
   supporting both row-oriented (maps) and columnar (Arrow) representations.

   Phase 2 will add Arrow vectorized support via iceberg-arrow module.")

(defprotocol IBatch
  "Batch abstraction for tabular data.

   Implementations may wrap:
   - Arrow VectorSchemaRoot (Phase 2 vectorized reads)
   - Seq of row maps (Phase 1 compatibility with IcebergGenerics)
   - Direct Parquet row groups"

  (row-count [this]
    "Returns number of rows in this batch.")

  (column-names [this]
    "Returns seq of column names in this batch.")

  (column [this name]
    "Returns column values as a seq or Arrow vector.
     For row-based batches, returns (map #(get % name) rows).
     For Arrow batches, returns the underlying vector.")

  (select-columns [this names]
    "Returns new batch with only the specified columns.
     For Arrow batches, creates a new VectorSchemaRoot subset.
     For row batches, selects keys from each row.")

  (slice [this start end]
    "Returns batch containing rows [start, end).
     For Arrow batches, creates a view/slice.
     For row batches, uses subvec/take/drop.")

  (to-row-seq [this]
    "Convert batch to lazy seq of row maps.
     For Arrow batches, materializes each row.
     For row batches, returns the underlying seq."))

;;; ---------------------------------------------------------------------------
;;; Row-based Batch (Phase 1 - IcebergGenerics compatibility)
;;; ---------------------------------------------------------------------------

(defrecord RowBatch [rows]
  IBatch

  (row-count [_]
    (count rows))

  (column-names [_]
    (when-let [first-row (first rows)]
      (keys first-row)))

  (column [_ name]
    (map #(get % name) rows))

  (select-columns [_ names]
    (let [name-set (set names)]
      (->RowBatch
       (map #(select-keys % name-set) rows))))

  (slice [_ start end]
    (->RowBatch
     (take (- end start) (drop start rows))))

  (to-row-seq [_]
    rows))

(defn wrap-rows
  "Wrap a seq of row maps in an IBatch.
   Use for IcebergGenerics row-oriented results."
  [rows]
  (->RowBatch (vec rows)))

(defn batch-seq->rows
  "Convert a seq of IBatches to a lazy seq of row maps.
   Useful for flattening batch results to simple row iteration."
  [batches]
  (mapcat to-row-seq batches))

;;; ---------------------------------------------------------------------------
;;; Arrow Batch Placeholder (Phase 2)
;;; ---------------------------------------------------------------------------

;; Phase 2 will add:
;; (defrecord ArrowBatch [^VectorSchemaRoot root]
;;   IBatch
;;   ...)
;;
;; (defn wrap-arrow-batch [^VectorSchemaRoot root]
;;   (->ArrowBatch root))
;;
;; This requires iceberg-arrow and arrow-vector dependencies.
