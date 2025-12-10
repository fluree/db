(ns fluree.db.tabular.iceberg.hadoop
  "Hadoop-based Iceberg source implementation.

   Uses HadoopTables for simple local filesystem access. Best for:
   - Local development and testing
   - Quick prototyping
   - Single-machine deployments

   For production with cloud storage, use FlureeIcebergSource instead."
  (:require [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.tabular.protocol :as proto]
            [fluree.db.util.log :as log])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.iceberg Table]
           [org.apache.iceberg.hadoop HadoopTables]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; IcebergSource Implementation (Hadoop-based)
;;; ---------------------------------------------------------------------------

(defrecord IcebergSource [^HadoopTables tables ^Configuration conf warehouse-path]
  proto/ITabularSource

  (scan-batches [_ table-name {:keys [columns predicates snapshot-id as-of-time batch-size limit]
                               :or {batch-size 4096}}]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)]
      (log/debug "IcebergSource scan-batches (Arrow):" {:table table-name
                                                         :batch-size batch-size
                                                         :columns (count columns)
                                                         :predicates (count predicates)})
      (core/scan-with-arrow table {:columns columns
                                   :predicates predicates
                                   :snapshot-id snapshot-id
                                   :as-of-time as-of-time
                                   :batch-size batch-size
                                   :limit limit})))

  (scan-rows [this table-name opts]
    ;; scan-batches now returns row maps directly
    (proto/scan-batches this table-name opts))

  (get-schema [_ table-name opts]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)]
      (core/extract-schema table opts)))

  (get-statistics [_ table-name opts]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)]
      (core/extract-statistics table opts)))

  (supported-predicates [_]
    core/supported-predicate-ops)

  proto/ICloseable
  (close [_]
    ;; Clean up Hadoop FileSystem resources
    (FileSystem/closeAll)))

;;; ---------------------------------------------------------------------------
;;; Factory Function
;;; ---------------------------------------------------------------------------

(defn create-iceberg-source
  "Create an IcebergSource for querying Iceberg tables via Hadoop.

   Config:
     :warehouse-path - Root path to Iceberg warehouse (required)

   Example:
     (create-iceberg-source {:warehouse-path \"/path/to/warehouse\"})

   The warehouse-path should contain table directories. Tables are loaded
   by path: warehouse-path + \"/\" + table-name

   Note: This uses HadoopTables which is simple but has no warehouse root
   concept. For production with many tables, consider using HadoopCatalog
   or REST/Glue catalogs instead."
  [{:keys [warehouse-path]}]
  {:pre [(string? warehouse-path)]}
  (let [conf (Configuration.)
        tables (HadoopTables. conf)]
    (->IcebergSource tables conf warehouse-path)))
