(ns fluree.db.iceberg-smoke-test
  "Minimal smoke test for Iceberg connectivity.
   Run with: clojure -M:dev:iceberg -m fluree.db.iceberg-smoke-test

   Best practices demonstrated:
   - Unpartitioned table (avoids partition key complexity)
   - GenericAppenderFactory with newDataWriter (provides toDataFile)
   - OutputFileFactory for proper file locations
   - Proper resource cleanup (catalog, writers)
   - IcebergGenerics for row-oriented reads (fine for smoke tests)"
  (:import [java.io File Closeable]
           [org.apache.hadoop.conf Configuration]
           [org.apache.iceberg Schema PartitionSpec FileFormat]
           [org.apache.iceberg.catalog TableIdentifier]
           [org.apache.iceberg.data GenericRecord GenericAppenderFactory IcebergGenerics]
           [org.apache.iceberg.expressions Expressions]
           [org.apache.iceberg.hadoop HadoopCatalog]
           [org.apache.iceberg.io OutputFileFactory]
           [org.apache.iceberg.types Types$NestedField
            Types$LongType Types$StringType Types$DoubleType]))

(defn create-test-schema
  "Create a simple schema: id, name, region, amount"
  []
  (Schema.
   (into-array Types$NestedField
               [(Types$NestedField/required 1 "id" (Types$LongType/get))
                (Types$NestedField/required 2 "name" (Types$StringType/get))
                (Types$NestedField/required 3 "region" (Types$StringType/get))
                (Types$NestedField/required 4 "amount" (Types$DoubleType/get))])))

(defn create-local-catalog
  "Create a Hadoop catalog pointing to local filesystem.
   Returns HadoopCatalog which is Closeable - caller must close."
  ^HadoopCatalog [^String warehouse-path]
  (let [conf (Configuration.)]
    (HadoopCatalog. conf warehouse-path)))

(defn create-test-table
  "Create an unpartitioned Iceberg table with test schema.
   Unpartitioned simplifies the smoke test - no partition key handling needed."
  [catalog table-name]
  (let [schema   (create-test-schema)
        table-id (TableIdentifier/parse table-name)
        ;; Unpartitioned for simplicity
        spec     (PartitionSpec/unpartitioned)]
    (if (.tableExists catalog table-id)
      (do
        (println "Table already exists, loading...")
        (.loadTable catalog table-id))
      (do
        (println "Creating new table...")
        (.createTable catalog table-id schema spec)))))

(defn create-test-records
  "Create sample GenericRecord objects"
  [schema]
  [(doto (GenericRecord/create schema)
     (.setField "id" (long 1))
     (.setField "name" "Alice")
     (.setField "region" "US")
     (.setField "amount" 1000.0))
   (doto (GenericRecord/create schema)
     (.setField "id" (long 2))
     (.setField "name" "Bob")
     (.setField "region" "EU")
     (.setField "amount" 2500.5))
   (doto (GenericRecord/create schema)
     (.setField "id" (long 3))
     (.setField "name" "Charlie")
     (.setField "region" "US")
     (.setField "amount" 750.25))
   (doto (GenericRecord/create schema)
     (.setField "id" (long 4))
     (.setField "name" "Diana")
     (.setField "region" "APAC")
     (.setField "amount" 3200.0))])

(defn write-test-data
  "Write sample records using GenericAppenderFactory (Iceberg 1.10+ best practice).

   Key patterns:
   - GenericAppenderFactory(schema, spec) for creating writers
   - newDataWriter() provides toDataFile() method for proper DataFile creation
   - OutputFileFactory for catalog-consistent file locations
   - try/finally for resource cleanup"
  [table]
  (let [schema   (.schema table)
        spec     (.spec table)
        records  (create-test-records schema)
        ;; Create appender factory for GenericRecord
        appender-factory (GenericAppenderFactory. schema spec)
        ;; Use OutputFileFactory for proper file locations
        out-factory      (-> (OutputFileFactory/builderFor table 1 1)
                             (.build))
        ;; Create output file - nil partition data for unpartitioned tables
        output-file      (.newOutputFile out-factory spec nil)
        ;; newDataWriter gives us toDataFile() method
        data-writer      (.newDataWriter appender-factory output-file FileFormat/PARQUET nil)]
    (println "Writing" (count records) "records...")
    (try
      ;; Write all records
      (doseq [record records]
        (.write data-writer record))
      (.close data-writer)

      ;; Commit the data file to the table
      (let [data-file (.toDataFile data-writer)]
        (-> (.newAppend table)
            (.appendFile data-file)
            (.commit))
        (println "Write complete. Committed" (count records) "records."))

      (catch Exception e
        (try (.close data-writer) (catch Exception _))
        (throw e)))))

(defn read-all-records
  "Read all records from table using IcebergGenerics.
   IcebergGenerics is row-oriented and single-threaded - fine for smoke tests.
   For production, consider Arrow vectorized readers."
  [table]
  (println "\n=== Reading all records ===")
  (let [scan    (-> (IcebergGenerics/read table)
                    (.build))
        records (vec (iterator-seq (.iterator scan)))]
    (doseq [record records]
      (println "  " record))
    (println "Total records:" (count records))
    (count records)))

(defn read-with-filter
  "Read with predicate pushdown - Iceberg pushes filter to Parquet"
  [table]
  (println "\n=== Reading with filter (region = 'US') ===")
  (let [expr    (Expressions/equal "region" "US")
        scan    (-> (IcebergGenerics/read table)
                    (.where expr)
                    (.build))
        records (vec (iterator-seq (.iterator scan)))]
    (doseq [record records]
      (println "  " record))
    (println "Filtered records:" (count records))
    (count records)))

(defn read-with-projection
  "Read with column projection - only requested columns are read from Parquet"
  [table]
  (println "\n=== Reading with projection (name, amount only) ===")
  (let [;; .select takes a Collection of column names
        scan    (-> (IcebergGenerics/read table)
                    (.select ["name" "amount"])
                    (.build))
        records (vec (iterator-seq (.iterator scan)))]
    (doseq [record records]
      (println "  " record))
    (count records)))

(defn show-table-metadata
  "Display table metadata including snapshot summary"
  [table]
  (println "\n=== Table Metadata ===")
  (println "Location:" (.location table))
  (println "Schema:" (.schema table))
  (println "Partition spec:" (.spec table))
  (when-let [snapshot (.currentSnapshot table)]
    (println "Snapshot ID:" (.snapshotId snapshot))
    (println "Timestamp:" (java.time.Instant/ofEpochMilli (.timestampMillis snapshot)))
    (println "Summary:" (.summary snapshot))))

(defn -main
  "Run Iceberg smoke test with proper resource cleanup"
  [& _args]
  (println "=== Iceberg Smoke Test (v1.10 best practices) ===\n")

  (let [warehouse-path (str (System/getProperty "user.dir") "/target/iceberg-warehouse")
        table-name     "test.sales"]

    ;; Ensure warehouse directory exists
    (.mkdirs (File. warehouse-path))

    (println "Warehouse:" warehouse-path)
    (println "Table:" table-name)

    ;; Use with-open for proper catalog cleanup (HadoopCatalog is Closeable)
    (with-open [^Closeable catalog (create-local-catalog warehouse-path)]
      (let [table (create-test-table catalog table-name)]

        ;; Write data only if table is empty
        (when (nil? (.currentSnapshot table))
          (write-test-data table))

        ;; Reload table to see new snapshot
        (let [table (.loadTable catalog (TableIdentifier/parse table-name))]
          ;; Show metadata
          (show-table-metadata table)

          ;; Read tests
          (let [cnt (read-all-records table)]
            (when (zero? cnt)
              (println "\nNo records found. Writing test data...")
              (write-test-data table)
              (let [table (.loadTable catalog (TableIdentifier/parse table-name))]
                (read-all-records table))))

          (read-with-filter table)
          (read-with-projection table))))

    (println "\n=== Smoke Test Complete ===")
    (println "Iceberg files at:" (str warehouse-path "/test/sales"))))
