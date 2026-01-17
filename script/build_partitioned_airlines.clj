(ns build-partitioned-airlines
  "Builds a partitioned Iceberg table for partition pruning tests.

  Usage:
    make iceberg-partitioned

  Or manually:
    clojure -Sdeps '{:paths [\"script\"] :deps {...}}' -M -m build-partitioned-airlines

  This creates the airlines table partitioned by 'active' column (Y/N),
  which is ideal for testing partition pruning."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (java.io File)
           (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem)
           (org.apache.iceberg FileFormat PartitionSpec Schema PartitionKey)
           (org.apache.iceberg.hadoop HadoopTables)
           (org.apache.iceberg.types Types Types$NestedField
                                     Types$LongType Types$StringType)
           (org.apache.iceberg.data GenericRecord GenericAppenderFactory)
           (org.apache.iceberg.io OutputFileFactory)))

(set! *warn-on-reflection* true)

(def root-dir
  (-> (io/file (System/getProperty "user.dir") "dev-resources" "openflights")
      .getAbsolutePath))

(def raw-dir (str root-dir File/separator "raw"))
(def warehouse-dir (str root-dir File/separator "warehouse"))

(defn ensure-dir [path]
  (let [f (io/file path)]
    (.mkdirs f)
    f))

(defn fail [msg]
  (binding [*out* *err*]
    (println msg))
  (System/exit 1))

(defn expect-file [^String name]
  (let [f (io/file raw-dir name)]
    (when-not (.exists f)
      (fail (str "Missing file: " (.getAbsolutePath f)
                 "\nRun script/fetch-openflights.sh first.")))
    f))

(defn schema-airlines []
  (Schema.
   (into-array Types$NestedField
               [(Types$NestedField/required 1 "id" (Types$LongType/get))
                (Types$NestedField/optional 2 "name" (Types$StringType/get))
                (Types$NestedField/optional 3 "alias" (Types$StringType/get))
                (Types$NestedField/optional 4 "iata" (Types$StringType/get))
                (Types$NestedField/optional 5 "icao" (Types$StringType/get))
                (Types$NestedField/optional 6 "callsign" (Types$StringType/get))
                (Types$NestedField/optional 7 "country" (Types$StringType/get))
                (Types$NestedField/optional 8 "active" (Types$StringType/get))])))

(defn my-parse-long [s]
  (when (and s (not= s "\\N") (not= s ""))
    (Long/parseLong s)))

(defn load-csv [^File f]
  (with-open [r (io/reader f)]
    (doall (csv/read-csv r))))

(defn delete-dir-recursive [^File f]
  (when (.exists f)
    (when (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-dir-recursive child)))
    (.delete f)))

(defn create-table! [^HadoopTables tables ^String path ^Schema schema ^PartitionSpec spec]
  (let [table-dir (io/file path)]
    (when (.exists tables path)
      (println "  Table already exists at" path "- deleting and recreating")
      (delete-dir-recursive table-dir))
    (.create tables schema spec path)))

(defn write-partition!
  "Write a single partition file using GenericAppenderFactory."
  [^org.apache.iceberg.Table table ^Schema schema ^PartitionSpec spec
   ^OutputFileFactory out-factory ^GenericAppenderFactory appender-factory
   partition-value rows file-num]
  (let [;; Create partition key with the partition value
        ^PartitionKey partition-key (PartitionKey. spec schema)
        ;; Create a record just to extract partition value
        sample-record (GenericRecord/create schema)
        _ (.set sample-record 7 partition-value)  ;; active column is at index 7
        _ (.partition partition-key sample-record)

        ;; Create output file with partition
        output-file (.newOutputFile out-factory spec partition-key)

        ;; Create data writer
        data-writer (.newDataWriter appender-factory output-file FileFormat/PARQUET partition-key)]
    (try
      (doseq [row rows]
        (let [record (GenericRecord/create schema)]
          (doseq [[idx v] (map-indexed vector row)]
            (.set record idx v))
          (.write data-writer record)))
      (.close data-writer)
      (let [data-file (.toDataFile data-writer)]
        (println "  Written" (count rows) "rows for partition active=" partition-value
                 "- file size:" (.fileSizeInBytes data-file) "bytes")
        data-file)
      (catch Exception e
        (try (.close data-writer) (catch Exception _))
        (throw e)))))

(defn build-partitioned-airlines! [^HadoopTables tables]
  (let [schema (schema-airlines)
        ;; Create partition spec by 'active' column (field id 8)
        spec (-> (PartitionSpec/builderFor schema)
                 (.identity "active")
                 .build)
        table-path (str warehouse-dir "/openflights/airlines_partitioned")
        _ (ensure-dir (.getParent (io/file table-path)))
        ^org.apache.iceberg.Table table (create-table! tables table-path schema spec)
        ;; Reload table to get proper state
        table (.load tables table-path)
        all-rows (->> (load-csv (expect-file "airlines.dat"))
                      (map (fn [[id name alias iata icao callsign country active & _]]
                             [(my-parse-long id) name alias iata icao callsign country active])))
        ;; Group rows by partition value (active column, index 7)
        rows-by-partition (group-by #(nth % 7) all-rows)
        _ (println "  Partitions found:" (keys rows-by-partition))

        ;; Create factories
        appender-factory (GenericAppenderFactory. schema spec)
        file-factory (OutputFileFactory/builderFor table 1 1)
        out-factory (.build file-factory)

        ;; Write each partition and collect data files
        data-files (doall
                    (for [[idx [partition-value rows]] (map-indexed vector (seq rows-by-partition))]
                      (write-partition! table schema spec out-factory appender-factory
                                        partition-value rows idx)))]

    ;; Commit all data files in a single append operation
    (let [append (.newAppend table)]
      (doseq [df data-files]
        (.appendFile append ^org.apache.iceberg.DataFile df))
      (.commit append))
    (println "  Committed" (count data-files) "partition files")))

(defn -main [& _args]
  (ensure-dir raw-dir)
  (ensure-dir warehouse-dir)
  (let [conf (Configuration.)
        tables (HadoopTables. conf)]
    (try
      (println "Building partitioned Iceberg table at" warehouse-dir)
      (build-partitioned-airlines! tables)
      (println "Done.")
      (finally
        (FileSystem/closeAll)))))
