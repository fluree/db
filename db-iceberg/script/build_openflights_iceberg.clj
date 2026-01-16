(ns build-openflights-iceberg
  "Builds a small local Iceberg warehouse from OpenFlights CSVs.

  Usage:
    cd db-iceberg && clojure -Sdeps '{:paths [\"script\"] :deps {org.apache.iceberg/iceberg-core {:mvn/version \"1.10.0\"} org.apache.iceberg/iceberg-parquet {:mvn/version \"1.10.0\"} org.apache.iceberg/iceberg-data {:mvn/version \"1.10.0\"} org.apache.iceberg/iceberg-bundled-guava {:mvn/version \"1.10.0\"} org.apache.parquet/parquet-hadoop {:mvn/version \"1.16.0\"} org.apache.hadoop/hadoop-common {:mvn/version \"3.3.6\" :exclusions [org.slf4j/slf4j-log4j12 log4j/log4j org.slf4j/slf4j-reload4j]} org.clojure/data.csv {:mvn/version \"1.0.1\"}}}' -M -m build-openflights-iceberg
  "
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (java.io File)
           (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.fs FileSystem)
           (org.apache.iceberg FileFormat PartitionSpec Schema)
           (org.apache.iceberg.hadoop HadoopTables)
           (org.apache.iceberg.types Types Types$NestedField
                                     Types$LongType Types$StringType
                                     Types$DoubleType Types$IntegerType)
           (org.apache.iceberg.data GenericRecord GenericAppenderFactory)
           (org.apache.iceberg.io OutputFileFactory)))

(set! *warn-on-reflection* true)

;; Output to test-resources for test fixtures
(def root-dir
  (-> (io/file (System/getProperty "user.dir") "test-resources" "openflights")
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
                 "\nDownload OpenFlights data first.")))
    f))

(defn schema-airports []
  (Schema.
   (into-array Types$NestedField
               [(Types$NestedField/required 1 "id" (Types$LongType/get))
                (Types$NestedField/optional 2 "name" (Types$StringType/get))
                (Types$NestedField/optional 3 "city" (Types$StringType/get))
                (Types$NestedField/optional 4 "country" (Types$StringType/get))
                (Types$NestedField/optional 5 "iata" (Types$StringType/get))
                (Types$NestedField/optional 6 "icao" (Types$StringType/get))
                (Types$NestedField/optional 7 "lat" (Types$DoubleType/get))
                (Types$NestedField/optional 8 "lon" (Types$DoubleType/get))
                (Types$NestedField/optional 9 "altitude" (Types$IntegerType/get))
                (Types$NestedField/optional 10 "tz_offset" (Types$DoubleType/get))
                (Types$NestedField/optional 11 "dst" (Types$StringType/get))
                (Types$NestedField/optional 12 "tz" (Types$StringType/get))])))

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

(defn schema-routes []
  (Schema.
   (into-array Types$NestedField
               [(Types$NestedField/optional 1 "airline" (Types$StringType/get))
                (Types$NestedField/optional 2 "airline_id" (Types$LongType/get))
                (Types$NestedField/optional 3 "src" (Types$StringType/get))
                (Types$NestedField/optional 4 "src_id" (Types$LongType/get))
                (Types$NestedField/optional 5 "dst" (Types$StringType/get))
                (Types$NestedField/optional 6 "dst_id" (Types$LongType/get))
                (Types$NestedField/optional 7 "codeshare" (Types$StringType/get))
                (Types$NestedField/optional 8 "stops" (Types$IntegerType/get))
                (Types$NestedField/optional 9 "equipment" (Types$StringType/get))])))

(defn parse-long [s]
  (when (and s (not= s "\\N") (not= s ""))
    (Long/parseLong s)))

(defn parse-int [s]
  (when (and s (not= s "\\N") (not= s ""))
    (Integer/parseInt s)))

(defn parse-double [s]
  (when (and s (not= s "\\N") (not= s ""))
    (Double/parseDouble s)))

(defn load-csv [^File f]
  (with-open [r (io/reader f)]
    (doall (csv/read-csv r))))

(defn record-writer
  "Writes rows to an Iceberg table using GenericAppenderFactory."
  [^Schema schema ^PartitionSpec spec ^HadoopTables tables ^String table-path rows]
  (let [table         (.load tables table-path)
        row-vec       (vec rows)
        appender-factory (GenericAppenderFactory. schema spec)
        file-factory  (OutputFileFactory/builderFor table 1 1)
        out-factory   (.build file-factory)
        output-file   (.newOutputFile out-factory spec nil)
        data-writer   (.newDataWriter appender-factory output-file FileFormat/PARQUET nil)]
    (try
      (doseq [row row-vec]
        (let [record (GenericRecord/create schema)]
          (doseq [[idx v] (map-indexed vector row)]
            (.set record idx v))
          (.write data-writer record)))
      (.close data-writer)
      (let [data-file (.toDataFile data-writer)]
        (-> (.newAppend table)
            (.appendFile data-file)
            (.commit)))
      (println "  Wrote" (count row-vec) "records to" table-path)
      (catch Exception e
        (try (.close data-writer) (catch Exception _))
        (throw e)))))

(defn create-table! [^HadoopTables tables ^String path ^Schema schema ^PartitionSpec spec]
  (if (.exists tables path)
    (.load tables path)
    (.create tables schema spec path)))

(defn build-airports! [^HadoopTables tables]
  (let [schema (schema-airports)
        spec (PartitionSpec/unpartitioned)
        table-path (str warehouse-dir "/openflights/airports")
        _ (ensure-dir (.getParent (io/file table-path)))
        _ (create-table! tables table-path schema spec)
        rows (->> (load-csv (expect-file "airports.dat"))
                  (map (fn [[id name city country iata icao lat lon alt tz dst tzname & _]]
                         [(parse-long id) name city country iata icao
                          (parse-double lat) (parse-double lon) (parse-int alt)
                          (parse-double tz) dst tzname])))]
    (record-writer schema spec tables table-path rows)))

(defn build-airlines! [^HadoopTables tables]
  (let [schema (schema-airlines)
        spec (PartitionSpec/unpartitioned)
        table-path (str warehouse-dir "/openflights/airlines")
        _ (ensure-dir (.getParent (io/file table-path)))
        _ (create-table! tables table-path schema spec)
        rows (->> (load-csv (expect-file "airlines.dat"))
                  (map (fn [[id name alias iata icao callsign country active & _]]
                         [(parse-long id) name alias iata icao callsign country active])))]
    (record-writer schema spec tables table-path rows)))

(defn build-routes! [^HadoopTables tables]
  (let [schema (schema-routes)
        spec (PartitionSpec/unpartitioned)
        table-path (str warehouse-dir "/openflights/routes")
        _ (ensure-dir (.getParent (io/file table-path)))
        _ (create-table! tables table-path schema spec)
        rows (->> (load-csv (expect-file "routes.dat"))
                  (map (fn [[airline airline-id src src-id dst dst-id codeshare stops equipment & _]]
                         [airline (parse-long airline-id) src (parse-long src-id)
                          dst (parse-long dst-id) codeshare (parse-int stops) equipment])))]
    (record-writer schema spec tables table-path rows)))

(defn -main [& _args]
  (ensure-dir raw-dir)
  (ensure-dir warehouse-dir)
  (let [conf   (Configuration.)
        tables (HadoopTables. conf)]
    (try
      (println "Building Iceberg warehouse at" warehouse-dir)
      (build-airports! tables)
      (build-airlines! tables)
      (build-routes! tables)
      (println "Done.")
      (finally
        (FileSystem/closeAll)))))
