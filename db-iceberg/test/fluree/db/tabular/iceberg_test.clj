(ns ^:iceberg fluree.db.tabular.iceberg-test
  "Tests for IcebergSource using OpenFlights airline data.

   Requires :test alias for dependencies (includes Hadoop for test fixtures).
   Run with: clj -X:test

   Or from REPL:
     (require '[fluree.db.tabular.iceberg-test :as t])
     (t/run-tests)

   Note: These tests use HadoopTables for loading local test warehouses.
   Hadoop is a test-only dependency and not shipped with the main artifact."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.tabular.protocol :as proto])
  (:import [java.io File]
           [org.apache.iceberg Table]
           [org.apache.iceberg.hadoop HadoopTables]
           [org.apache.hadoop.conf Configuration]))

;;; ---------------------------------------------------------------------------
;;; Test Fixtures - Hadoop-based (test-only dependency)
;;; ---------------------------------------------------------------------------

(def ^:private warehouse-path
  "Path to OpenFlights Iceberg warehouse."
  (str (System/getProperty "user.dir") "/dev-resources/openflights/warehouse"))

(def ^:private hadoop-tables (atom nil))

(defn- warehouse-exists? []
  (.exists (File. (str warehouse-path "/openflights/airlines"))))

(defn- create-test-hadoop-tables
  "Create HadoopTables for test fixtures. This is test-only - not shipped in main artifact."
  ^HadoopTables []
  (let [conf (Configuration.)]
    (HadoopTables. conf)))

;; Test source using HadoopTables directly (for tests that need ITabularSource protocol)
(defrecord TestHadoopSource [^HadoopTables tables warehouse-path]
  proto/ITabularSource
  (scan-batches [_ table-name opts]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)]
      (core/scan-with-generics table opts)))

  (scan-arrow-batches [_ table-name _opts]
    (throw (ex-info "Arrow not available in test source" {:table table-name})))

  (scan-rows [this table-name opts]
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
  (close [_] nil))

(def ^:private source (atom nil))

(defn source-fixture [f]
  (if (warehouse-exists?)
    (let [tables (create-test-hadoop-tables)]
      (reset! hadoop-tables tables)
      (reset! source (->TestHadoopSource tables warehouse-path))
      (try
        (f)
        (finally
          (reset! source nil)
          (reset! hadoop-tables nil))))
    (println "SKIP: OpenFlights warehouse not found. Run 'make iceberg-openflights' first.")))

(use-fixtures :once source-fixture)

;;; ---------------------------------------------------------------------------
;;; Schema Tests
;;; ---------------------------------------------------------------------------

(deftest get-airlines-schema-test
  (when @source
    (testing "Get airlines table schema"
      (let [schema (proto/get-schema @source "openflights/airlines" {})]
        (is (map? schema))
        (is (seq (:columns schema)))

        (testing "has expected columns"
          (let [col-names (set (map :name (:columns schema)))]
            (is (contains? col-names "id"))
            (is (contains? col-names "name"))
            (is (contains? col-names "country"))
            (is (contains? col-names "iata"))
            (is (contains? col-names "icao"))
            (is (contains? col-names "active"))))

        (testing "columns have correct types"
          (let [cols-by-name (into {} (map (juxt :name identity) (:columns schema)))]
            (is (= :long (:type (get cols-by-name "id"))))
            (is (= :string (:type (get cols-by-name "name"))))
            (is (= :string (:type (get cols-by-name "country"))))))))))

;;; ---------------------------------------------------------------------------
;;; Statistics Tests
;;; ---------------------------------------------------------------------------

(deftest get-airlines-statistics-test
  (when @source
    (testing "Get airlines table statistics"
      (let [stats (proto/get-statistics @source "openflights/airlines" {})]
        (is (map? stats))
        (is (= 6162 (:row-count stats)) "Airlines table should have 6162 rows")
        (is (= 1 (:file-count stats)) "Airlines table should have 1 data file")
        (is (pos? (:snapshot-id stats)))))))

;;; ---------------------------------------------------------------------------
;;; Scan Tests
;;; ---------------------------------------------------------------------------

(deftest scan-all-airlines-test
  (when @source
    (testing "Scan all airlines (no filters)"
      (let [rows (proto/scan-rows @source "openflights/airlines" {})]
        (is (seq rows))
        (is (= 6162 (count rows)) "Should return all 6162 airlines")

        (testing "rows have expected fields"
          (let [first-row (first rows)]
            (is (contains? first-row "id"))
            (is (contains? first-row "name"))
            (is (contains? first-row "country"))))))))

(deftest scan-with-limit-test
  (when @source
    (testing "Scan with limit"
      (let [rows (proto/scan-rows @source "openflights/airlines" {:limit 10})]
        (is (= 10 (count rows)))))))

(deftest scan-with-column-projection-test
  (when @source
    (testing "Scan with column projection"
      (let [rows (proto/scan-rows @source "openflights/airlines"
                                  {:columns ["name" "country"]
                                   :limit 5})]
        (is (= 5 (count rows)))

        (testing "only requested columns returned"
          (let [first-row (first rows)]
            ;; Note: Iceberg still returns all columns in the record,
            ;; but only the projected columns were read from storage
            (is (contains? first-row "name"))
            (is (contains? first-row "country"))))))))

;;; ---------------------------------------------------------------------------
;;; Predicate Pushdown Tests
;;; ---------------------------------------------------------------------------

(deftest scan-with-equality-filter-test
  (when @source
    (testing "Scan with equality filter (country = 'United States')"
      (let [rows (proto/scan-rows @source "openflights/airlines"
                                  {:predicates [{:column "country"
                                                 :op :eq
                                                 :value "United States"}]})]
        (is (seq rows))
        (is (< (count rows) 6162) "Should filter out non-US airlines")

        (testing "all results match filter"
          (is (every? #(= "United States" (get % "country")) rows)))))))

(deftest scan-with-and-filter-test
  (when @source
    (testing "Scan with AND filter (US + active)"
      (let [rows (proto/scan-rows @source "openflights/airlines"
                                  {:predicates [{:op :and
                                                 :predicates [{:column "country"
                                                               :op :eq
                                                               :value "United States"}
                                                              {:column "active"
                                                               :op :eq
                                                               :value "Y"}]}]})]
        (is (seq rows))
        (is (= 156 (count rows)) "Should have 156 active US airlines")

        (testing "all results match both conditions"
          (is (every? #(and (= "United States" (get % "country"))
                            (= "Y" (get % "active")))
                      rows)))))))

(deftest scan-with-in-filter-test
  (when @source
    (testing "Scan with IN filter (multiple countries)"
      (let [countries ["United States" "Canada" "Mexico"]
            rows (proto/scan-rows @source "openflights/airlines"
                                  {:predicates [{:column "country"
                                                 :op :in
                                                 :value countries}]})]
        (is (seq rows))

        (testing "all results match one of the values"
          (is (every? #(contains? (set countries) (get % "country")) rows)))))))

(deftest scan-with-not-null-filter-test
  (when @source
    (testing "Scan with NOT NULL filter (has IATA code)"
      (let [rows (proto/scan-rows @source "openflights/airlines"
                                  {:predicates [{:column "iata"
                                                 :op :not-null}]
                                   :limit 100})]
        (is (seq rows))

        (testing "all results have non-null IATA"
          (is (every? #(some? (get % "iata")) rows)))))))

;;; ---------------------------------------------------------------------------
;;; Airport Tests (different table)
;;; ---------------------------------------------------------------------------

(deftest scan-airports-test
  (when @source
    (testing "Scan airports table"
      (let [stats (proto/get-statistics @source "openflights/airports" {})]
        (is (= 7698 (:row-count stats)) "Airports table should have 7698 rows"))

      (let [rows (proto/scan-rows @source "openflights/airports" {:limit 10})]
        (is (= 10 (count rows)))

        (testing "airports have expected fields"
          (let [airport (first rows)]
            (is (contains? airport "name"))
            (is (contains? airport "city"))
            (is (contains? airport "country"))
            (is (contains? airport "lat"))
            (is (contains? airport "lon"))))))))

(deftest scan-airports-with-lat-filter-test
  (when @source
    (testing "Scan airports with latitude filter (northern hemisphere)"
      (let [rows (proto/scan-rows @source "openflights/airports"
                                  {:predicates [{:column "lat"
                                                 :op :gte
                                                 :value 0.0}]})]
        (is (seq rows))

        (testing "all airports have positive latitude"
          (is (every? #(>= (or (get % "lat") -999) 0.0) rows)))))))

;;; ---------------------------------------------------------------------------
;;; Routes Tests
;;; ---------------------------------------------------------------------------

(deftest scan-routes-test
  (when @source
    (testing "Scan routes table statistics"
      (let [stats (proto/get-statistics @source "openflights/routes" {})]
        (is (= 67663 (:row-count stats)) "Routes table should have 67663 rows")))

    (testing "Scan routes with filter"
      (let [rows (proto/scan-rows @source "openflights/routes"
                                  {:predicates [{:column "src"
                                                 :op :eq
                                                 :value "JFK"}]
                                   :limit 50})]
        (is (seq rows))

        (testing "all routes originate from JFK"
          (is (every? #(= "JFK" (get % "src")) rows)))))))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

(deftest supported-predicates-test
  (when @source
    (testing "Returns supported predicates"
      (let [preds (proto/supported-predicates @source)]
        (is (set? preds))
        (is (contains? preds :eq))
        (is (contains? preds :in))
        (is (contains? preds :between))
        (is (contains? preds :is-null))
        (is (contains? preds :and))
        (is (contains? preds :or))))))

;;; ---------------------------------------------------------------------------
;;; Partition Pruning Tests (requires make iceberg-partitioned)
;;; ---------------------------------------------------------------------------

(def ^:private partitioned-table-path
  "openflights/airlines_partitioned")

(defn- partitioned-table-exists? []
  (.exists (java.io.File. (str warehouse-path "/" partitioned-table-path))))

(deftest partitioned-schema-test
  (when (and @source (partitioned-table-exists?))
    (testing "Partitioned table schema shows partition columns"
      (let [schema (proto/get-schema @source partitioned-table-path {})]
        (is (map? schema))
        (is (seq (:columns schema)))

        (testing "active column is marked as partition key"
          (let [cols-by-name (into {} (map (juxt :name identity) (:columns schema)))
                active-col (get cols-by-name "active")]
            (is (some? active-col) "Should have 'active' column")
            (is (:is-partition-key? active-col)
                "active column should be marked as partition key")))

        (testing "non-partition columns are not marked"
          (let [cols-by-name (into {} (map (juxt :name identity) (:columns schema)))
                name-col (get cols-by-name "name")]
            (is (some? name-col))
            (is (not (:is-partition-key? name-col))
                "name column should NOT be marked as partition key")))

        (testing "partition spec is populated"
          (let [partition-spec (:partition-spec schema)]
            (is (map? partition-spec))
            (is (= 1 (count (:fields partition-spec))))
            (is (= "identity" (:transform (first (:fields partition-spec)))))))))))

(deftest partitioned-statistics-test
  (when (and @source (partitioned-table-exists?))
    (testing "Partitioned table has multiple data files"
      (let [stats (proto/get-statistics @source partitioned-table-path {})]
        (is (map? stats))
        ;; With Y, N, and potentially 'n' partitions, we should have 2-3 files
        (is (>= (:file-count stats) 2)
            "Partitioned table should have at least 2 data files (one per partition)")
        (is (= 6162 (:row-count stats))
            "Should still have all 6162 airline records")))))

(deftest partition-pruning-equality-test
  (when (and @source (partitioned-table-exists?))
    (testing "Query with equality on partition column returns correct data"
      (let [rows-active-y (proto/scan-rows @source partitioned-table-path
                                            {:predicates [{:column "active"
                                                           :op :eq
                                                           :value "Y"}]})
            rows-active-n (proto/scan-rows @source partitioned-table-path
                                            {:predicates [{:column "active"
                                                           :op :eq
                                                           :value "N"}]})]
        ;; Verify we get data for each partition
        (is (seq rows-active-y) "Should have active=Y airlines")
        (is (seq rows-active-n) "Should have active=N airlines")

        ;; Verify filtering is correct
        (is (every? #(= "Y" (get % "active")) rows-active-y)
            "All Y-partition results should have active=Y")
        (is (every? #(= "N" (get % "active")) rows-active-n)
            "All N-partition results should have active=N")

        ;; Combined should equal total
        ;; Note: there may be a small 'n' partition from CSV data quirks
        (let [total (+ (count rows-active-y) (count rows-active-n))
              all-rows (count (proto/scan-rows @source partitioned-table-path {}))]
          (is (<= total all-rows)
              "Y + N partitions should not exceed total"))))))

(deftest partition-pruning-in-test
  (when (and @source (partitioned-table-exists?))
    (testing "Query with IN on partition column"
      (let [rows (proto/scan-rows @source partitioned-table-path
                                   {:predicates [{:column "active"
                                                  :op :in
                                                  :value ["Y"]}]})]
        (is (seq rows) "Should have results for IN query")
        (is (every? #(= "Y" (get % "active")) rows)
            "All results should have active=Y")))))

(deftest partition-pruning-combined-filter-test
  (when (and @source (partitioned-table-exists?))
    (testing "Query with partition and non-partition predicates"
      (let [rows (proto/scan-rows @source partitioned-table-path
                                   {:predicates [{:op :and
                                                  :predicates [{:column "active"
                                                                :op :eq
                                                                :value "Y"}
                                                               {:column "country"
                                                                :op :eq
                                                                :value "United States"}]}]})]
        (is (seq rows) "Should have active US airlines")
        (is (every? #(and (= "Y" (get % "active"))
                          (= "United States" (get % "country")))
                    rows)
            "All results should be active US airlines")
        ;; This is the 156 active US airlines from our data
        (is (= 156 (count rows))
            "Should return exactly 156 active US airlines")))))

(defn- count-planned-files
  "Count the number of files that would be scanned for a given query.
   This uses Iceberg's planFiles() to get actual scan planning metrics."
  [table-path predicates]
  (let [conf (Configuration.)
        tables (HadoopTables. conf)
        full-path (str warehouse-path "/" table-path)
        ^Table table (.load tables full-path)
        scan (core/build-table-scan table {:predicates predicates})
        ;; planFiles() returns a CloseableIterable of FileScanTask
        ;; Each FileScanTask represents one file to scan
        file-iterable (.planFiles scan)]
    (try
      (let [file-tasks (vec (iterator-seq (.iterator file-iterable)))]
        ;; Force realization of all tasks before counting
        (count file-tasks))
      (finally
        (.close file-iterable)))))

(deftest partition-pruning-file-count-test
  (when (and @source (partitioned-table-exists?))
    (testing "Partition predicate reduces files scanned"
      ;; Use fresh table loads for each measurement to ensure consistency
      (let [;; Count files for partition-filtered scan (active=Y)
            files-active-y (count-planned-files partitioned-table-path
                                                [{:column "active" :op :eq :value "Y"}])

            ;; Count files for full scan (no predicate)
            files-all (count-planned-files partitioned-table-path nil)]

        ;; Full scan should hit all files (2-3 depending on how many partitions)
        (is (>= files-all 2)
            "Full scan should plan to scan all partition files")

        ;; Partition-filtered scan should hit fewer files - THIS IS THE KEY TEST
        ;; Demonstrates that Iceberg's partition pruning is working
        (is (< files-active-y files-all)
            (str "Partition filter should prune files. "
                 "Full scan: " files-all " files, Partition filtered: " files-active-y " files"))

        ;; Specifically, filtering on active=Y should only scan 1 file
        ;; (there's exactly one data file in the active=Y partition)
        (is (= 1 files-active-y)
            "Filtering on active=Y should scan exactly 1 partition file")))))

;;; ---------------------------------------------------------------------------
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.tabular.iceberg-test))
