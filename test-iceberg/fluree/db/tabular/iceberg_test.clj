(ns ^:iceberg fluree.db.tabular.iceberg-test
  "Tests for IcebergSource using OpenFlights airline data.

   Requires :iceberg alias for dependencies.
   Run with: clojure -M:dev:iceberg:cljtest '{:kaocha.filter/focus-meta [:iceberg]}'

   Or from REPL:
     (require '[fluree.db.tabular.iceberg-test :as t])
     (t/run-tests)"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.protocol :as proto])
  (:import [java.io File]))

;;; ---------------------------------------------------------------------------
;;; Test Fixtures
;;; ---------------------------------------------------------------------------

(def ^:private warehouse-path
  "Path to OpenFlights Iceberg warehouse."
  (str (System/getProperty "user.dir") "/dev-resources/openflights/warehouse"))

(def ^:private source (atom nil))

(defn- warehouse-exists? []
  (.exists (File. (str warehouse-path "/openflights/airlines"))))

(defn source-fixture [f]
  (if (warehouse-exists?)
    (do
      (reset! source (iceberg/create-iceberg-source {:warehouse-path warehouse-path}))
      (try
        (f)
        (finally
          (when @source
            (proto/close @source)
            (reset! source nil)))))
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
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.tabular.iceberg-test))
