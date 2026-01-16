(ns fluree.db.tabular.iceberg-bench
  "Benchmark comparing Arrow vectorized reads vs IcebergGenerics.

   Run from REPL:
     (require '[fluree.db.tabular.iceberg-bench :as bench])
     (bench/run-benchmark)

   Or from command line:
     clojure -M:dev:iceberg -m fluree.db.tabular.iceberg-bench"
  (:require [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.tabular.iceberg.arrow :as arrow]
            [fluree.db.tabular.protocol :as proto])
  (:import [java.io File]
           [org.apache.iceberg Table]
           [org.apache.hadoop.conf Configuration]
           [org.apache.iceberg.hadoop HadoopTables]))

(defn- find-resource-path
  "Find a resource path, checking multiple locations."
  [relative-path]
  (let [user-dir (System/getProperty "user.dir")
        candidates [(str user-dir "/test-resources/" relative-path)              ;; from db-iceberg-arrow/
                    (str user-dir "/../db-iceberg/test-resources/" relative-path) ;; from db-iceberg-arrow/ -> db-iceberg/
                    (str user-dir "/db-iceberg/test-resources/" relative-path)]] ;; from db/
    (first (filter #(.exists (File. %)) candidates))))

(def ^:private warehouse-path
  (find-resource-path "openflights/warehouse"))

(def ^:private table-name "openflights/airlines")

(defn- load-table
  "Load Iceberg table directly for benchmarking."
  ^Table []
  (let [conf (Configuration.)
        tables (HadoopTables. conf)
        table-path (str warehouse-path "/" table-name)]
    (.load tables table-path)))

(defn- time-ms
  "Execute f and return [result time-ms]."
  [f]
  (let [start (System/nanoTime)
        result (f)
        end (System/nanoTime)]
    [result (/ (- end start) 1e6)]))

(defn- force-and-count
  "Force lazy seq and count results."
  [lazy-seq]
  (count (doall lazy-seq)))

(defn benchmark-full-scan
  "Benchmark full table scan (no predicates)."
  []
  (println "\n=== Full Table Scan (6162 rows) ===")
  (let [table (load-table)
        opts {:columns ["id" "name" "country"]}]

    ;; Warm up
    (println "Warming up...")
    (force-and-count (arrow/scan-with-arrow table opts))
    (force-and-count (core/scan-with-generics table opts))

    ;; Arrow
    (print "Arrow vectorized:   ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (arrow/scan-with-arrow table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))

    ;; Generics
    (print "IcebergGenerics:    ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (core/scan-with-generics table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))))

(defn benchmark-filtered-scan
  "Benchmark scan with IN predicate (US + Canada = 1422 rows)."
  []
  (println "\n=== Filtered Scan (IN predicate, ~1422 rows) ===")
  (let [table (load-table)
        opts {:columns ["id" "name" "country"]
              :predicates [{:column "country" :op :in :value ["United States" "Canada"]}]}]

    ;; Warm up
    (println "Warming up...")
    (force-and-count (arrow/scan-with-arrow table opts))
    (force-and-count (core/scan-with-generics table opts))

    ;; Arrow
    (print "Arrow vectorized:   ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (arrow/scan-with-arrow table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))

    ;; Generics
    (print "IcebergGenerics:    ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (core/scan-with-generics table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))))

(defn benchmark-small-result
  "Benchmark scan returning small result set."
  []
  (println "\n=== Small Result (single country, ~1099 rows) ===")
  (let [table (load-table)
        opts {:columns ["id" "name" "country"]
              :predicates [{:column "country" :op :eq :value "United States"}]}]

    ;; Warm up
    (println "Warming up...")
    (force-and-count (arrow/scan-with-arrow table opts))
    (force-and-count (core/scan-with-generics table opts))

    ;; Arrow
    (print "Arrow vectorized:   ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (arrow/scan-with-arrow table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))

    ;; Generics
    (print "IcebergGenerics:    ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (core/scan-with-generics table opts)))]
      (println (format "%d rows in %.1f ms (%.0f rows/sec)" cnt ms (* 1000 (/ cnt ms)))))))

(defn benchmark-with-limit
  "Benchmark scan with LIMIT."
  []
  (println "\n=== With LIMIT 100 ===")
  (let [table (load-table)
        opts {:columns ["id" "name" "country"]
              :limit 100}]

    ;; Warm up
    (println "Warming up...")
    (force-and-count (arrow/scan-with-arrow table opts))
    (force-and-count (core/scan-with-generics table opts))

    ;; Arrow
    (print "Arrow vectorized:   ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (arrow/scan-with-arrow table opts)))]
      (println (format "%d rows in %.1f ms" cnt ms)))

    ;; Generics
    (print "IcebergGenerics:    ")
    (flush)
    (let [[cnt ms] (time-ms #(force-and-count (core/scan-with-generics table opts)))]
      (println (format "%d rows in %.1f ms" cnt ms)))))

(defn benchmark-repeated
  "Run multiple iterations to get stable timings."
  [iterations]
  (println (format "\n=== Repeated Full Scan (%d iterations) ===" iterations))
  (let [table (load-table)
        opts {:columns ["id" "name" "country"]}]

    ;; Warm up
    (println "Warming up...")
    (dotimes [_ 3]
      (force-and-count (arrow/scan-with-arrow table opts))
      (force-and-count (core/scan-with-generics table opts)))

    ;; Arrow
    (print "Arrow vectorized:   ")
    (flush)
    (let [times (for [_ (range iterations)]
                  (second (time-ms #(force-and-count (arrow/scan-with-arrow table opts)))))
          avg (/ (reduce + times) iterations)
          min-t (apply min times)
          max-t (apply max times)]
      (println (format "avg=%.1f ms, min=%.1f ms, max=%.1f ms" avg min-t max-t)))

    ;; Generics
    (print "IcebergGenerics:    ")
    (flush)
    (let [times (for [_ (range iterations)]
                  (second (time-ms #(force-and-count (core/scan-with-generics table opts)))))
          avg (/ (reduce + times) iterations)
          min-t (apply min times)
          max-t (apply max times)]
      (println (format "avg=%.1f ms, min=%.1f ms, max=%.1f ms" avg min-t max-t)))))

(defn run-benchmark
  "Run all benchmarks."
  []
  (println "========================================")
  (println "Iceberg Read Performance: Arrow vs Generics")
  (println "========================================")
  (println (str "Table: " table-name))
  (println (str "Warehouse: " warehouse-path))

  (benchmark-full-scan)
  (benchmark-filtered-scan)
  (benchmark-small-result)
  (benchmark-with-limit)
  (benchmark-repeated 5)

  (println "\n========================================")
  (println "Benchmark complete.")
  (println "========================================"))

(defn -main [& _args]
  (run-benchmark)
  (System/exit 0))
