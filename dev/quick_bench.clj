(ns quick-bench
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.flake :as flake]))

(defn bench-sid-creation
  "Benchmark SID creation with interning."
  [n]
  (println (str "\n=== Benchmarking " n " SID creations ==="))

  ;; Warm up
  (dotimes [i 1000]
    (iri/deserialize-sid [8 (str "test-" i)]))

  ;; Benchmark
  (let [start (System/nanoTime)]
    (dotimes [i n]
      (iri/deserialize-sid [8 (str "subject-" (mod i 100))]))
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1000000.0)
          throughput (/ n (/ elapsed-ms 1000.0))]
      (println (str "Time: " (format "%.2f ms" elapsed-ms)))
      (println (str "Throughput: " (format "%,.0f SIDs/sec" throughput)))
      {:elapsed-ms elapsed-ms
       :throughput throughput})))

(comment
  ;; Run benchmark
  (bench-sid-creation 1000000)
  )
