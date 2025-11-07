(ns hometap-query-bench
  "Benchmark SPARQL queries on Hometap database.

   Tests performance and memory consumption."
  (:require [fluree.db.api :as fluree]))

(def storage-path "/Volumes/OWC Envoy Ultra/fluree-main-backup")
(def db-name "fluree-jld/387028092979374")

(defn get-memory-stats
  "Get current JVM memory statistics in MB."
  []
  (let [runtime (Runtime/getRuntime)
        max-mem (.maxMemory runtime)
        total-mem (.totalMemory runtime)
        free-mem (.freeMemory runtime)
        used-mem (- total-mem free-mem)]
    {:max-mb (/ max-mem 1024.0 1024.0)
     :total-mb (/ total-mem 1024.0 1024.0)
     :used-mb (/ used-mem 1024.0 1024.0)
     :free-mb (/ free-mem 1024.0 1024.0)}))

(defn force-gc
  "Request garbage collection and wait a moment for it to complete."
  []
  (System/gc)
  (Thread/sleep 100)
  (System/gc)
  (Thread/sleep 100))

(defn print-memory-stats
  "Print memory statistics."
  [label stats]
  (println (str "\n" label ":"))
  (println (str "  Max memory:   " (format "%.2f MB" (:max-mb stats))))
  (println (str "  Total memory: " (format "%.2f MB" (:total-mb stats))))
  (println (str "  Used memory:  " (format "%.2f MB" (:used-mb stats))))
  (println (str "  Free memory:  " (format "%.2f MB" (:free-mb stats)))))

(defn time-query
  "Execute a SPARQL query with memory and timing measurements."
  [db label query]
  (println (str "\n" (apply str (repeat 60 "-"))))
  (println (str "Query: " label))
  (println (apply str (repeat 60 "-")))

  ;; Force GC and measure baseline memory
  (force-gc)
  (let [mem-before (get-memory-stats)]
    (print-memory-stats "Memory before query" mem-before)

    ;; Execute query with timing
    (let [start (System/nanoTime)
          result @(fluree/query db query {:format :sparql})
          elapsed-ms (/ (- (System/nanoTime) start) 1000000.0)
          mem-after (get-memory-stats)
          mem-delta (- (:used-mb mem-after) (:used-mb mem-before))]

      (println (str "\nResult: " result))
      (println (str "Elapsed: " (format "%.2f ms" elapsed-ms)))
      (print-memory-stats "Memory after query" mem-after)
      (println (str "\nMemory delta: " (format "%+.2f MB" mem-delta)))

      ;; Force GC and measure final memory
      (force-gc)
      (let [mem-after-gc (get-memory-stats)
            mem-retained (- (:used-mb mem-after-gc) (:used-mb mem-before))]
        (print-memory-stats "Memory after GC" mem-after-gc)
        (println (str "\nMemory retained: " (format "%+.2f MB" mem-retained)))

        {:label label
         :result result
         :elapsed-ms elapsed-ms
         :mem-before-mb (:used-mb mem-before)
         :mem-after-mb (:used-mb mem-after)
         :mem-after-gc-mb (:used-mb mem-after-gc)
         :mem-delta-mb mem-delta
         :mem-retained-mb mem-retained}))))

(defn run-benchmark
  "Load database and run benchmark queries with memory measurements."
  []
  (println (str "\n" (apply str (repeat 60 "="))))
  (println "HOMETAP DATABASE BENCHMARK")
  (println (apply str (repeat 60 "=")))
  (println (str "Storage path: " storage-path))
  (println (str "Database: " db-name))

  (println "\nConnecting to file storage...")
  (let [conn @(fluree/connect {:method :file
                               :storage-path storage-path})]
    (println "Connected!")

    (println (str "\nLoading database: " db-name))
    (force-gc)
    (let [mem-before-load (get-memory-stats)]
      (print-memory-stats "Memory before loading database" mem-before-load)

      (let [db @(fluree/db conn db-name)]
        (force-gc)
        (let [mem-after-load (get-memory-stats)
              db-mem (- (:used-mb mem-after-load) (:used-mb mem-before-load))]
          (println "\nDatabase loaded successfully!")
          (print-memory-stats "Memory after loading database" mem-after-load)
          (println (str "\nDatabase size in memory: " (format "%.2f MB" db-mem)))

          ;; Query 1: Count WA Inquiries
          (let [q1 "PREFIX ht: <https://hometap.com/ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                    SELECT (COUNT(DISTINCT ?inquiry) AS ?totalInquiries)
                    WHERE {
                      ?inquiry a ht:Inquiry .
                      ?inquiry ht:inquiryPrequalState \"WA\" .
                    }"

                ;; Query 2: Count WA Tracks
                q2 "PREFIX ht: <https://hometap.com/ns#>

                    SELECT (COUNT(DISTINCT ?track) AS ?totalTracks)
                    WHERE {
                      ?track a ht:Track .
                      ?track ht:state \"WA\" .
                    }"

                ;; Run queries
                r1 (time-query db "WA Inquiries" q1)
                r2 (time-query db "WA Tracks" q2)]

            ;; Summary
            (println (str "\n" (apply str (repeat 60 "="))))
            (println "BENCHMARK SUMMARY")
            (println (apply str (repeat 60 "=")))
            (println (str "\nDatabase load memory: " (format "%.2f MB" db-mem)))
            (println (str "\nTotal query time: "
                          (format "%.2f ms"
                                  (+ (:elapsed-ms r1)
                                     (:elapsed-ms r2)))))
            (println (str "\nQuery 1 (WA Inquiries):"))
            (println (str "  Time: " (format "%.2f ms" (:elapsed-ms r1))))
            (println (str "  Memory retained: " (format "%+.2f MB" (:mem-retained-mb r1))))
            (println (str "\nQuery 2 (WA Tracks):"))
            (println (str "  Time: " (format "%.2f ms" (:elapsed-ms r2))))
            (println (str "  Memory retained: " (format "%+.2f MB" (:mem-retained-mb r2))))
            (println (str "\nTotal memory retained: "
                          (format "%+.2f MB"
                                  (+ (:mem-retained-mb r1)
                                     (:mem-retained-mb r2)))))
            (println (str "\n" (apply str (repeat 60 "="))))

            {:db-load-mem-mb db-mem
             :queries [r1 r2]
             :total-time-ms (+ (:elapsed-ms r1) (:elapsed-ms r2))
             :total-mem-retained-mb (+ (:mem-retained-mb r1) (:mem-retained-mb r2))}))))))

(comment
  ;; Run the benchmark
  (run-benchmark)

  ;; Or run individual queries
  (let [conn @(fluree/connect {:storage-path storage-path})
        db @(fluree/db conn db-name)]
    (time-query db "WA Inquiries"
                "PREFIX ht: <https://hometap.com/ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                 SELECT (COUNT(DISTINCT ?inquiry) AS ?totalInquiries)
                 WHERE {
                   ?inquiry a ht:Inquiry .
                   ?inquiry ht:inquiryPrequalState \"WA\" .
                 }"))
  
  (def conn @(fluree/connect-file {:storage-path storage-path}))
  (def db @(fluree/db conn db-name))
  @(fluree/query db "PREFIX ht: <https://hometap.com/ns#>
                                     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    
                                     SELECT (COUNT(DISTINCT ?inquiry) AS ?totalInquiries)
                                     WHERE {
                                       ?inquiry a ht:Inquiry .
                                       ?inquiry ht:inquiryPrequalState \"WA\" .
                                     }" {:format :sparql})
  (time-query db "WA Inquiries"
              "PREFIX ht: <https://hometap.com/ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  
                   SELECT (COUNT(DISTINCT ?inquiry) AS ?totalInquiries)
                   WHERE {
                     ?inquiry ht:inquiryPrequalState \"WA\" .
                     ?inquiry a ht:Inquiry .
                   }")


  ;; Check current memory usage
  (let [stats (get-memory-stats)]
    (println (str "Used memory: " (format "%.2f MB" (:used-mb stats)))))

  ;; Force GC and check memory
  (do
    (force-gc)
    (let [stats (get-memory-stats)]
      (println (str "Used memory after GC: " (format "%.2f MB" (:used-mb stats))))))

  )
