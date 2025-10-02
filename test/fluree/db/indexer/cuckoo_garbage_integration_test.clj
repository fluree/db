(ns fluree.db.indexer.cuckoo-garbage-integration-test
  "Integration test verifying cuckoo filter garbage collection mechanics."
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.util.cbor :as cbor])
  (:import (java.io FileInputStream)))

(defn- read-cuckoo-filter
  "Helper to read and decode a cuckoo filter file."
  [storage-path ledger branch]
  (let [filter-path (io/file storage-path ledger "index" "cuckoo" (str branch ".cbor"))]
    (when (.exists filter-path)
      (with-open [fis (FileInputStream. filter-path)]
        (let [cbor-bytes (.readAllBytes fis)]
          (cuckoo/deserialize (cbor/decode cbor-bytes)))))))

(defn- list-index-segments
  "Lists all index segment files (excluding cuckoo, root, garbage)."
  [storage-path ledger]
  (let [index-dir (io/file storage-path ledger "index")]
    (when (.exists index-dir)
      (->> (file-seq index-dir)
           (filter #(.isFile ^java.io.File %))
           (filter #(str/ends-with? (.getName ^java.io.File %) ".json"))
           (remove #(str/includes? (.getPath ^java.io.File %) "/cuckoo/"))
           (remove #(str/includes? (.getPath ^java.io.File %) "/garbage/"))
           (remove #(str/includes? (.getName ^java.io.File %) "root"))
           (map (fn [^java.io.File f]
                  (let [name (.getName f)]
                    ;; Extract just the hash part (remove .json extension)
                    (str/replace name #"\.json$" ""))))
           set))))

(deftest ^:integration cuckoo-garbage-collection-integration-test
  (testing "Cuckoo filter correctly tracks segments for garbage collection"
    (with-temp-dir [storage-path {}]
      (let [storage-str (str storage-path)
            conn @(fluree/connect-file {:storage-path storage-str
                                        :defaults {:indexing {:reindex-min-bytes 100
                                                              :reindex-max-bytes 1000
                                                              :max-old-indexes 2}}})
            _ @(fluree/create conn "gc-test")]

        (testing "Initial index has segments in cuckoo filter"
          ;; Add data to trigger indexing
          @(fluree/insert! conn "gc-test"
                           [{"@context" {"ex" "http://example.org/"}
                             "@id" "ex:alice"
                             "@type" "ex:Person"
                             "ex:name" "Alice"
                             "ex:age" 30}])
          @(fluree/trigger-index conn "gc-test" {:block? true})

          (let [filter (read-cuckoo-filter storage-str "gc-test" "main")
                segments (list-index-segments storage-str "gc-test")]

            (is (some? filter) "Cuckoo filter should exist")
            (is (seq segments) "Index segments should exist")

            ;; Verify every segment file is in the cuckoo filter
            (testing "All segment files are tracked in cuckoo filter"
              (doseq [segment segments]
                (is (cuckoo/contains-hash-chain? filter segment)
                    (str "Segment " segment " should be in cuckoo filter"))))))

        (testing "After garbage collection, filter matches current segments"
          ;; Trigger several more indexes to create garbage
          @(fluree/insert! conn "gc-test"
                           [{"@id" "ex:bob" "@type" "ex:Person" "ex:name" "Bob"}])
          @(fluree/trigger-index conn "gc-test" {:block? true})

          @(fluree/insert! conn "gc-test"
                           [{"@id" "ex:charlie" "@type" "ex:Person" "ex:name" "Charlie"}])
          @(fluree/trigger-index conn "gc-test" {:block? true})

          @(fluree/insert! conn "gc-test"
                           [{"@id" "ex:diana" "@type" "ex:Person" "ex:name" "Diana"}])
          @(fluree/trigger-index conn "gc-test" {:block? true})

          ;; Wait for GC to complete
          (Thread/sleep 100)

          (let [filter (read-cuckoo-filter storage-str "gc-test" "main")
                segments (list-index-segments storage-str "gc-test")]

            (is (some? filter) "Cuckoo filter should exist after GC")
            (is (seq segments) "Index segments should exist after GC")

            (testing "All current segments exist in cuckoo filter"
              (doseq [segment segments]
                (is (cuckoo/contains-hash-chain? filter segment)
                    (str "Segment " segment " should be in filter"))))))

        (testing "Branch isolation with shared segments"
          @(fluree/create-branch! conn "gc-test:feature" "gc-test:main")

          (let [main-filter (read-cuckoo-filter storage-str "gc-test" "main")
                feature-filter (read-cuckoo-filter storage-str "gc-test" "feature")
                current-segments (list-index-segments storage-str "gc-test")
                main-stats (cuckoo/get-chain-stats main-filter)
                feature-stats (cuckoo/get-chain-stats feature-filter)]

            (is (some? feature-filter) "Feature branch filter should exist")
            (is (= (:total-count main-stats) (:total-count feature-stats))
                "Branch filter should have same total count as main")

            (testing "Branch filter contains all current segments"
              (doseq [segment current-segments]
                (is (cuckoo/contains-hash-chain? feature-filter segment)
                    (str "Current segment " segment " should be in feature branch filter"))))))))))
