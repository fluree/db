(ns fluree.db.indexer.cuckoo-test
  "Test suite for cuckoo filter functionality."
  (:require [alphabase.core :as alphabase]
            [babashka.fs :refer [with-temp-dir]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.util.cbor :as cbor])
  (:import (java.io FileInputStream)))

(defn- test-hash
  "Create a valid base32 hash for testing from a string."
  [s]
  (-> s
      crypto/sha2-256  ; Returns hex string
      alphabase/hex->bytes  ; Convert hex to bytes
      alphabase/bytes->base32))  ; Already lowercase from fluree crypto

(deftest create-filter-test
  (testing "Create filter with expected capacity"
    (let [filter (cuckoo/create-filter 1000)]
      (is (= 16 (:fingerprint-bits filter)))
      (is (> (:num-buckets filter) 0))
      (is (= 0 (:count filter))))))

(deftest add-and-contains-test
  (testing "Add item and check membership"
    (let [chain (cuckoo/create-filter-chain)
          hash1  (test-hash "abc123def456")
          chain' (cuckoo/add-item-chain chain hash1)]
      (is chain')
      (is (= 1 (-> chain' cuckoo/get-chain-stats :total-count)))
      (is (cuckoo/contains-hash-chain? chain' hash1))
      (is (not (cuckoo/contains-hash-chain? chain (test-hash "nonexistent"))))))

  (testing "Batch add items"
    (let [chain  (cuckoo/create-filter-chain)
          hashes  [(test-hash "hash1") (test-hash "hash2") (test-hash "hash3")]
          chain' (cuckoo/batch-add-chain chain hashes)]
      (is (= 3 (-> chain' cuckoo/get-chain-stats :total-count)))
      (is (every? #(cuckoo/contains-hash-chain? chain' %) hashes)))))

(deftest remove-item-test
  (testing "Remove existing item"
    (let [chain  (cuckoo/create-filter-chain)
          hash1   (test-hash "removeme")
          chain' (-> chain
                     (cuckoo/add-item-chain hash1)
                     (cuckoo/remove-item-chain hash1))]
      (is (= 0 (-> chain' cuckoo/get-chain-stats :total-count)))
      (is (not (cuckoo/contains-hash-chain? chain' hash1)))))

  (testing "Remove non-existent item"
    (let [chain  (cuckoo/create-filter-chain)
          chain' (cuckoo/remove-item-chain chain (test-hash "nonexistent"))]
      (is (= (-> chain cuckoo/get-chain-stats :total-count)
             (-> chain' cuckoo/get-chain-stats :total-count))))))

(deftest serialization-test
  (testing "Serialize and deserialize filter chain"
    (let [chain    (-> (cuckoo/create-filter-chain)
                       (cuckoo/add-item-chain (test-hash "hash1"))
                       (cuckoo/add-item-chain (test-hash "hash2")))
          restored (cuckoo/deserialize chain)]
      (let [original-stats (cuckoo/get-chain-stats chain)
            restored-stats (cuckoo/get-chain-stats restored)]
        (is (= (:total-count original-stats) (:total-count restored-stats)))
        (is (= (:fingerprint-bits original-stats) (:fingerprint-bits restored-stats))))
      (is (cuckoo/contains-hash-chain? restored (test-hash "hash1")))
      (is (cuckoo/contains-hash-chain? restored (test-hash "hash2"))))))

(deftest metrics-test
  (testing "Chain statistics"
    (let [chain (-> (cuckoo/create-filter-chain)
                    (cuckoo/add-item-chain (test-hash "item1"))
                    (cuckoo/add-item-chain (test-hash "item2")))
          stats (cuckoo/get-chain-stats chain)]
      (is (contains? stats :total-count))
      (is (contains? stats :total-capacity))
      (is (contains? stats :overall-load-factor))
      (is (contains? stats :filter-count))
      (is (> (:overall-load-factor stats) 0))
      (is (< (:overall-load-factor stats) 1)))))

(deftest realistic-address-test
  (testing "Works with realistic Fluree index addresses"
    (let [chain (cuckoo/create-filter-chain)
          ;; Simulate realistic index segment addresses with real base32 hashes
          hash1 (test-hash "segment1")
          hash2 (test-hash "segment2")
          hash3 (test-hash "segment3")
          hash4 (test-hash "segment4")
          hash5 (test-hash "segment5")
          addresses [(str "fluree:file://ledger/index/spot/" hash1 ".json")
                     (str "fluree:file://ledger/index/post/" hash2 ".json")
                     (str "fluree:file://ledger/index/opst/" hash3 ".json")
                     (str "ledger/index/tspo/" hash4 ".json")
                     hash5]  ; Just the hash itself
          chain' (cuckoo/batch-add-chain chain addresses)]
      (is (= 5 (-> chain' cuckoo/get-chain-stats :total-count)))
      (is (every? #(cuckoo/contains-hash-chain? chain' %) addresses))
      (let [not-in-filter-hash (test-hash "notinfilter")]
        (is (not (cuckoo/contains-hash-chain? chain'
                                              (str "fluree:file://ledger/index/spot/" not-in-filter-hash ".json")))))))

  (testing "Chain auto-expansion with many items"
    (let [chain (cuckoo/create-filter-chain)
          ;; Add many items to test chain expansion
          many-items (map #(test-hash (str "item-" %)) (range 50))
          chain' (cuckoo/batch-add-chain chain many-items)]
      (is (= 50 (-> chain' cuckoo/get-chain-stats :total-count)))
      (is (every? #(cuckoo/contains-hash-chain? chain' %) many-items)))))

(deftest false-positive-rate-test
  (testing "No false negatives with moderate dataset"
    (let [chain (cuckoo/create-filter-chain)
          items  (map #(test-hash (str "item" %)) (range 100))
          chain' (cuckoo/batch-add-chain chain items)]
      ;; All added items must be found (no false negatives)
      (is (every? #(cuckoo/contains-hash-chain? chain' %) items))))

  (testing "Acceptable false positive rate"
    (let [chain     (cuckoo/create-filter-chain)
          items     (map #(test-hash (str "item" %)) (range 250))
          chain'    (cuckoo/batch-add-chain chain items)
          non-items (map #(test-hash (str "nonitem" %)) (range 1000))
          fps       (count (clojure.core/filter #(cuckoo/contains-hash-chain? chain' %) non-items))
          fp-rate   (/ fps 1000.0)]
      ;; With 16-bit fingerprints, expect low FPR
      (is (< fp-rate 0.05) "False positive rate should be less than 5%"))))

(deftest cross-branch-checking-test
  (testing "Multiple filters can check for shared items"
    (let [;; Simulate three branch filters using chains
          main-chain (cuckoo/create-filter-chain)
          branch1-chain (cuckoo/create-filter-chain)
          branch2-chain (cuckoo/create-filter-chain)

          ;; Shared segments (in all branches)
          shared [(test-hash "segment1") (test-hash "segment2") (test-hash "segment3")]

          ;; Branch-specific segments
          main-only [(test-hash "main1") (test-hash "main2")]
          branch1-only [(test-hash "branch1-1") (test-hash "branch1-2")]

          ;; Add to filters
          main-chain' (cuckoo/batch-add-chain main-chain (concat shared main-only))
          branch1-chain' (cuckoo/batch-add-chain branch1-chain (concat shared branch1-only))
          branch2-chain' (cuckoo/batch-add-chain branch2-chain shared)

          other-filters [branch1-chain' branch2-chain']]

      ;; Verify main filter has the expected items
      (is (= 5 (-> main-chain' cuckoo/get-chain-stats :total-count)))

      ;; Shared segments should be found in other branches
      (is (every? #(cuckoo/any-branch-uses? other-filters %) shared))

      ;; Main-only segments should not be found in other branches
      (is (not-any? #(cuckoo/any-branch-uses? other-filters %) main-only)))))

(deftest collision-handling-test
  (testing "Handle items with similar patterns that might collide"
    (let [chain (cuckoo/create-filter-chain)
          ;; Create items with similar patterns
          base-items [(test-hash "aaaa") (test-hash "aaab") (test-hash "aaac")
                      (test-hash "aaad") (test-hash "aaae")
                      (test-hash "baaa") (test-hash "baab") (test-hash "baac")
                      (test-hash "baad") (test-hash "baae")]
          chain-with-items (reduce cuckoo/add-item-chain chain base-items)]

      (testing "All similar items are stored"
        (doseq [item base-items]
          (is (cuckoo/contains-hash-chain? chain-with-items item)
              (str item " should be found"))))

      (testing "Removing one item doesn't affect similar items"
        (let [item-to-remove (test-hash "aaaa")
              similar-items (remove #{item-to-remove} base-items)
              chain-after-remove (cuckoo/remove-item-chain chain-with-items item-to-remove)]

          (is (not (cuckoo/contains-hash-chain? chain-after-remove item-to-remove))
              "Removed item should not be found")

          (doseq [item similar-items]
            (is (cuckoo/contains-hash-chain? chain-after-remove item)
                (str item " should still be found after removing similar item"))))))))

(deftest edge-cases-test
  (testing "Edge cases for chain operations"
    (testing "Empty chain operations"
      (let [empty-chain (cuckoo/create-filter-chain)]
        (is (not (cuckoo/contains-hash-chain? empty-chain (test-hash "anything"))))

        (let [after-remove (cuckoo/remove-item-chain empty-chain (test-hash "anything"))]
          (is (= (:version after-remove) (:version empty-chain))
              "Version should be unchanged")
          (is (= (count (:filters after-remove)) (count (:filters empty-chain)))
              "Filter count should be unchanged"))))

    (testing "Single item chain"
      (let [chain (cuckoo/create-filter-chain)
            single-chain (cuckoo/add-item-chain chain (test-hash "only-item"))]
        (is (cuckoo/contains-hash-chain? single-chain (test-hash "only-item")))

        (let [empty-again (cuckoo/remove-item-chain single-chain (test-hash "only-item"))]
          (is (or (empty? (:filters empty-again))
                  (zero? (-> empty-again :filters first :count)))
              "Chain should be empty after removing only item"))))))

(deftest ^:integration cuckoo-filter-file-integration-test
  (testing "Cuckoo filter file is created with index and correctly tracks segments"
    (with-temp-dir [storage-path {}]
      (let [storage-str (str storage-path)
            ;; Create connection with file storage and low indexing threshold
            conn @(fluree/connect-file {:storage-path storage-str
                                        :defaults {:indexing {:reindex-min-bytes 100
                                                              :indexing-disabled false}}})
            _    @(fluree/create conn "cuckoo-test")]

        (testing "Initial state - cuckoo filter should exist even for empty ledger"
          (let [filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.cbor")]
            (is (.exists filter-path) "Cuckoo filter file should exist after ledger creation")))

        (testing "After adding data and triggering index"
          ;; Add some data to trigger indexing
          (let [_ @(fluree/insert! conn "cuckoo-test"
                                   [{"@context" {"ex" "http://example.org/"}
                                     "@id"      "ex:alice"
                                     "@type"    "ex:Person"
                                     "ex:name"  "Alice"}
                                    {"@id"      "ex:bob"
                                     "@type"    "ex:Person"
                                     "ex:name"  "Bob"}])
                ;; Trigger manual indexing to ensure index files are created
                _ @(fluree/trigger-index conn "cuckoo-test" {:block? true})

                ;; List all index segment files
                index-dir (io/file storage-str "cuckoo-test" "index")
                index-files (when (.exists index-dir)
                              (->> (file-seq index-dir)
                                   (filter #(.isFile ^java.io.File %))
                                   (filter #(str/ends-with? (.getName ^java.io.File %) ".json"))
                                   ;; Exclude cuckoo filter directory
                                   (remove #(str/includes? (.getPath ^java.io.File %) "/cuckoo/"))
                                   (map #(.getName ^java.io.File %))
                                   (remove #(str/includes? % "root"))  ; Exclude root files
                                   (remove #(str/includes? % "commit")) ; Exclude commit files
                                   vec))

                ;; Load the cuckoo filter to check contents
                filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.cbor")
                filter-data (when (.exists filter-path)
                              (with-open [fis (FileInputStream. filter-path)]
                                (let [cbor-bytes (.readAllBytes fis)]
                                  (-> cbor-bytes
                                      cbor/decode
                                      cuckoo/deserialize))))]

            (is (seq index-files) "Index files should have been created")
            (is filter-data "Cuckoo filter should be readable")

            (when (and (seq index-files) filter-data)
              (testing "Filter contains actual index segment addresses"
                ;; Check that each index file is in the cuckoo filter
                ;; Using the file name as the address (simplified check)
                (doseq [file-name index-files]
                  (is (cuckoo/contains-hash-chain? filter-data file-name)
                      (str "Cuckoo filter should contain index file: " file-name))))

              (testing "Filter correctly rejects non-existent segments"
                ;; Use valid base32 hashes that aren't in the filter
                (let [fake-segments [(str (test-hash "fake-segment-1") ".json")
                                     (str (test-hash "nonexistent-2") ".json")
                                     (str (test-hash "imaginary-3") ".json")]]
                  (doseq [fake-seg fake-segments]
                    (is (not (cuckoo/contains-hash-chain? filter-data fake-seg))
                        (str "Cuckoo filter should not contain fake segment: " fake-seg))))))))

        (testing "After creating a branch"
          (let [;; Create a new branch with full spec
                _ @(fluree/create-branch! conn "cuckoo-test:feature" "cuckoo-test:main")

                ;; Check that branch has its own cuckoo filter
                branch-filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "feature.cbor")]

            (is (.exists branch-filter-path)
                "Branch should have its own cuckoo filter file")

            (when (.exists branch-filter-path)
              (let [main-filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.cbor")
                    main-filter (with-open [fis (FileInputStream. main-filter-path)]
                                  (let [cbor-bytes (.readAllBytes fis)]
                                    (-> cbor-bytes
                                        cbor/decode
                                        cuckoo/deserialize)))
                    branch-filter (with-open [fis (FileInputStream. branch-filter-path)]
                                    (let [cbor-bytes (.readAllBytes fis)]
                                      (-> cbor-bytes
                                          cbor/decode
                                          cuckoo/deserialize)))]

                ;; Branch filter should start as a copy of main's filter
                (is (= (-> main-filter cuckoo/get-chain-stats :total-count)
                       (-> branch-filter cuckoo/get-chain-stats :total-count))
                    "Branch filter should have same count as main filter initially")))))))))