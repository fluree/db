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
            [fluree.db.util.json :as json]))

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
    (let [filter (cuckoo/create-filter 100)
          hash1  (test-hash "abc123def456")
          filter' (cuckoo/add-item filter hash1)]
      (is filter')
      (is (= 1 (:count filter')))
      (is (cuckoo/contains-hash? filter' hash1))
      (is (not (cuckoo/contains-hash? filter (test-hash "nonexistent"))))))

  (testing "Batch add items"
    (let [filter  (cuckoo/create-filter 100)
          hashes  [(test-hash "hash1") (test-hash "hash2") (test-hash "hash3")]
          filter' (cuckoo/batch-add filter hashes)]
      (is (= 3 (:count filter')))
      (is (every? #(cuckoo/contains-hash? filter' %) hashes)))))

(deftest remove-item-test
  (testing "Remove existing item"
    (let [filter  (cuckoo/create-filter 100)
          hash1   (test-hash "removeme")
          filter' (-> filter
                      (cuckoo/add-item hash1)
                      (cuckoo/remove-item hash1))]
      (is (= 0 (:count filter')))
      (is (not (cuckoo/contains-hash? filter' hash1)))))

  (testing "Remove non-existent item"
    (let [filter  (cuckoo/create-filter 100)
          filter' (cuckoo/remove-item filter (test-hash "nonexistent"))]
      (is (= filter filter')))))

(deftest serialization-test
  (testing "Serialize and deserialize filter"
    (let [filter    (-> (cuckoo/create-filter 100)
                        (cuckoo/add-item (test-hash "hash1"))
                        (cuckoo/add-item (test-hash "hash2")))
          chain     (cuckoo/single-filter->chain filter)
          serialized (cuckoo/serialize chain)
          restored   (cuckoo/deserialize serialized)]
      ;; Restored is in chain format, get stats from it
      (let [restored-stats (cuckoo/get-chain-stats restored)]
        (is (= (:count filter) (:total-count restored-stats)))
        (is (= (:fingerprint-bits filter) (:fingerprint-bits restored-stats))))
      ;; contains-hash? works with both formats
      (is (cuckoo/contains-hash? restored (test-hash "hash1")))
      (is (cuckoo/contains-hash? restored (test-hash "hash2"))))))

(deftest metrics-test
  (testing "Load factor calculation"
    (let [filter (-> (cuckoo/create-filter 100)
                     (cuckoo/add-item (test-hash "item1"))
                     (cuckoo/add-item (test-hash "item2")))
          load   (cuckoo/load-factor filter)]
      (is (> load 0))
      (is (< load 1))))

  (testing "Filter statistics"
    (let [filter (cuckoo/create-filter 100)
          stats  (cuckoo/filter-stats filter)]
      (is (contains? stats :count))
      (is (contains? stats :capacity))
      (is (contains? stats :load-factor))
      (is (contains? stats :estimated-fpr)))))

(deftest realistic-address-test
  (testing "Works with realistic Fluree index addresses"
    (let [filter (cuckoo/create-filter 1000)
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
          filter' (reduce cuckoo/add-item filter addresses)]
      (is (= 5 (:count filter')))
      (is (every? #(cuckoo/contains-hash? filter' %) addresses))
      (let [not-in-filter-hash (test-hash "notinfilter")]
        (is (not (cuckoo/contains-hash? filter'
                                        (str "fluree:file://ledger/index/spot/" not-in-filter-hash ".json")))))))

  (testing "Filter capacity handling"
    (let [small-filter (cuckoo/create-filter 10)
          ;; Try to add more items than initial capacity
          many-items (map #(test-hash (str "item-" %)) (range 50))
          filter' (reduce (fn [f item]
                            (or (cuckoo/add-item f item) f))
                          small-filter many-items)
          added-count (:count filter')]
      ;; Should be able to add items up to ~95% capacity
      (is (> added-count 8))
      ;; All successfully added items should be found
      (let [added-items (take added-count many-items)]
        (is (every? #(cuckoo/contains-hash? filter' %) added-items))))))

(deftest false-positive-rate-test
  (testing "No false negatives with moderate dataset"
    (let [filter (cuckoo/create-filter 500)
          items  (map #(test-hash (str "item" %)) (range 100))
          filter' (reduce cuckoo/add-item filter items)]
      ;; All added items must be found (no false negatives)
      (is (every? #(cuckoo/contains-hash? filter' %) items))))

  (testing "Acceptable false positive rate"
    (let [filter    (cuckoo/create-filter 500)
          items     (map #(test-hash (str "item" %)) (range 250))
          filter'   (reduce cuckoo/add-item filter items)
          non-items (map #(test-hash (str "nonitem" %)) (range 1000))
          fps       (count (clojure.core/filter #(cuckoo/contains-hash? filter' %) non-items))
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
          (let [filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.json")]
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
                filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.json")
                filter-data (when (.exists filter-path)
                              (-> filter-path
                                  slurp
                                  (json/parse true)
                                  cuckoo/deserialize))]

            (is (seq index-files) "Index files should have been created")
            (is filter-data "Cuckoo filter should be readable")

            (when (and (seq index-files) filter-data)
              (testing "Filter contains actual index segment addresses"
                ;; Check that each index file is in the cuckoo filter
                ;; Using the file name as the address (simplified check)
                (doseq [file-name index-files]
                  (is (cuckoo/contains-hash? filter-data file-name)
                      (str "Cuckoo filter should contain index file: " file-name))))

              (testing "Filter correctly rejects non-existent segments"
                ;; Use valid base32 hashes that aren't in the filter
                (let [fake-segments [(str (test-hash "fake-segment-1") ".json")
                                     (str (test-hash "nonexistent-2") ".json")
                                     (str (test-hash "imaginary-3") ".json")]]
                  (doseq [fake-seg fake-segments]
                    (is (not (cuckoo/contains-hash? filter-data fake-seg))
                        (str "Cuckoo filter should not contain fake segment: " fake-seg))))))))

        (testing "After creating a branch"
          (let [;; Create a new branch with full spec
                _ @(fluree/create-branch! conn "cuckoo-test:feature" "cuckoo-test:main")

                ;; Check that branch has its own cuckoo filter  
                branch-filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "feature.json")]

            (is (.exists branch-filter-path)
                "Branch should have its own cuckoo filter file")

            (when (.exists branch-filter-path)
              (let [main-filter-path (io/file storage-str "cuckoo-test" "index" "cuckoo" "main.json")
                    main-filter (-> main-filter-path slurp cuckoo/deserialize)
                    branch-filter (-> branch-filter-path slurp cuckoo/deserialize)]

                ;; Branch filter should start as a copy of main's filter
                (is (= (:count main-filter) (:count branch-filter))
                    "Branch filter should have same count as main filter initially")))))))))