(ns fluree.db.indexer.cuckoo-roundtrip-test
  "Tests verifying cuckoo filter storage layer: CBOR serialization, disk I/O, and hash normalization.

  This file focuses on the persistence and storage aspects of cuckoo filters, ensuring:
  - Filters can be written to and read from disk without data loss
  - CBOR encoding/decoding preserves all filter data
  - Hash normalization works correctly across different address formats
  - Multiple write-read cycles maintain filter integrity"
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.core.async :refer [<!!]]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.util.cbor :as cbor])
  (:import (java.io FileInputStream)))

(defn- read-filter-from-disk
  "Directly reads and decodes a cuckoo filter file from disk."
  [storage-path ledger branch]
  (let [filter-path (io/file storage-path ledger "index" "cuckoo" (str branch ".cbor"))]
    (when (.exists filter-path)
      (with-open [fis (FileInputStream. filter-path)]
        (let [cbor-bytes (.readAllBytes fis)]
          (cuckoo/deserialize (cbor/decode cbor-bytes)))))))

(deftest cuckoo-filter-roundtrip-test
  (testing "Cuckoo filter round-trips correctly through disk serialization"
    (with-temp-dir [storage-path {}]
      (let [storage-str (str storage-path)
            storage (file-storage/open storage-str)
            index-catalog {:storage storage}
            ledger-id "test-ledger"
            branch-name "main"

            ;; Create filter with known contents
            original-filter (cuckoo/create-filter-chain)
            test-hashes ["beyc5cjwueyz5fbuwlpvehpgvy33cbawx5kvsb3ife6obdfevqrz"
                         "bu5cke44qwx7xkfndq2dpxm4e3y3ohz5s42czasikkqrwzpice5z"
                         "bykti63xio62lhycjy44x75a3hu5xigsdks5npyk6btazgjqivfm"]
            filter-with-data (cuckoo/batch-add-chain original-filter test-hashes)
            original-stats (cuckoo/get-chain-stats filter-with-data)
            test-t 42]

        (testing "Write filter to disk"
          (<!! (cuckoo/write-filter index-catalog ledger-id branch-name test-t filter-with-data))

          (let [filter-path (io/file storage-str ledger-id "index" "cuckoo" (str branch-name ".cbor"))]
            (is (.exists filter-path) "Filter file should exist on disk")
            (is (> (.length filter-path) 0) "Filter file should have content")))

        (testing "Read filter from disk and verify complete round-trip"
          (let [loaded-filter (read-filter-from-disk storage-str ledger-id branch-name)]
            (is (some? loaded-filter) "Should successfully load filter from disk")

            (testing "Filter structure is preserved"
              (let [loaded-stats (cuckoo/get-chain-stats loaded-filter)]
                (is (= (:version original-stats) (:version loaded-stats))
                    "Version should match")
                (is (= (:total-count original-stats) (:total-count loaded-stats))
                    "Total count should match")
                (is (= (:total-buckets original-stats) (:total-buckets loaded-stats))
                    "Total buckets should match")
                (is (= test-t (:t loaded-filter))
                    "Timestamp t should match")))

            (testing "Filter membership is preserved"
              (doseq [hash test-hashes]
                (is (cuckoo/contains-hash-chain? loaded-filter hash)
                    (str "Loaded filter should contain hash: " hash))))

            (testing "Filter rejects unknown hashes"
              (let [unknown-hash "bzunknownhashnotaddedtofilterxxxxxxxxxxxxxxxxx"]
                (is (not (cuckoo/contains-hash-chain? loaded-filter unknown-hash))
                    "Loaded filter should reject unknown hashes")))))

        (testing "Verify CBOR encoding by inspecting raw file"
          (let [filter-path (io/file storage-str ledger-id "index" "cuckoo" (str branch-name ".cbor"))]
            (with-open [fis (FileInputStream. filter-path)]
              (let [cbor-bytes (.readAllBytes fis)
                    decoded-data (cbor/decode cbor-bytes)]
                (is (map? decoded-data) "Should decode to a map")
                (is (contains? decoded-data :version) "Should contain version")
                (is (contains? decoded-data :t) "Should contain timestamp")
                (is (= test-t (:t decoded-data)) "Timestamp should match")
                (is (< (count cbor-bytes) (count (pr-str decoded-data)))
                    "CBOR should be more compact than pr-str")))))

        (testing "Multiple write-read cycles preserve data"
          (let [new-hash "bznewhashtoadd22345abcdefghijklmnopqrstuvwxyza"
                filter-v2 (cuckoo/batch-add-chain filter-with-data [new-hash])]
            (<!! (cuckoo/write-filter index-catalog ledger-id branch-name 43 filter-v2))
            (let [reloaded (read-filter-from-disk storage-str ledger-id branch-name)]
              (is (cuckoo/contains-hash-chain? reloaded new-hash)
                  "Should contain newly added hash after second round-trip")
              (doseq [hash test-hashes]
                (is (cuckoo/contains-hash-chain? reloaded hash)
                    (str "Should still contain original hash: " hash))))))))))

(deftest cuckoo-filter-nonexistent-test
  (testing "Reading non-existent filter returns nil"
    (with-temp-dir [storage-path {}]
      (let [storage-str (str storage-path)
            result (read-filter-from-disk storage-str "nonexistent" "branch")]
        (is (nil? result) "Should return nil for non-existent filter")))))

(deftest hash-extraction-test
  (testing "extract-hash-part normalizes various address formats to same hash"
    (let [hash "bykti63xio62lhycjy44x75a3hu5xigsdks5npyk6btazgjqivfm"]
      (testing "All formats extract to same hash"
        (is (= hash (cuckoo/extract-hash-part (str "fluree:file://ledger/index/spot/" hash ".json")))
            "Full URI with .json")
        (is (= hash (cuckoo/extract-hash-part (str "ledger/index/spot/" hash ".json")))
            "Relative path with .json")
        (is (= hash (cuckoo/extract-hash-part (str hash ".json")))
            "Filename with .json")
        (is (= hash (cuckoo/extract-hash-part hash))
            "Just hash")))))

(deftest hash-normalization-integration-test
  (testing "Hash normalization works end-to-end in cuckoo filter operations"
    (let [chain (cuckoo/create-filter-chain)
          ;; Simulate addresses as they come from indexing (just hashes)
          indexed-segments ["beyc5cjwueyz5fbuwlpvehpgvy33cbawx5kvsb3ife6obdfevqrz"
                            "bu5cke44qwx7xkfndq2dpxm4e3y3ohz5s42czasikkqrwzpice5z"]
          ;; Simulate addresses as they come from garbage collection (full paths)
          gc-addresses ["fluree:file://test/index/spot/beyc5cjwueyz5fbuwlpvehpgvy33cbawx5kvsb3ife6obdfevqrz.json"
                        "fluree:file://test/index/psot/bu5cke44qwx7xkfndq2dpxm4e3y3ohz5s42czasikkqrwzpice5z.json"]
          chain-with-segments (cuckoo/batch-add-chain chain indexed-segments)]

      (testing "Filter contains segments added with just hash"
        (doseq [segment indexed-segments]
          (is (cuckoo/contains-hash-chain? chain-with-segments segment)
              (str "Filter should contain " segment))))

      (testing "Filter finds segments when checked with full GC addresses"
        (doseq [address gc-addresses]
          (is (cuckoo/contains-hash-chain? chain-with-segments address)
              (str "Filter should find segment from GC address " address))))

      (testing "Segments added via one format can be found via another"
        (is (cuckoo/contains-hash-chain? chain-with-segments
                                         (str "fluree:file://different/path/" (first indexed-segments) ".json"))
            "Should find segment regardless of path")))))
