(ns fluree.db.property-class-statistics-test
  "Tests for property and class statistics tracking during indexing"
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration property-class-statistics-test
  (testing "Property and class statistics are accumulated during indexing"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/stats"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            txn1    (merge context
                           {"insert" [{"@id"      "ex:alice"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Alice"
                                       "ex:age"   30
                                       "ex:email" "alice@example.com"}
                                      {"@id"      "ex:bob"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Bob"
                                       "ex:age"   25
                                       "ex:email" "bob@example.com"}
                                      {"@id"      "ex:acme"
                                       "@type"    "ex:Organization"
                                       "ex:name"  "Acme Corp"
                                       "ex:founded" 1990}]})
            db1      @(fluree/update db0 txn1)

            index-ch   (async/chan 10)
            _db-commit @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _          (<!! (test-utils/block-until-index-complete index-ch))

            ;; Reload to get indexed db
            loaded     (test-utils/retry-load conn ledger-id 100)]

        (testing "Reified database has property and class counts"
          (let [property-counts (get-in loaded [:stats :property-counts])
                class-counts    (get-in loaded [:stats :class-counts])
                ;; Helper to get count for a specific IRI (stats use SIDs as keys)
                get-count (fn [stats-map iri-str]
                            (let [sid (iri/encode-iri loaded iri-str)]
                              (get stats-map sid)))]

            (is (map? property-counts) "Property counts should be a map")
            (is (map? class-counts) "Class counts should be a map")

            (is (= 3 (get-count property-counts "http://example.org/name"))
                "ex:name should have count 3")
            (is (= 2 (get-count property-counts "http://example.org/age"))
                "ex:age should have count 2")
            (is (= 2 (get-count property-counts "http://example.org/email"))
                "ex:email should have count 2")
            (is (= 1 (get-count property-counts "http://example.org/founded"))
                "ex:founded should have count 1")

            (is (= 2 (get-count class-counts "http://example.org/Person"))
                "Person class should have count 2")
            (is (= 1 (get-count class-counts "http://example.org/Organization"))
                "Organization class should have count 1")))

        (testing "Index root file contains serialized statistics"
          (let [index-catalog (-> conn :index-catalog)
                index-address (get-in loaded [:commit :index :address])
                root-data     (<!! (index-storage/read-db-root index-catalog index-address))]

            (is (some? root-data) "Should be able to read index root")

            (let [stats (get root-data :stats)]
              (is (map? (:property-counts stats)) "Serialized stats should have property-counts")
              (is (map? (:class-counts stats)) "Serialized stats should have class-counts")

              (is (= (count (get-in loaded [:stats :property-counts]))
                     (count (:property-counts stats)))
                  "Serialized property counts should have same entry count as reified")
              (is (= (count (get-in loaded [:stats :class-counts]))
                     (count (:class-counts stats)))
                  "Serialized class counts should have same entry count as reified"))))))))

(deftest ^:integration property-class-statistics-with-retracts-test
  (testing "Statistics correctly handle retracts (decrement counts)"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/stats-retracts"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            txn1    (merge context
                           {"insert" [{"@id"      "ex:alice"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Alice"}
                                      {"@id"      "ex:bob"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Bob"}
                                      {"@id"      "ex:carol"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Carol"}]})
            db1      @(fluree/update db0 txn1)
            index-ch1 (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch1})
            _        (<!! (test-utils/block-until-index-complete index-ch1))

            db-after-idx1 @(fluree/db conn ledger-id)
            txn2    (merge context
                           {"delete" {"@id"   "ex:bob"
                                      "@type" "ex:Person"
                                      "ex:name" "Bob"}})
            db2      @(fluree/update db-after-idx1 txn2)
            index-ch2 (async/chan 10)
            _        @(fluree/commit! conn db2 {:index-files-ch index-ch2})
            _        (<!! (test-utils/block-until-index-complete index-ch2))

            loaded   (test-utils/retry-load conn ledger-id 100)]

        (testing "Class count decremented after delete"
          (let [class-counts (get-in loaded [:stats :class-counts])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded iri)]
                              (get stats-map sid)))]

            (is (map? class-counts) "Class counts should be a map")

            (is (= 2 (get-count class-counts "http://example.org/Person"))
                "Person class should have count 2 after deleting Bob")))

        (testing "Property counts decremented for deleted properties"
          (let [property-counts (get-in loaded [:stats :property-counts])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded iri)]
                              (get stats-map sid)))]

            (is (map? property-counts) "Property counts should be a map")

            (is (= 2 (get-count property-counts "http://example.org/name"))
                "ex:name should have count 2 after deleting Bob")))))))

(deftest ^:integration property-class-statistics-memory-storage-test
  (testing "Statistics work with in-memory storage"
    (let [conn    @(fluree/connect-memory {:defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
          ledger-id "test/stats-memory"
          context {"@context" {"ex" "http://example.org/"}}
          db0     @(fluree/create conn ledger-id)

          txn     (merge context
                         {"insert" [{"@id"      "ex:alice"
                                     "@type"    "ex:Person"
                                     "ex:name"  "Alice"
                                     "ex:age"   30}
                                    {"@id"      "ex:product1"
                                     "@type"    "ex:Product"
                                     "ex:name"  "Widget"
                                     "ex:price" 19.99}]})
          db1      @(fluree/update db0 txn)

          index-ch (async/chan 10)
          _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _        (<!! (test-utils/block-until-index-complete index-ch))

          indexed-db @(fluree/db conn ledger-id)]

      (testing "Memory-based index has statistics"
        (let [property-counts (get-in indexed-db [:stats :property-counts])
              class-counts    (get-in indexed-db [:stats :class-counts])
              get-count (fn [stats-map iri]
                          (let [sid (iri/encode-iri indexed-db iri)]
                            (get stats-map sid)))]

          (is (map? property-counts) "Property counts should be a map")
          (is (map? class-counts) "Class counts should be a map")

          (is (= 2 (get-count property-counts "http://example.org/name"))
              "ex:name should have count 2")
          (is (= 1 (get-count property-counts "http://example.org/age"))
              "ex:age should have count 1")
          (is (= 1 (get-count property-counts "http://example.org/price"))
              "ex:price should have count 1")

          (is (= 1 (get-count class-counts "http://example.org/Person"))
              "Person class should have count 1")
          (is (= 1 (get-count class-counts "http://example.org/Product"))
              "Product class should have count 1"))))))

(deftest ^:integration property-class-statistics-zero-counts-test
  (testing "Properties and classes with zero counts are preserved"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/stats-zero"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            txn1    (merge context
                           {"insert" [{"@id"      "ex:temp"
                                       "@type"    "ex:TempClass"
                                       "ex:tempProp" "value"}]})
            db1      @(fluree/update db0 txn1)
            index-ch1 (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch1})
            _        (<!! (test-utils/block-until-index-complete index-ch1))

            db-after-idx1 @(fluree/db conn ledger-id)
            txn2    (merge context
                           {"delete" {"@id"   "ex:temp"
                                      "@type" "ex:TempClass"
                                      "ex:tempProp" "value"}})
            db2      @(fluree/update db-after-idx1 txn2)
            index-ch2 (async/chan 10)
            _        @(fluree/commit! conn db2 {:index-files-ch index-ch2})
            _        (<!! (test-utils/block-until-index-complete index-ch2))

            loaded   (test-utils/retry-load conn ledger-id 100)]

        (testing "Zero-count properties/classes are preserved"
          (let [property-counts (get-in loaded [:stats :property-counts])
                class-counts    (get-in loaded [:stats :class-counts])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded iri)]
                              (get stats-map sid)))]

            (is (= 0 (get-count class-counts "http://example.org/TempClass"))
                "TempClass should have count 0 after deletion")

            (is (= 0 (get-count property-counts "http://example.org/tempProp"))
                "ex:tempProp should have count 0 after deletion")))))))

(deftest ^:integration ledger-info-api-test
  (testing "ledger-info API returns property and class statistics"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/ledger-info"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            txn     (merge context
                           {"insert" [{"@id"      "ex:alice"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Alice"}
                                      {"@id"      "ex:bob"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Bob"}]})
            db1      @(fluree/update db0 txn)
            index-ch (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _        (<!! (test-utils/block-until-index-complete index-ch))

            info     @(fluree/ledger-info conn ledger-id)]

        (testing "ledger-info includes standard fields"
          (is (some? (:address info)) "Should have address")
          (is (some? (:alias info)) "Should have alias")
          (is (some? (:branch info)) "Should have branch")
          (is (some? (:t info)) "Should have t")
          (is (some? (:size info)) "Should have size")
          (is (some? (:flakes info)) "Should have flakes")
          (is (some? (:commit info)) "Should have commit"))

        (testing "ledger-info includes statistics with decoded IRIs"
          (let [prop-counts  (:property-counts info)
                class-counts (:class-counts info)]
            (is (map? prop-counts) "Should have property-counts map")
            (is (map? class-counts) "Should have class-counts map")
            (is (pos? (count prop-counts)) "Should have property counts")
            (is (pos? (count class-counts)) "Should have class counts")

            (is (= 2 (get class-counts "http://example.org/Person"))
                "Should have exactly 2 Person entities")

            (is (= 2 (get prop-counts "http://example.org/name"))
                "Should have exactly 2 ex:name properties")

            (is (every? string? (keys prop-counts))
                "All property keys should be decoded IRIs (strings)")
            (is (every? string? (keys class-counts))
                "All class keys should be decoded IRIs (strings)")))))))