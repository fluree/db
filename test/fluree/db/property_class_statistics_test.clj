(ns fluree.db.property-class-statistics-test
  "Tests for property and class statistics tracking during indexing"
  (:require [babashka.fs :as bfs :refer [with-temp-dir]]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.async-db]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.filesystem :as fs]))

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
          (let [property-counts (get-in loaded [:stats :properties])
                class-counts    (get-in loaded [:stats :classes])
                ;; Helper to get count for a specific IRI (stats use SIDs as keys with nested :count)
                get-count (fn [stats-map iri-str]
                            (let [sid (iri/encode-iri loaded iri-str)]
                              (get-in stats-map [sid :count])))]

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
              (is (map? (:properties stats)) "Serialized stats should have property-counts")
              (is (map? (:classes stats)) "Serialized stats should have class-counts")

              (is (= (count (get-in loaded [:stats :properties]))
                     (count (:properties stats)))
                  "Serialized property counts should have same entry count as reified")
              (is (= (count (get-in loaded [:stats :classes]))
                     (count (:classes stats)))
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
          (let [class-counts (get-in loaded [:stats :classes])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded iri)]
                              (get-in stats-map [sid :count])))]

            (is (map? class-counts) "Class counts should be a map")

            (is (= 2 (get-count class-counts "http://example.org/Person"))
                "Person class should have count 2 after deleting Bob")))

        (testing "Property counts decremented for deleted properties"
          (let [property-counts (get-in loaded [:stats :properties])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded iri)]
                              (get-in stats-map [sid :count])))]

            (is (map? property-counts) "Property counts should be a map")

            (is (= 2 (get-count property-counts "http://example.org/name"))
                "ex:name should have count 2 after deleting Bob")))

        (testing "Counts are clamped at zero for excess retracts"
          ;; Delete Bob's data again (already deleted) - should not go negative
          (let [db-after-idx2 @(fluree/db conn ledger-id)
                txn3    (merge context
                               {"delete" {"@id"   "ex:bob"
                                          "@type" "ex:Person"
                                          "ex:name" "Bob"}})
                db3      @(fluree/update db-after-idx2 txn3)
                index-ch3 (async/chan 10)
                _        @(fluree/commit! conn db3 {:index-files-ch index-ch3})
                _        (<!! (test-utils/block-until-index-complete index-ch3))
                loaded3  (test-utils/retry-load conn ledger-id 100)

                class-counts (get-in loaded3 [:stats :classes])
                property-counts (get-in loaded3 [:stats :properties])
                get-count (fn [stats-map iri]
                            (let [sid (iri/encode-iri loaded3 iri)]
                              (get-in stats-map [sid :count])))]

            (is (= 2 (get-count class-counts "http://example.org/Person"))
                "Person class count should remain at 2 (not negative) after duplicate delete")

            (is (= 2 (get-count property-counts "http://example.org/name"))
                "ex:name count should remain at 2 (not negative) after duplicate delete")))))))

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
        (let [property-counts (get-in indexed-db [:stats :properties])
              class-counts    (get-in indexed-db [:stats :classes])
              get-count (fn [stats-map iri]
                          (let [sid (iri/encode-iri indexed-db iri)]
                            (get-in stats-map [sid :count])))]

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
          (is (some? (get-in info [:stats :size])) "Should have size")
          (is (some? (get-in info [:stats :flakes])) "Should have flakes")
          (is (some? (:commit info)) "Should have commit"))

        (testing "ledger-info includes statistics with decoded IRIs and nested structure"
          (let [properties (get-in info [:stats :properties])
                classes    (get-in info [:stats :classes])]
            (is (map? properties) "Should have properties map")
            (is (map? classes) "Should have classes map")

            ;; Check exact classes - should only have Person
            (is (= ["http://example.org/Person"] (vec (keys classes)))
                "Should have exactly 1 class (Person)")
            (is (= 2 (get-in classes ["http://example.org/Person" :count]))
                "Should have exactly 2 Person entities")

            ;; Check properties include @type and ex:name (plus Fluree internal properties)
            (let [prop-keys (set (keys properties))]
              (is (contains? prop-keys "@type")
                  "Should include @type property")
              (is (contains? prop-keys "http://example.org/name")
                  "Should include ex:name property")
              (is (= 2 (get-in properties ["http://example.org/name" :count]))
                  "Should have exactly 2 ex:name properties"))

            (is (every? string? (keys properties))
                "All property keys should be decoded IRIs (strings)")
            (is (every? string? (keys classes))
                "All class keys should be decoded IRIs (strings)")))))))

(deftest ^:integration stats-serialization-roundtrip-test
  (testing "Statistics can be serialized to file and deserialized correctly"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/stats-roundtrip"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            txn     (merge context
                           {"insert" [{"@id"      "ex:alice"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Alice"
                                       "ex:age"   30}
                                      {"@id"      "ex:bob"
                                       "@type"    "ex:Employee"
                                       "ex:name"  "Bob"
                                       "ex:email" "bob@example.com"}]})
            db1      @(fluree/update db0 txn)

            index-ch (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _        (<!! (test-utils/block-until-index-complete index-ch))]

        (testing "After disconnect and reconnect, stats can be read from index"
          ;; Disconnect to force reload from disk
          @(fluree/disconnect conn)

          ;; Reconnect and load the ledger - this should read stats from the index file
          (let [conn2    @(fluree/connect-file {:storage-path (str storage-path)
                                                :defaults
                                                {:indexing {:reindex-min-bytes 100
                                                            :reindex-max-bytes 10000000}}})
                async-db (test-utils/retry-load conn2 ledger-id 100)
                loaded   (<!! (fluree.db.async-db/deref-async async-db))

                property-counts (get-in loaded [:stats :properties])
                class-counts    (get-in loaded [:stats :classes])

                get-count (fn [stats-map iri-str]
                            (let [sid (iri/encode-iri loaded iri-str)]
                              (get-in stats-map [sid :count])))]

            (is (map? property-counts) "Property counts should be a map after reload")
            (is (map? class-counts) "Class counts should be a map after reload")

            ;; Verify we can actually look up stats by IRI
            (is (= 2 (get-count property-counts "http://example.org/name"))
                "Should be able to retrieve ex:name count from deserialized stats")
            (is (= 1 (get-count property-counts "http://example.org/age"))
                "Should be able to retrieve ex:age count from deserialized stats")
            (is (= 1 (get-count property-counts "http://example.org/email"))
                "Should be able to retrieve ex:email count from deserialized stats")

            (is (= 1 (get-count class-counts "http://example.org/Person"))
                "Should be able to retrieve Person class count from deserialized stats")
            (is (= 1 (get-count class-counts "http://example.org/Employee"))
                "Should be able to retrieve Employee class count from deserialized stats")

            ;; Verify the stats keys are actual SID objects, not strings
            (is (every? iri/sid? (keys property-counts))
                "All property count keys should be SID objects after deserialization")
            (is (every? iri/sid? (keys class-counts))
                "All class count keys should be SID objects after deserialization")

            @(fluree/disconnect conn2)))))))

(deftest ^:integration ndv-computation-test
  (testing "NDV (Number of Distinct Values) is computed via HLL and persisted"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/ndv"
            context {"@context" {"ex" "http://example.org/"}}
            db0     @(fluree/create conn ledger-id)

            ;; Insert data with varying cardinalities
            ;; - ex:email: 100 distinct values (unique per person)
            ;; - ex:department: 3 distinct values (low cardinality)
            ;; - ex:name: 100 distinct values
            txn     (merge context
                           {"insert" (into []
                                           (for [i (range 100)]
                                             {"@id"         (str "ex:person" i)
                                              "@type"       "ex:Person"
                                              "ex:name"     (str "Person" i)
                                              "ex:email"    (str "person" i "@example.org")
                                              "ex:department" (condp = (mod i 3)
                                                                0 "Engineering"
                                                                1 "Sales"
                                                                2 "Marketing")}))})
            db1      @(fluree/update db0 txn)

            index-ch (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _        (<!! (test-utils/block-until-index-complete index-ch))

            loaded   (test-utils/retry-load conn ledger-id 100)]

        (testing "Properties have NDV values computed"
          (let [properties (get-in loaded [:stats :properties])
                get-ndv (fn [prop-iri]
                          (let [sid (iri/encode-iri loaded prop-iri)]
                            (get-in properties [sid :ndv-values])))
                get-ndv-subjects (fn [prop-iri]
                                   (let [sid (iri/encode-iri loaded prop-iri)]
                                     (get-in properties [sid :ndv-subjects])))]

            (is (some? properties) "Properties map should exist")

            ;; Check NDV(values|p) - distinct object values
            (let [email-ndv (get-ndv "http://example.org/email")
                  dept-ndv  (get-ndv "http://example.org/department")
                  name-ndv  (get-ndv "http://example.org/name")]

              (is (some? email-ndv) "ex:email should have ndv-values")
              (is (some? dept-ndv) "ex:department should have ndv-values")
              (is (some? name-ndv) "ex:name should have ndv-values")

              ;; HLL with p=8 has ~6.5% error, so allow 10% tolerance
              (is (< 90 email-ndv 110)
                  (str "ex:email should have ~100 distinct values, got " email-ndv))
              (is (< 2 dept-ndv 5)
                  (str "ex:department should have ~3 distinct values, got " dept-ndv))
              (is (< 90 name-ndv 110)
                  (str "ex:name should have ~100 distinct values, got " name-ndv)))

            ;; Check NDV(subjects|p) - distinct subjects per property
            (let [email-ndv-subj (get-ndv-subjects "http://example.org/email")
                  dept-ndv-subj  (get-ndv-subjects "http://example.org/department")]

              (is (some? email-ndv-subj) "ex:email should have ndv-subjects")
              (is (some? dept-ndv-subj) "ex:department should have ndv-subjects")

              ;; All 100 people have each property, so NDV(subjects) should be ~100
              (is (< 90 email-ndv-subj 110)
                  (str "ex:email should have ~100 distinct subjects, got " email-ndv-subj))
              (is (< 90 dept-ndv-subj 110)
                  (str "ex:department should have ~100 distinct subjects, got " dept-ndv-subj)))))

        (testing "Computed selectivity fields are added to properties"
          (let [properties (get-in loaded [:stats :properties])
                get-prop-data (fn [prop-iri]
                                (let [sid (iri/encode-iri loaded prop-iri)]
                                  (get properties sid)))]

            (is (map? properties) "Properties should be a map")

            ;; Test ex:email (100 unique emails, 100 subjects)
            (let [email-prop (get-prop-data "http://example.org/email")]
              (is (some? email-prop) "ex:email should have property data")
              (is (= 100 (:count email-prop)) "Should have exactly 100 email property instances")
              ;; HLL has ~6.5% error at p=8, allow 10% tolerance for NDV estimates
              (is (< 90 (:ndv-values email-prop) 110)
                  (str "ex:email should have ~100 distinct values (HLL estimate), got " (:ndv-values email-prop)))
              (is (< 90 (:ndv-subjects email-prop) 110)
                  (str "ex:email should have ~100 distinct subjects (HLL estimate), got " (:ndv-subjects email-prop)))
              ;; Selectivity = ceil(count/ndv), clamped to at least 1
              ;; For email with HLL variance: ceil(100/90-110) = 1-2
              (is (<= 1 (:selectivity-value email-prop) 2)
                  (str "ex:email selectivity-value should be 1-2 (highly selective), got " (:selectivity-value email-prop)))
              (is (<= 1 (:selectivity-subject email-prop) 2)
                  (str "ex:email selectivity-subject should be 1-2 (highly selective), got " (:selectivity-subject email-prop))))

            ;; Test ex:department (3 distinct values, 100 subjects)
            (let [dept-prop (get-prop-data "http://example.org/department")]
              (is (some? dept-prop) "ex:department should have property data")
              (is (= 100 (:count dept-prop)) "Should have exactly 100 department property instances")
              (is (< 2 (:ndv-values dept-prop) 5)
                  (str "ex:department should have ~3 distinct values (HLL estimate), got " (:ndv-values dept-prop)))
              (is (< 90 (:ndv-subjects dept-prop) 110)
                  (str "ex:department should have ~100 distinct subjects (HLL estimate), got " (:ndv-subjects dept-prop)))
              ;; For department with HLL variance: ceil(100/2-5) = 20-50
              (is (< 20 (:selectivity-value dept-prop) 50)
                  (str "ex:department selectivity-value should be 20-50 (low cardinality), got " (:selectivity-value dept-prop)))
              (is (<= 1 (:selectivity-subject dept-prop) 2)
                  (str "ex:department selectivity-subject should be 1-2, got " (:selectivity-subject dept-prop))))))

        (testing "NDV values are monotone across index cycles with new connection"
          ;; Disconnect first connection to ensure clean state
          @(fluree/disconnect conn)

          ;; Create a new connection to ensure we're not using cached data
          (let [conn2   @(fluree/connect-file {:storage-path (str storage-path)
                                               :defaults
                                               {:indexing {:reindex-min-bytes 100
                                                           :reindex-max-bytes 10000000}}})
                async-db @(fluree/load conn2 ledger-id)
                db-after-idx1 (<!! (fluree.db.async-db/deref-async async-db))
                get-ndv  (fn [db prop-iri]
                           (let [sid (iri/encode-iri db prop-iri)
                                 ndv (get-in db [:stats :properties sid :ndv-values])]
                             ndv))

                ;; Capture NDV values before adding more data
                email-ndv-before (get-ndv db-after-idx1 "http://example.org/email")
                dept-ndv-before  (get-ndv db-after-idx1 "http://example.org/department")
                ;; Add more data with overlapping values (duplicates)
                txn2    (merge context
                               {"insert" [{"@id"         "ex:person200"
                                           "@type"       "ex:Person"
                                           "ex:name"     "Person200"
                                           "ex:email"    "person0@example.org"  ;; Duplicate email
                                           "ex:department" "Engineering"}]})      ;; Duplicate dept
                db2      @(fluree/update db-after-idx1 txn2)
                index-ch2 (async/chan 10)
                _        @(fluree/commit! conn2 db2 {:index-files-ch index-ch2})
                _        (<!! (test-utils/block-until-index-complete index-ch2))

                ;; Get the updated db after indexing completes
                ;; NDV should remain approximately the same since we added duplicates
                ;; (monotone property: NDV never decreases, but duplicates don't increase it much)
                loaded2   @(fluree/load conn2 ledger-id)
                email-ndv-after  (get-ndv loaded2 "http://example.org/email")
                dept-ndv-after   (get-ndv loaded2 "http://example.org/department")]
            (is (<= email-ndv-before email-ndv-after)
                "NDV should be monotone (non-decreasing)")
            (is (< (- email-ndv-after email-ndv-before) 5)
                "Adding duplicate email shouldn't increase NDV significantly")

            (is (<= dept-ndv-before dept-ndv-after)
                "NDV should be monotone (non-decreasing)")
            (is (< (- dept-ndv-after dept-ndv-before) 2)
                "Adding duplicate department shouldn't increase NDV significantly")

            @(fluree/disconnect conn2)))

        (testing "NDV increases when new distinct values are added, verified with fresh connection"
          ;; Create yet another new connection (conn3) to add new distinct values
          (let [conn3 @(fluree/connect-file {:storage-path (str storage-path)
                                             :defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
                async-db3 @(fluree/load conn3 ledger-id)
                loaded-before (<!! (fluree.db.async-db/deref-async async-db3))
                get-ndv (fn [db prop-iri]
                          (let [sid (iri/encode-iri db prop-iri)]
                            (get-in db [:stats :properties sid :ndv-values])))

                ;; Capture current NDV values (should be ~100 emails, ~3 departments)
                email-ndv-before (get-ndv loaded-before "http://example.org/email")
                dept-ndv-before  (get-ndv loaded-before "http://example.org/department")

                ;; Add data with NEW distinct values (not duplicates)
                txn3 (merge context
                            {"insert" [{"@id"         "ex:person300"
                                        "@type"       "ex:Person"
                                        "ex:name"     "Person300"
                                        "ex:email"    "person300@example.org"  ;; NEW email
                                        "ex:department" "Operations"}]})        ;; NEW department
                db3      @(fluree/update loaded-before txn3)
                index-ch3 (async/chan 10)
                _        @(fluree/commit! conn3 db3 {:index-files-ch index-ch3})
                _        (<!! (test-utils/block-until-index-complete index-ch3))]

            @(fluree/disconnect conn3)

            ;; Create conn4 and load to verify NDV increased
            (let [conn4 @(fluree/connect-file {:storage-path (str storage-path)
                                               :defaults
                                               {:indexing {:reindex-min-bytes 100
                                                           :reindex-max-bytes 10000000}}})
                  async-db4 @(fluree/load conn4 ledger-id)
                  loaded-after (<!! (fluree.db.async-db/deref-async async-db4))
                  get-ndv (fn [db prop-iri]
                            (let [sid (iri/encode-iri db prop-iri)]
                              (get-in db [:stats :properties sid :ndv-values])))

                  ;; Should now have ~101 emails and ~4 departments
                  email-ndv-after (get-ndv loaded-after "http://example.org/email")
                  dept-ndv-after  (get-ndv loaded-after "http://example.org/department")]

              (is (some? email-ndv-before) "Should have email NDV before")
              (is (some? email-ndv-after) "Should have email NDV after")
              (is (some? dept-ndv-before) "Should have dept NDV before")
              (is (some? dept-ndv-after) "Should have dept NDV after")

              ;; NDV should increase by approximately 1 for each (allowing HLL variance)
              (is (< 95 email-ndv-after 115)
                  (str "ex:email should have ~101 distinct values after adding one, got " email-ndv-after))
              (is (< 3 dept-ndv-after 6)
                  (str "ex:department should have ~4 distinct values after adding one, got " dept-ndv-after))

              ;; Verify it actually increased from before
              (is (>= email-ndv-after email-ndv-before)
                  "Email NDV should not decrease")
              (is (>= dept-ndv-after dept-ndv-before)
                  "Department NDV should not decrease")

              @(fluree/disconnect conn4))))))))

(deftest ^:integration last-modified-t-sketch-persistence-test
  (testing "Sketch files are persisted with :last-modified-t and managed correctly across indexes"
    (with-temp-dir [storage-path {}]
      (let [storage-path-str (str storage-path)
            ledger-id "test/sketch-persist"]

        (testing "Phase 1: Initial index creates sketch files with t=1"
          (let [conn1   @(fluree/connect-file {:storage-path storage-path-str
                                               :defaults
                                               {:indexing {:reindex-min-bytes 100
                                                           :reindex-max-bytes 10000000}}})
                context {"@context" {"ex" "http://example.org/"}}
                db0     @(fluree/create conn1 ledger-id)

                ;; Insert data with three properties: name, email, department
                txn1    (merge context
                               {"insert" (into []
                                               (for [i (range 10)]
                                                 {"@id" (str "ex:person" i)
                                                  "@type" "ex:Person"
                                                  "ex:name" (str "Person" i)
                                                  "ex:email" (str "person" i "@example.org")
                                                  "ex:department" (if (< i 5) "Engineering" "Sales")}))})
                db1      @(fluree/update db0 txn1)

                index-ch1 (async/chan 10)
                _         @(fluree/commit! conn1 db1 {:index-files-ch index-ch1})
                _         (<!! (test-utils/block-until-index-complete index-ch1))

                ;; Reload to get db with indexed stats
                loaded1   (test-utils/retry-load conn1 ledger-id 100)

                ;; Get SIDs for our properties
                name-sid  (iri/encode-iri loaded1 "http://example.org/name")
                email-sid (iri/encode-iri loaded1 "http://example.org/email")
                dept-sid  (iri/encode-iri loaded1 "http://example.org/department")]

            (testing "Properties have :last-modified-t = 1"
              (is (= 1 (get-in loaded1 [:stats :properties name-sid :last-modified-t])))
              (is (= 1 (get-in loaded1 [:stats :properties email-sid :last-modified-t])))
              (is (= 1 (get-in loaded1 [:stats :properties dept-sid :last-modified-t]))))

            (testing "Sketch files exist on disk with t=1"
              (let [sketch-dir (str storage-path-str "/" ledger-id "/index/stats-sketches")
                    values-files (<!! (fs/list-files (str sketch-dir "/values")))
                    subjects-files (<!! (fs/list-files (str sketch-dir "/subjects")))]

                ;; Each property should have values and subjects sketch files with _1.hll suffix
                (is (some #(re-find #"name_1\.hll$" %) values-files)
                    "name values sketch should exist at t=1")
                (is (some #(re-find #"email_1\.hll$" %) values-files)
                    "email values sketch should exist at t=1")
                (is (some #(re-find #"department_1\.hll$" %) values-files)
                    "department values sketch should exist at t=1")

                (is (some #(re-find #"name_1\.hll$" %) subjects-files)
                    "name subjects sketch should exist at t=1")
                (is (some #(re-find #"email_1\.hll$" %) subjects-files)
                    "email subjects sketch should exist at t=1")
                (is (some #(re-find #"department_1\.hll$" %) subjects-files)
                    "department subjects sketch should exist at t=1")))))

        (testing "Phase 2: Second index updates only modified properties"
          (let [conn2   @(fluree/connect-file {:storage-path storage-path-str
                                               :defaults
                                               {:indexing {:reindex-min-bytes 100
                                                           :reindex-max-bytes 10000000}}})
                context {"@context" {"ex" "http://example.org/"}}
                ;; Load from new connection
                async-db1 @(fluree/db conn2 ledger-id)
                loaded1   (<!! (fluree.db.async-db/deref-async async-db1))

                ;; Get SIDs for verification
                name-sid  (iri/encode-iri loaded1 "http://example.org/name")
                email-sid (iri/encode-iri loaded1 "http://example.org/email")
                dept-sid  (iri/encode-iri loaded1 "http://example.org/department")

                ;; Update existing person - modify ONLY email and department, leave name unchanged
                txn2    (merge context
                               {"delete" [{"@id" "ex:person0"
                                           "ex:email" "person0@example.org"
                                           "ex:department" "Engineering"}]
                                "insert" [{"@id" "ex:person0"
                                           "ex:email" "person0.updated@example.org"
                                           "ex:department" "Marketing"}]})
                db2      @(fluree/update loaded1 txn2)

                index-ch2 (async/chan 10)
                _         @(fluree/commit! conn2 db2 {:index-files-ch index-ch2})
                _         (<!! (test-utils/block-until-index-complete index-ch2))

                loaded2   (test-utils/retry-load conn2 ledger-id 100)]

            (testing "Modified properties have :last-modified-t = 2, unchanged keep t = 1"
              ;; Only email and department were modified in txn2, name was NOT modified
              (is (= 1 (get-in loaded2 [:stats :properties name-sid :last-modified-t]))
                  "name should still have t=1 (not modified)")
              (is (= 2 (get-in loaded2 [:stats :properties email-sid :last-modified-t]))
                  "email should have t=2 (was modified)")
              (is (= 2 (get-in loaded2 [:stats :properties dept-sid :last-modified-t]))
                  "department should have t=2 (was modified)"))

            (testing "New sketch files exist with t=2 for modified properties"
              (let [sketch-dir (str storage-path-str "/" ledger-id "/index/stats-sketches")
                    values-files (<!! (fs/list-files (str sketch-dir "/values")))]

                ;; Only email and department were modified, so only they should have t=2 sketches
                (is (not (some #(re-find #"name_2\.hll$" %) values-files))
                    "name should NOT have t=2 sketch (not modified)")
                (is (some #(re-find #"email_2\.hll$" %) values-files)
                    "email should have t=2 sketch (was modified)")
                (is (some #(re-find #"department_2\.hll$" %) values-files)
                    "department should have t=2 sketch (was modified)")

                ;; Unchanged property (name) should still only have t=1 sketch
                (is (some #(re-find #"name_1\.hll$" %) values-files)
                    "name should still have t=1 sketch")

                ;; Modified properties should have BOTH t=1 and t=2 (old not yet garbage collected)
                (is (some #(re-find #"email_1\.hll$" %) values-files)
                    "email should still have t=1 sketch (not yet garbage collected)")
                (is (some #(re-find #"department_1\.hll$" %) values-files)
                    "department should still have t=1 sketch (not yet garbage collected)")))

            (testing "Load index root and garbage from disk, verify correct sketch files in garbage"
              (let [index-catalog (-> conn2 :index-catalog)
                    ;; Get the index root address directly from loaded2 db
                    index-root-address (get-in loaded2 [:commit :index :address])
                    _ (is (some? index-root-address)
                          "Should have index root address from commit")
                    ;; Load the index root from disk to get garbage reference
                    index-root (<!! (index-storage/read-db-root index-catalog index-root-address))
                    _ (is (some? index-root)
                          "Index root should be loadable from disk")

                    ;; Get garbage reference from index root
                    garbage-ref (get-in index-root [:garbage :address])
                    _ (is (some? garbage-ref)
                          "Index root should contain garbage reference")

                    ;; Load garbage data from disk using the reference
                    garbage-data (<!! (index-storage/read-garbage index-catalog garbage-ref))
                    _ (is (some? garbage-data)
                          "Garbage should be loadable from disk using index root reference")]

                (when garbage-data
                  (let [garbage-items (:garbage garbage-data)]
                    ;; (a) Old sketch files for MODIFIED properties should be in garbage
                    (is (some #(re-find #"email_1\.hll$" %) garbage-items)
                        "Garbage should contain old email sketch from t=1 (email was modified)")
                    (is (some #(re-find #"department_1\.hll$" %) garbage-items)
                        "Garbage should contain old department sketch from t=1 (department was modified)")

                    ;; (b) Old sketch files for UNCHANGED properties should NOT be in garbage
                    (is (not (some #(re-find #"name_1\.hll$" %) garbage-items))
                        "Garbage should NOT contain name_1 sketch (name was not modified)")

                    ;; (b cont.) NEW sketch files (t=2) should NOT be in garbage
                    (is (not (some #(re-find #"_2\.hll$" %) garbage-items))
                        "Garbage should NOT contain any t=2 sketch files (they are current)")
                    (is (not (some #(re-find #"email_2\.hll$" %) garbage-items))
                        "Garbage should NOT contain email_2 sketch (it is current)")
                    (is (not (some #(re-find #"department_2\.hll$" %) garbage-items))
                        "Garbage should NOT contain department_2 sketch (it is current)"))))

              @(fluree/disconnect conn2))))))))

(deftest ^:integration class-property-tracking-structure-test
  (testing "Class property tracking captures types, ref-classes, and langs in expected structure"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 100
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "test/class-props"
            context {"@context" {"ex" "http://example.org/"
                                 "schema" "http://schema.org/"}}
            db0     @(fluree/create conn ledger-id)

            ;; Create rich test data with various datatypes, references, and language tags
            txn     (merge context
                           {"insert" [{"@id"      "ex:company1"
                                       "@type"    "ex:Company"
                                       "schema:name" "Acme Corp"}
                                      {"@id"      "ex:alice"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Alice"
                                       "ex:age"   30
                                       "ex:email" "alice@example.com"
                                       "ex:employer" {"@id" "ex:company1"}
                                       "ex:bio"   {"@value" "Software engineer"
                                                   "@language" "en"}}
                                      {"@id"      "ex:bob"
                                       "@type"    "ex:Person"
                                       "ex:name"  "Bob"
                                       "ex:title" {"@value" "Ingénieur"
                                                   "@language" "fr"}
                                       "ex:active" true
                                       "ex:employer" {"@id" "ex:company1"}}
                                      {"@id"      "ex:product1"
                                       "@type"    "ex:Product"
                                       "ex:name"  "Widget"
                                       "ex:price" 19.99
                                       "ex:inStock" true}]})
            db1      @(fluree/update db0 txn)

            index-ch (async/chan 10)
            _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _        (<!! (test-utils/block-until-index-complete index-ch))]

        (testing "Full class property structure matches expected format"
          (let [;; Use ledger-info API which decodes SIDs to IRIs for comparison
                info @(fluree/ledger-info conn ledger-id)
                classes (get-in info [:stats :classes])

                expected-classes
                {"http://example.org/Person"
                 {:count 2
                  :properties
                  {"http://example.org/name"
                   {:types #{"http://www.w3.org/2001/XMLSchema#string"}
                    :ref-classes #{}
                    :langs #{}}

                   "http://example.org/age"
                   {:types #{"http://www.w3.org/2001/XMLSchema#integer"}
                    :ref-classes #{}
                    :langs #{}}

                   "http://example.org/email"
                   {:types #{"http://www.w3.org/2001/XMLSchema#string"}
                    :ref-classes #{}
                    :langs #{}}

                   "http://example.org/employer"
                   {:types #{"@id"}
                    :ref-classes #{"http://example.org/Company"}
                    :langs #{}}

                   "http://example.org/bio"
                   {:types #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"}
                    :ref-classes #{}
                    :langs #{"en"}}

                   "http://example.org/title"
                   {:types #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"}
                    :ref-classes #{}
                    :langs #{"fr"}}

                   "http://example.org/active"
                   {:types #{"http://www.w3.org/2001/XMLSchema#boolean"}
                    :ref-classes #{}
                    :langs #{}}}}

                 "http://example.org/Product"
                 {:count 1
                  :properties
                  {"http://example.org/name"
                   {:types #{"http://www.w3.org/2001/XMLSchema#string"}
                    :ref-classes #{}
                    :langs #{}}

                   "http://example.org/price"
                   {:types #{"http://www.w3.org/2001/XMLSchema#double"}
                    :ref-classes #{}
                    :langs #{}}

                   "http://example.org/inStock"
                   {:types #{"http://www.w3.org/2001/XMLSchema#boolean"}
                    :ref-classes #{}
                    :langs #{}}}}

                 "http://example.org/Company"
                 {:count 1
                  :properties
                  {"http://schema.org/name"
                   {:types #{"http://www.w3.org/2001/XMLSchema#string"}
                    :ref-classes #{}
                    :langs #{}}}}}]

            (is (= expected-classes classes)
                "All class structures should match expected format")))))))
