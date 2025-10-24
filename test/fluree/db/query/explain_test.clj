(ns fluree.db.query.explain-test
  "Tests for query explain API"
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration explain-no-optimization-test
  (testing "Explain API with equal selectivity patterns (no reordering)"
    (let [conn      @(fluree/connect-memory {:defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
          ledger-id "test/explain"
          db0       @(fluree/create conn ledger-id)

          ;; Insert test data
          txn       {"@context" {"ex" "http://example.org/"}
                     "insert" [{"@id" "ex:alice"
                                "@type" "ex:Person"
                                "ex:name" "Alice"
                                "ex:age" 30}
                               {"@id" "ex:bob"
                                "@type" "ex:Person"
                                "ex:name" "Bob"
                                "ex:age" 25}]}
          db1       @(fluree/update db0 txn)

          index-ch  (async/chan 10)
          _         @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _         (<!! (test-utils/block-until-index-complete index-ch))

          db        @(fluree/db conn ledger-id)

          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person" "?name"]
                     :where   [{"@id" "?person"
                                "@type" "ex:Person"}
                               {"@id" "?person"
                                "ex:name" "?name"}]}

          plan      @(fluree/explain db query-map)]

      ;; Test the complete explain output with user-readable patterns
      (is (= {:optimization :unchanged
              :changed? false
              :statistics {:property-counts 12  ;; rdf:type + ex:name + ex:age for 2 entities (6 each)
                           :class-counts 1      ;; ex:Person
                           :total-flakes 16     ;; total flakes in index
                           :indexed-at-t 1}
              :original [{:type :class
                          :pattern {:subject "?person"
                                    :property "@type"
                                    :object "ex:Person"}
                          :selectivity 2        ;; 2 Person instances
                          :optimizable :class}
                         {:type :tuple
                          :pattern {:subject "?person"
                                    :property "ex:name"
                                    :object "?name"}
                          :selectivity 2        ;; ex:name property has 2 values
                          :optimizable :tuple}]
              :optimized [{:type :class
                           :pattern {:subject "?person"
                                     :property "@type"
                                     :object "ex:Person"}
                           :selectivity 2
                           :optimizable :class}
                          {:type :tuple
                           :pattern {:subject "?person"
                                     :property "ex:name"
                                     :object "?name"}
                           :selectivity 2
                           :optimizable :tuple}]}
             (-> (:plan plan)
                 (select-keys [:optimization :changed? :statistics :original :optimized])))
          "Explain should not reorder when both patterns have equal selectivity (2 = 2)"))))

(deftest ^:integration explain-value-lookup-optimization-test
  (testing "Explain API reorders based on specific value lookup (selectivity 0)"
    (let [conn      @(fluree/connect-memory {:defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
          ledger-id "test/optimize"
          db0       @(fluree/create conn ledger-id)

          ;; Insert data: 100 people but only 2 with email "rare@example.org"
          txn       {"@context" {"ex" "http://example.org/"}
                     "insert" (into [{"@id" "ex:alice"
                                      "@type" "ex:Person"
                                      "ex:name" "Alice"
                                      "ex:email" "rare@example.org"}
                                     {"@id" "ex:bob"
                                      "@type" "ex:Person"
                                      "ex:name" "Bob"
                                      "ex:email" "rare@example.org"}]
                                    (for [i (range 2 100)]
                                      {"@id" (str "ex:person" i)
                                       "@type" "ex:Person"
                                       "ex:name" (str "Person" i)
                                       "ex:email" (str "person" i "@example.org")}))}
          db1       @(fluree/update db0 txn)

          index-ch  (async/chan 10)
          _         @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _         (<!! (test-utils/block-until-index-complete index-ch))

          db        @(fluree/db conn ledger-id)

          ;; Query: Find people of type Person with a specific email
          ;; Original order: class pattern first (100 results), then email filter (2 results)
          ;; Optimized order: email filter first (2 results), then class check
          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person"]
                     :where   [{"@id" "?person"
                                "@type" "ex:Person"}
                               {"@id" "?person"
                                "ex:email" "rare@example.org"}]}

          plan      @(fluree/explain db query-map)]

      ;; Test complete deterministic output showing patterns were reordered
      (is (= {:optimization :reordered
              :changed? true
              :statistics {:property-counts 12  ;; rdf:type + ex:name + ex:email for 100 entities
                           :class-counts 1      ;; ex:Person
                           :total-flakes 310    ;; 100 entities × 3 properties + 10 extra flakes
                           :indexed-at-t 1}
              :original [{:type :class
                          :pattern {:subject "?person"
                                    :property "@type"
                                    :object "ex:Person"}
                          :selectivity 100      ;; 100 Person instances
                          :optimizable :class}
                         {:type :tuple
                          :pattern {:subject "?person"
                                    :property "ex:email"
                                    :object "rare@example.org"}
                          :selectivity 0        ;; Specific value lookup (most selective)
                          :optimizable :tuple}]
              ;; Optimized order: email first (0), then class (100)
              :optimized [{:type :tuple
                           :pattern {:subject "?person"
                                     :property "ex:email"
                                     :object "rare@example.org"}
                           :selectivity 0
                           :optimizable :tuple}
                          {:type :class
                           :pattern {:subject "?person"
                                     :property "@type"
                                     :object "ex:Person"}
                           :selectivity 100
                           :optimizable :class}]}
             (-> (:plan plan)
                 (select-keys [:optimization :changed? :statistics :original :optimized])))
          "Explain should reorder patterns from [class, email] to [email, class] based on selectivity (0 < 100)"))))

(deftest ^:integration explain-property-count-optimization-test
  (testing "Explain API reorders based on property counts (property scan vs class scan)"
    (let [conn      @(fluree/connect-memory {:defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
          ledger-id "test/property-opt"
          db0       @(fluree/create conn ledger-id)

          ;; Insert data: 50 Person entities, but only 5 have an "ex:badge" property
          txn       {"@context" {"ex" "http://example.org/"}
                     "insert" (concat
                               ;; 5 people with badges
                               (for [i (range 5)]
                                 {"@id" (str "ex:person" i)
                                  "@type" "ex:Person"
                                  "ex:name" (str "Person" i)
                                  "ex:badge" (str "Badge" i)})
                               ;; 45 people without badges
                               (for [i (range 5 50)]
                                 {"@id" (str "ex:person" i)
                                  "@type" "ex:Person"
                                  "ex:name" (str "Person" i)}))}
          db1       @(fluree/update db0 txn)

          index-ch  (async/chan 10)
          _         @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _         (<!! (test-utils/block-until-index-complete index-ch))

          db        @(fluree/db conn ledger-id)

          ;; Query: Find people of type Person who have a badge (any badge)
          ;; Original order: class pattern first (50 results), then badge property (5 results)
          ;; Optimized order: badge property first (5 results), then class check
          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person" "?badge"]
                     :where   [{"@id" "?person"
                                "@type" "ex:Person"}
                               {"@id" "?person"
                                "ex:badge" "?badge"}]}

          plan      @(fluree/explain db query-map)]

      ;; Test complete deterministic output showing property count drove reordering
      (is (= {:optimization :reordered
              :changed? true
              :statistics {:property-counts 12  ;; rdf:type + ex:name + ex:badge for 50 entities
                           :class-counts 1      ;; ex:Person
                           :total-flakes 115    ;; 50 rdf:type + 50 ex:name + 5 ex:badge + metadata
                           :indexed-at-t 1}
              :original [{:type :class
                          :pattern {:subject "?person"
                                    :property "@type"
                                    :object "ex:Person"}
                          :selectivity 50       ;; 50 Person instances
                          :optimizable :class}
                         {:type :tuple
                          :pattern {:subject "?person"
                                    :property "ex:badge"
                                    :object "?badge"}
                          :selectivity 5        ;; Only 5 entities have ex:badge property
                          :optimizable :tuple}]
              ;; Optimized order: badge property first (5), then class (50)
              :optimized [{:type :tuple
                           :pattern {:subject "?person"
                                     :property "ex:badge"
                                     :object "?badge"}
                           :selectivity 5
                           :optimizable :tuple}
                          {:type :class
                           :pattern {:subject "?person"
                                     :property "@type"
                                     :object "ex:Person"}
                           :selectivity 50
                           :optimizable :class}]}
             (-> (:plan plan)
                 (select-keys [:optimization :changed? :statistics :original :optimized])))
          "Explain should reorder patterns from [class, badge] to [badge, class] based on property count (5 < 50)"))))

(deftest ^:integration explain-no-stats-test
  (testing "Explain API without statistics (no indexing)"
    (let [conn      @(fluree/connect-memory)
          _         @(fluree/create conn "no-stats")
          db        @(fluree/db conn "no-stats")

          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person"]
                     :where   [{"@id" "?person" "ex:name" "?name"}]}

          plan      @(fluree/explain db query-map)
          plan-info (get-in plan [:plan])]

      ;; When no statistics available, should return reason and original where clause
      (is (= :none (:optimization plan-info))
          "Should indicate optimization is :none")

      (is (= "No statistics available" (:reason plan-info))
          "Should provide reason explaining why optimization is :none")

      (is (contains? plan :query)
          "Should contain parsed query")

      (is (some? (:where-clause plan-info))
          "Should include original where clause in plan"))))
