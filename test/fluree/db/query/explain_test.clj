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

          db        @(fluree/db conn ledger-id)]

      ;; Test the complete explain output with user-readable patterns
      (is (= {:optimizations [:none],
              :original
              [{:pattern
                {:subject "?person", :property "@type", :object "ex:Person"},
                :type :class,
                :selectivity 2}
               {:pattern {:subject "?person", :property "ex:name", :object "?name"},
                :type :triple,
                :selectivity 2}],
              :optimized
              [{:pattern
                {:subject "?person", :property "@type", :object "ex:Person"},
                :type :class,
                :selectivity 2}
               {:pattern {:subject "?person", :property "ex:name", :object "?name"},
                :type :triple,
                :selectivity 2}]}
             (:plan @(fluree/explain db {:context {"ex" "http://example.org/"}
                                         :select ["?person" "?name"]
                                         :where [{"@id" "?person"
                                                  "@type" "ex:Person"}
                                                 {"@id" "?person"
                                                  "ex:name" "?name"}]})))
          "Explain should not reorder when both patterns have equal selectivity (2 = 2)"))))

(deftest ^:integration explain-value-lookup-optimization-test
  (testing "Explain API reorders based on specific value lookup (selectivity 0)"
    (let [conn      @(fluree/connect-memory {:defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
          ledger-id "test/optimize"
          db0       @(fluree/create conn ledger-id)

          ;; Insert data: 100 people but only 2 with email "rare@example.org"
          txn {"@context" {"ex" "http://example.org/"}
               "insert"   (into [{"@id"      "ex:alice"
                                  "@type"    "ex:Person"
                                  "ex:name"  "Alice"
                                  "ex:email" "rare@example.org"}
                                 {"@id"      "ex:bob"
                                  "@type"    "ex:Person"
                                  "ex:name"  "Bob"
                                  "ex:email" "rare@example.org"}]
                                (for [i (range 2 100)]
                                  {"@id"      (str "ex:person" i)
                                   "@type"    "ex:Person"
                                   "ex:name"  (str "Person" i)
                                   "ex:email" (str "person" i "@example.org")}))}
          db1 @(fluree/update db0 txn)

          index-ch (async/chan 10)
          _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _        (<!! (test-utils/block-until-index-complete index-ch))

          db @(fluree/db conn ledger-id)

          ;; Query: Find people of type Person with a specific email
          ;; Original order: class pattern first (100 results), then email filter (2 results)
          ;; Optimized order: email filter first (2 results), then class check
          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person"]
                     :where   [{"@id"   "?person"
                                "@type" "ex:Person"}
                               {"@id"      "?person"
                                "ex:email" "rare@example.org"}]}

          plan @(fluree/explain db query-map)]

      ;; Test complete deterministic output showing patterns were reordered
      (is (= {:optimizations [:statistics],
              :original      [{:pattern     {:subject "?person", :property "@type", :object "ex:Person"},
                               :type        :class,
                               :selectivity 100}
                              {:pattern     {:subject "?person", :property "ex:email", :object "rare@example.org"},
                               :type        :triple,
                               :selectivity 0}],
              :optimized     [{:pattern     {:subject "?person", :property "ex:email", :object "rare@example.org"},
                               :type        :triple,
                               :selectivity 0}
                              {:pattern     {:subject "?person", :property "@type", :object "ex:Person"},
                               :type        :class,
                               :selectivity 100}]
              :statistics    {:properties 12,
                              :classes    1,
                              :flakes     310,
                              :index-t    1,
                              :segments
                              [{:type :optimizable,
                                :patterns [{:pattern
                                            {:subject "?person", :property "@type", :object "ex:Person"},
                                            :type :class,
                                            :selectivity 100}
                                           {:pattern
                                            {:subject "?person", :property "ex:email", :object "rare@example.org"},
                                            :type :triple,
                                            :selectivity 0}]}]}}
             (:plan plan))
          "Explain should reorder patterns from [class, email] to [email, class] based on selectivity (0 < 100)"))))

(deftest ^:integration explain-property-count-optimization-test
  (testing "Explain API reorders based on property counts (property scan vs class scan)"
    (let [conn      @(fluree/connect-memory {:defaults
                                             {:indexing {:reindex-min-bytes 100
                                                         :reindex-max-bytes 10000000}}})
          ledger-id "test/property-opt"
          db0       @(fluree/create conn ledger-id)

          ;; Insert data: 50 Person entities, but only 5 have an "ex:badge" property
          txn {"@context" {"ex" "http://example.org/"}
               "insert"   (concat
                          ;; 5 people with badges
                           (for [i (range 5)]
                             {"@id"      (str "ex:person" i)
                              "@type"    "ex:Person"
                              "ex:name"  (str "Person" i)
                              "ex:badge" (str "Badge" i)})
                          ;; 45 people without badges
                           (for [i (range 5 50)]
                             {"@id"     (str "ex:person" i)
                              "@type"   "ex:Person"
                              "ex:name" (str "Person" i)}))}
          db1 @(fluree/update db0 txn)

          index-ch (async/chan 10)
          _        @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _        (<!! (test-utils/block-until-index-complete index-ch))

          db @(fluree/db conn ledger-id)

          ;; Query: Find people of type Person who have a badge (any badge)
          ;; Original order: class pattern first (50 results), then badge property (5 results)
          ;; Optimized order: badge property first (5 results), then class check
          query-map {:context {"ex" "http://example.org/"}
                     :select  ["?person" "?badge"]
                     :where   [{"@id"   "?person"
                                "@type" "ex:Person"}
                               {"@id"      "?person"
                                "ex:badge" "?badge"}]}

          plan @(fluree/explain db query-map)]

      ;; Test complete deterministic output showing property count drove reordering
      (is (= {:optimizations [:statistics],
              :original      [{:pattern
                               {:subject "?person", :property "@type", :object "ex:Person"},
                               :type        :class,
                               :selectivity 50}
                              {:pattern
                               {:subject "?person", :property "ex:badge", :object "?badge"},
                               :type        :triple,
                               :selectivity 5}],
              :optimized     [{:pattern
                               {:subject "?person", :property "ex:badge", :object "?badge"},
                               :type        :triple,
                               :selectivity 5}
                              {:pattern
                               {:subject "?person", :property "@type", :object "ex:Person"},
                               :type        :class,
                               :selectivity 50}],
              :statistics    {:properties 12,
                              :classes    1,
                              :flakes     115,
                              :index-t    1,
                              :segments
                              [{:type :optimizable,
                                :patterns
                                [{:pattern
                                  {:subject "?person", :property "@type", :object "ex:Person"},
                                  :type        :class,
                                  :selectivity 50}
                                 {:pattern
                                  {:subject "?person", :property "ex:badge", :object "?badge"},
                                  :type        :triple,
                                  :selectivity 5}]}]}}
             (:plan plan))
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
      (is (= {:query {:context {"ex" "http://example.org/"},
                      :select ["?person"],
                      :where [{"@id" "?person", "ex:name" "?name"}]},
              :plan {:optimizations [:none],
                     :original [{:pattern {:subject "?person", :property "ex:name", :object "?name"},
                                 :type :triple,
                                 :selectivity 1000}],
                     :optimized [{:pattern {:subject "?person", :property "ex:name", :object "?name"},
                                  :type :triple,
                                  :selectivity 1000}]}}
             plan))
      (is (= [:none] (:optimizations plan-info))
          "Should indicate optimization is :none")

      (is (contains? plan :query)
          "Should contain parsed query"))))

(defn gen-subj
  [i]
  {"@id" (str "ex:" i)
   "ex:foo" "foo"
   "ex:i" i})

(deftest explain-analyze-test
  (let [conn @(fluree/connect-memory)
        db0  @(fluree/create conn "test-explain")
        db1  @(fluree/update db0 {"@context" {"ex" "http://example.com/"}
                                  "insert"
                                  (mapv (fn [i]
                                          (cond-> (gen-subj i)
                                            (odd? i) (assoc "ex:ref1"
                                                            (cond-> (gen-subj (+ 1000 i))
                                                              (#{0} (mod i 3))
                                                              (assoc "ex:ref2" (gen-subj (+ 2000 i))
                                                                     "ex:ref3" {"@id" (str "ex:" i)})))
                                            (even? i) (assoc
                                                       "ex:bar" "bar"
                                                       "ex:num" [i (inc i) (dec i)])))
                                        (range 1 100 3))})]
    (testing "meta"
      (let [result @(fluree/explain db1
                                    {"@context" {"ex" "http://example.com/"}
                                     "where" [{"@id" "?s" "ex:num" "?num"}
                                              ["optional"
                                               {"@id" "?s" "ex:ref1" "?ref1"}]
                                              ["union"
                                               {"@id" "?s" "ex:foo" "?str"}
                                               {"@id" "?s" "ex:bar" "?str"}]
                                              ["filter" "(> 50 ?num)"]]
                                     "select" ["?s" "?ref1" "?str"]}
                                    {:analyze true})]
        (is (= [:analyze :fuel :plan :policy :query :result :status :time]
               (sort (keys result))))
        (is (= '[{:in 1,
                  :out 48,
                  :binds-in [],
                  :binds-out [?s ?num],
                  :pattern
                  "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/num\"} #:fluree.db.query.exec.where{:var ?num}]]"}
                 {:in 48,
                  :out 48,
                  :binds-in [?s ?num],
                  :binds-out [?s ?num ?ref1],
                  :pattern
                  "[:optional [[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/ref1\"} #:fluree.db.query.exec.where{:var ?ref1}]]]]"}
                 {:in 48,
                  :out 0,
                  :binds-in [?s ?num],
                  :binds-out [],
                  :pattern
                  "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/ref1\"} #:fluree.db.query.exec.where{:var ?ref1}]]"}
                 {:in 48,
                  :out 96,
                  :binds-in [?s ?num ?ref1],
                  :binds-out [?s ?num ?ref1 ?str],
                  :pattern
                  "[:union [[[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/foo\"} #:fluree.db.query.exec.where{:var ?str}]]] [[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/bar\"} #:fluree.db.query.exec.where{:var ?str}]]]]]"}
                 {:in 48,
                  :out 48,
                  :binds-in [?s ?num ?ref1],
                  :binds-out [?s ?num ?ref1 ?str],
                  :pattern
                  "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/foo\"} #:fluree.db.query.exec.where{:var ?str}]]"}
                 {:in 48,
                  :out 48,
                  :binds-in [?s ?num ?ref1],
                  :binds-out [?s ?num ?ref1 ?str],
                  :pattern
                  "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/bar\"} #:fluree.db.query.exec.where{:var ?str}]]"}
                 {:in 96,
                  :out 48,
                  :binds-in [?s ?num ?ref1 ?str],
                  :binds-out [?s ?num ?ref1 ?str],
                  :pattern "[:filter #function[clojure.lang.AFunction/1]]"}]
               (->> result :analyze
                    (reduce (fn [det-result curr]
                              ;; execution order of the union clauses is nondeterministic, make them deterministic
                              (let [prev (peek det-result)]
                                ;; bar always before foo
                                (if (and (= (:pattern prev) "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/bar\"} #:fluree.db.query.exec.where{:var ?str}]]")
                                         (= (:pattern curr) "[:triple [#:fluree.db.query.exec.where{:var ?s} #:fluree.db.query.exec.where{:iri \"http://example.com/foo\"} #:fluree.db.query.exec.where{:var ?str}]]"))
                                  (into (pop det-result) [curr prev])
                                  (conj det-result curr))))
                            []))))))))
