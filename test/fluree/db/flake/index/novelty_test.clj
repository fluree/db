(ns fluree.db.flake.index.novelty-test
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.core.async :as async :refer [<!! timeout]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.async-db :as async-db]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.novelty :as novelty]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.async :refer [<? <?? go-try]]))

(defn collect-leaf-chain
  "Walks the index tree depth-first and collects all leaf nodes in order,
  returning a vector of [leaf-id next-id] pairs to verify the chain."
  [index-catalog root-node]
  (go-try
    (loop [stack  [root-node]
           leaves []]
      (if-let [node (peek stack)]
        (let [resolved-node (<? (index/resolve index-catalog node))]
          (if (index/leaf? resolved-node)
            (let [leaf-info {:id      (:id resolved-node)
                             :next-id (:next-id resolved-node)
                             :first   (:first resolved-node)
                             :rhs     (:rhs resolved-node)}]
              (recur (pop stack)
                     (conj leaves leaf-info)))
            (let [children (->> (:children resolved-node)
                                vals
                                reverse
                                vec)]
              (recur (into (pop stack) children)
                     leaves))))
        leaves))))

(defn verify-leaf-chain
  "Verifies that all leaves form a proper chain via :next-id fields.
  Each leaf's :next-id should match the next leaf's :id."
  [leaves]
  (loop [[leaf & rest-leaves] leaves
         prev-leaf nil]
    (when leaf
      (when prev-leaf
        (when (:next-id prev-leaf)
          (is (= (:next-id prev-leaf) (:id leaf))
              (str "Leaf " (:id prev-leaf) " next-id should point to " (:id leaf)))))
      (recur rest-leaves leaf))))

(deftest ^:integration index-datetimes-test
  (testing "Serialize and reread flakes with time types"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 12
                                                       :reindex-max-bytes 10000000}}})
            ledger-id "index/datetimes"
            context (merge test-utils/default-str-context {"ex" "http://example.org/ns/"})
            db0     @(fluree/create conn ledger-id)
            db      @(fluree/update
                      db0
                      {"@context" context
                       "insert"
                       [{"@id"   "ex:Foo",
                         "@type" "ex:Bar",

                         "ex:offsetDateTime"  {"@type"  "xsd:dateTime"
                                               "@value" "2023-04-01T00:00:00.000Z"}
                         "ex:localDateTime"   {"@type"  "xsd:dateTime"
                                               "@value" "2021-09-24T11:14:32.833"}
                         "ex:offsetDateTime2" {"@type"  "xsd:date"
                                               "@value" "2022-01-05Z"}
                         "ex:localDate"       {"@type"  "xsd:date"
                                               "@value" "2024-02-02"}
                         "ex:offsetTime"      {"@type"  "xsd:time"
                                               "@value" "12:42:00Z"}
                         "ex:localTime"       {"@type"  "xsd:time"
                                               "@value" "12:42:00"}}]})
            index-ch   (async/chan 10)
            _db-commit @(fluree/commit! conn db {:index-files-ch index-ch})
            _          (loop []
                         (when-let [msg (<!! index-ch)]
                           (when-not (= :root (:file-type msg))
                             (recur))))
            _          (<!! (timeout 100))
            loaded     (test-utils/retry-load conn ledger-id 100)
            q          {"@context" context
                        "select"   {"?s" ["*"]}
                        "where"    {"@id" "?s", "type" "ex:Bar"}}]
        (is (= @(fluree/query loaded q)
               @(fluree/query db q)))))))

(deftest byte-based-leaf-split-test
  (testing "Leaf split using byte-based distribution maintains invariants"
    (let [ledger-alias "test-ledger"
          cmp          flake/cmp-flakes-spot
          flakes       (reduce (fn [acc i]
                                 (conj acc (flake/create i 1 (str "value-" i) 1 42 true nil)))
                               (flake/sorted-set-by cmp)
                               (range 1 101))
          leaf         {:ledger-alias ledger-alias
                        :comparator   cmp
                        :id           (random-uuid)
                        :leaf         true
                        :first        (first flakes)
                        :rhs          nil
                        :flakes       flakes
                        :size         (flake/size-bytes flakes)
                        :t            42
                        :leftmost?    true}]
      (binding [novelty/*overflow-bytes* 10000]
        (let [result (novelty/rebalance-leaf leaf)]
          (is (= 3 (count result))
              "100 flakes at 11384 bytes with 10000 overflow should create exactly 3 leaves")

          (let [first-leaf (first result)
                last-leaf  (last result)]

            (is (:leftmost? last-leaf) "Leftmost (last) leaf should preserve leftmost flag")
            (is (= (:first last-leaf) (first (:flakes last-leaf)))
                "Leftmost leaf :first should match first flake")

            (is (nil? (:rhs first-leaf)) "Rightmost (first) leaf should inherit parent's rhs (nil)")
            (is (= (:first first-leaf) (first (:flakes first-leaf)))
                "Rightmost leaf :first should match first flake")

            (doseq [[left-leaf right-leaf] (rest (partition 2 1 (reverse result)))]
              (is (some? (:next-tempid left-leaf))
                  "Each non-leftmost leaf should have :next-tempid")
              (is (= (:next-tempid left-leaf) (:tempid right-leaf))
                  "Each leaf's :next-tempid should match next leaf's :tempid")
              (is (= (:rhs left-leaf) (:first right-leaf))
                  "Each leaf's :rhs should equal next leaf's :first"))

            (let [all-split-flakes (reduce into (map :flakes result))]
              (is (= (count flakes) (count all-split-flakes))
                  "Total flake count should be preserved")
              (is (= flakes all-split-flakes)
                  "All flakes should be preserved in order"))))))))

(deftest median-by-count-branch-split-test
  (testing "Branch split using median-by-count maintains invariants"
    (let [ledger-alias "test-ledger"
          cmp          flake/cmp-flakes-spot
          children     (mapv (fn [i]
                               {:ledger-alias ledger-alias
                                :comparator   cmp
                                :id           (random-uuid)
                                :leaf         true
                                :first        (flake/create (* i 10) 1 "v" 1 42 true nil)
                                :rhs          (when (< i 9)
                                                (flake/create (* (inc i) 10) 1 "v" 1 42 true nil))
                                :size         1000
                                :t            42
                                :leftmost?    (zero? i)})
                             (range 10))
          branch       {:ledger-alias ledger-alias
                        :comparator   cmp
                        :id           (random-uuid)
                        :leaf         false
                        :first        (:first (first children))
                        :rhs          (:rhs (last children))
                        :size         10000
                        :t            42
                        :leftmost?    true}]
      (binding [novelty/*overflow-children* 8]
        (let [[left-branch right-branch :as result] (novelty/rebalance-children branch 43 children)]
          (is (= 2 (count result)))

          (is (:leftmost? left-branch) "Left branch should be leftmost")
          (is (= (:first left-branch) (:first (first children)))
              "Left :first should match first child's :first")

          (is (false? (:leftmost? right-branch)) "Right branch should not be leftmost")
          (is (= (:rhs right-branch) (:rhs branch))
              "Right branch should inherit parent's :rhs")

          (is (= (:rhs left-branch) (:first right-branch))
              "Left :rhs should equal right :first")

          (let [left-children  (-> left-branch :children vals vec)
                right-children (-> right-branch :children vals vec)
                all-children   (into left-children right-children)]
            (is (= (count children) (count all-children))
                "Total child count should be preserved")))))))

(deftest ^:integration skewed-inserts-cascade-test
  (testing "Skewed inserts cause cascading splits to maintain uniform height"
    (with-temp-dir [storage-path {}]
      (let [storage-opts {:storage-path (str storage-path)
                          :defaults
                          {:indexing {:reindex-min-bytes 100
                                      :reindex-max-bytes 10000000}}}
            conn         @(fluree/connect-file storage-opts)
            ledger-id    "test/cascade"
            context      {"ex" "http://example.org/ns/"}
            db0          @(fluree/create conn ledger-id)
            items        (mapv (fn [i]
                                 {"@id"      (str "ex:item" i)
                                  "@type"    "ex:Item"
                                  "ex:name"  (str "Item " i)
                                  "ex:index" i})
                               (range 1 200))
            db           @(fluree/update db0
                                         {"@context" context
                                          "insert"   items})
            index-ch     (async/chan 10)
            _db-commit   @(fluree/commit! conn db {:index-files-ch index-ch})
            _            (loop []
                           (when-let [msg (<!! index-ch)]
                             (when-not (= :root (:file-type msg))
                               (recur))))
            _            (<!! (timeout 100))
            conn-fresh   @(fluree/connect-file storage-opts)
            loaded       @(fluree/load conn-fresh ledger-id)]
        (is (nil? (:novelty loaded))
            "Loaded db should have no novelty, proving it was loaded from index")

        (is (= 199
               (count @(fluree/query loaded
                                     {"@context" context
                                      "select"   "?item"
                                      "where"    {"@id" "?item", "@type" "ex:Item"}})))
            "All items should be queryable after cascading splits")

        (is (= [{"@id"      "ex:item42"
                 "@type"    "ex:Item"
                 "ex:name"  "Item 42"
                 "ex:index" 42}]
               @(fluree/query loaded
                              {"@context" context
                               "select"   {"?s" ["*"]}
                               "where"    {"@id" "?s", "ex:index" 42}}))
            "Range scans should work correctly after splits")))))

(deftest ^:integration next-id-persistence-test
  (testing "next-id field is persisted to storage and read back correctly"
    (with-temp-dir [storage-path {}]
      (let [storage-opts {:storage-path (str storage-path)
                          :defaults
                          {:indexing {:reindex-min-bytes 100
                                      :reindex-max-bytes 10000000}}}
            conn         @(fluree/connect-file storage-opts)
            ledger-id    "test/nextid"
            context      {"ex" "http://example.org/ns/"}
            db0          @(fluree/create conn ledger-id)
            items        (mapv (fn [i]
                                 {"@id"      (str "ex:item" i)
                                  "@type"    "ex:Item"
                                  "ex:name"  (str "Item " i)
                                  "ex:index" i})
                               (range 1 150))
            db           @(fluree/update db0
                                         {"@context" context
                                          "insert"   items})
            index-ch     (async/chan 10)
            _db-commit   @(fluree/commit! conn db {:index-files-ch index-ch})
            _            (loop []
                           (when-let [msg (<!! index-ch)]
                             (when-not (= :root (:file-type msg))
                               (recur))))
            _            (<!! (timeout 100))
            conn-fresh   @(fluree/connect-file storage-opts)
            loaded       @(fluree/load conn-fresh ledger-id)]
        (is (nil? (:novelty loaded))
            "Loaded db should have no novelty, proving it was loaded from index")

        (is (= 149
               (count @(fluree/query loaded
                                     {"@context" context
                                      "select"   "?item"
                                      "where"    {"@id" "?item", "@type" "ex:Item"}})))
            "All items should be queryable after split with next-id")

        (is (= 10
               (count @(fluree/query loaded
                                     {"@context" context
                                      "select"   "?item"
                                      "where"    {"@id" "?item", "@type" "ex:Item"}
                                      "limit"    10})))
            "Limited range queries should work correctly with next-id")))))

(deftest ^:integration next-id-chain-verification-test
  (testing "Verify next-id chain is correct after first and second reindex"
    (with-temp-dir [storage-path {}]
      (binding [novelty/*overflow-bytes* 1000
                novelty/*overflow-children* 8]
        (let [storage-opts  {:storage-path (str storage-path)
                             :defaults
                             {:indexing {:reindex-min-bytes 100
                                         :reindex-max-bytes 10000000}}}
              conn          @(fluree/connect-file storage-opts)
              ledger-id     "test/chain"
              context       {"ex" "http://example.org/ns/"}
              db0           @(fluree/create conn ledger-id)
              items         (mapv (fn [i]
                                    {"@id"      (str "ex:item" i)
                                     "@type"    "ex:Item"
                                     "ex:name"  (str "Item " i)
                                     "ex:value" i})
                                  (range 1 150))
              db1           @(fluree/update db0
                                            {"@context" context
                                             "insert"   items})
              index-ch1     (async/chan 10)
              _             @(fluree/commit! conn db1 {:index-files-ch index-ch1})
              _             (loop []
                              (when-let [msg (<!! index-ch1)]
                                (when-not (= :root (:file-type msg))
                                  (recur))))
              _             (<!! (timeout 100))
              conn-fresh1   @(fluree/connect-file storage-opts)
              loaded1-async @(fluree/load conn-fresh1 ledger-id)
              loaded1       (<?? (async-db/deref-async loaded1-async))
              leaves1       (<?? (collect-leaf-chain (:index-catalog loaded1)
                                                     (:spot loaded1)))]

          (is (> (count leaves1) 1)
              "Should have multiple leaves after split")
          (verify-leaf-chain leaves1)

          (let [more-items   (mapv (fn [i]
                                     {"@id"      (str "ex:item" i)
                                      "@type"    "ex:Item"
                                      "ex:name"  (str "Item " i)
                                      "ex:value" i})
                                   (range 150 200))
                db2          @(fluree/update loaded1
                                             {"@context" context
                                              "insert"   more-items})
                index-ch2    (async/chan 10)
                _            @(fluree/commit! conn-fresh1 db2 {:index-files-ch index-ch2})
                _            (loop []
                               (when-let [msg (<!! index-ch2)]
                                 (when-not (= :root (:file-type msg))
                                   (recur))))
                _            (<!! (timeout 100))
                conn-fresh2  @(fluree/connect-file storage-opts)
                loaded2-async @(fluree/load conn-fresh2 ledger-id)
                loaded2      (<?? (async-db/deref-async loaded2-async))
                leaves2      (<?? (collect-leaf-chain (:index-catalog loaded2)
                                                      (:spot loaded2)))]

            (is (>= (count leaves2) (count leaves1))
                "Second index should have at least as many leaves as first")
            (verify-leaf-chain leaves2)

            (is (= 199
                   (count @(fluree/query loaded2
                                         {"@context" context
                                          "select"   "?item"
                                          "where"    {"@id" "?item", "@type" "ex:Item"}})))
                "All items should be queryable after second reindex")))))))

(deftest ^:integration backward-compatibility-test
  (testing "New split logic is backward compatible with old indexes"
    (with-temp-dir [storage-path {}]
      (let [storage-opts {:storage-path (str storage-path)
                          :defaults
                          {:indexing {:reindex-min-bytes 100
                                      :reindex-max-bytes 10000000}}}
            conn         @(fluree/connect-file storage-opts)
            ledger-id    "test/compat"
            context      {"ex" "http://example.org/ns/"}
            db0          @(fluree/create conn ledger-id)
            db1          @(fluree/update db0
                                         {"@context" context
                                          "insert"   [{"@id"     "ex:alice"
                                                       "@type"   "ex:Person"
                                                       "ex:name" "Alice"}]})
            index-ch     (async/chan 10)
            _db-commit   @(fluree/commit! conn db1 {:index-files-ch index-ch})
            _            (loop []
                           (when-let [msg (<!! index-ch)]
                             (when-not (= :root (:file-type msg))
                               (recur))))
            _            (<!! (timeout 100))
            conn-fresh   @(fluree/connect-file storage-opts)
            loaded       @(fluree/load conn-fresh ledger-id)
            db2          @(fluree/update loaded
                                         {"@context" context
                                          "insert"   [{"@id"     "ex:bob"
                                                       "@type"   "ex:Person"
                                                       "ex:name" "Bob"}]})]
        (is (nil? (:novelty loaded))
            "Loaded db should have no novelty, proving it was loaded from index")

        (is (= 2
               (count @(fluree/query db2
                                     {"@context" context
                                      "select"   "?person"
                                      "where"    {"@id" "?person", "@type" "ex:Person"}})))
            "Mixed old and new index nodes should work together")))))
