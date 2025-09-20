(ns fluree.db.transact.upsert-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.test-utils :as test-utils]))

(def sample-insert-txn {"@context" {"ex" "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "@graph"   [{"@id"         "ex:alice",
                                     "@type"        "ex:User",
                                     "schema:name" "Alice"
                                     "ex:nums"     [1 2 3]
                                     "schema:age"  42}
                                    {"@id"         "ex:bob",
                                     "@type"        "ex:User",
                                     "schema:name" "Bob"
                                     "ex:nums"     [1 2 3]
                                     "schema:age"  22}]})

(def sample-upsert-txn {"@context" {"ex" "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "@graph"   [{"@id"         "ex:alice"
                                     "ex:nums"     [4 5 6]
                                     "schema:name" "Alice2"}
                                    {"@id"         "ex:bob"
                                     "ex:nums"     [4 5 6]
                                     "schema:name" "Bob2"}
                                    {"@id"         "ex:jane"
                                     "ex:nums"     [4 5 6]
                                     "schema:name" "Jane2"}]})

(def sample-update-txn {"@context" (get sample-upsert-txn "@context")
                        "where" [["optional" {"@id" "ex:alice" "ex:nums" "?f0"}]
                                 ["optional" {"@id" "ex:alice" "schema:name" "?f1"}]
                                 ["optional" {"@id" "ex:bob" "ex:nums" "?f2"}]
                                 ["optional" {"@id" "ex:bob" "schema:name" "?f3"}]
                                 ["optional" {"@id" "ex:jane" "ex:nums" "?f4"}]
                                 ["optional" {"@id" "ex:jane" "schema:name" "?f5"}]]
                        "delete" [{"@id" "ex:alice" "ex:nums" "?f0"}
                                  {"@id" "ex:alice" "schema:name" "?f1"}
                                  {"@id" "ex:bob" "ex:nums" "?f2"}
                                  {"@id" "ex:bob" "schema:name" "?f3"}
                                  {"@id" "ex:jane" "ex:nums" "?f4"}
                                  {"@id" "ex:jane" "schema:name" "?f5"}]
                        "insert" (get sample-upsert-txn "@graph")})

(deftest upsert-parsing
  (testing "Parsed upsert txn is identical to long-form update txn"
    (is (= (update (parse/parse-upsert-txn sample-upsert-txn {}) :opts dissoc :object-var-parsing)
           (parse/parse-update-txn sample-update-txn {})))))

(deftest ^:integration upsert-data
  (testing "Upserting data into a ledger is identitcal to long-form update txn"
    (let [conn      (test-utils/create-conn)
          db0       @(fluree/create conn "tx/upsert-test")
          db        @(fluree/insert db0 sample-insert-txn)
          db+upsert @(fluree/upsert db sample-upsert-txn)]

      (is (= [{"@id"         "ex:alice",
               "@type"       "ex:User",
               "schema:age"  42,
               "ex:nums"     [4 5 6],
               "schema:name" "Alice2"}
              {"@id"         "ex:bob",
               "schema:age"  22,
               "ex:nums"     [4 5 6],
               "schema:name" "Bob2",
               "@type"       "ex:User"}
              {"@id"         "ex:jane",
               "ex:nums"     [4 5 6],
               "schema:name" "Jane2"}]
             @(fluree/query db+upsert
                            {"@context" {"ex"     "http://example.org/ns/"
                                         "schema" "http://schema.org/"}
                             "select"   {"?id" ["*"]}
                             "where"    {"@id"         "?id"
                                         "schema:name" "?name"}}))
          "Upsert data is inconsistent"))))

(deftest ^:integration upsert-no-changes
  (testing "Upserting identical data to existing does not change ledger"
    (let [conn   (test-utils/create-conn)
          db0    @(fluree/create conn "tx/upsert2")

          db     @(fluree/insert db0 sample-insert-txn)

          db2    @(fluree/upsert db sample-insert-txn)

          db3    @(fluree/upsert db sample-insert-txn)

          query  {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"}
                  "select"   {"?id" ["*"]}
                  "where"    {"@id"         "?id"
                              "schema:name" "?name"}}]

      (is (= (get sample-insert-txn "@graph")
             @(fluree/query db query)
             @(fluree/query db2 query)
             @(fluree/query db3 query))
          "Resulting data should be identical to original insert"))))

(deftest ^:integration upsert-multicardinal-data
  (let [conn (test-utils/create-conn)
        db0  @(fluree/create conn "tx/upsert3")
        db1  @(fluree/insert db0 {"@context" {"ex" "http://example.org/ns/"}
                                  "@graph"   [{"@id"       "ex:alice",
                                               "@type"     "ex:User",
                                               "ex:letter" ["a" "b" "c" "d"]
                                               "ex:num"    [2 4 6 8]}
                                              {"@id"       "ex:bob",
                                               "@type"     "ex:User",
                                               "ex:letter" ["a" "b" "c" "d"]
                                               "ex:num"    [2 4 6 8]}]})]
    (testing "multiple multicardinal properties can be upserted"
      (let [{_time :time db2 :db}
            @(fluree/upsert db1 {"@context" {"ex"     "http://example.org/ns/"
                                             "schema" "http://schema.org/"}
                                 "@graph"   [{"@id"       "ex:alice"
                                              "ex:letter" ["e" "f" "g" "h"]
                                              "ex:num"    [3 5 7 9]}
                                             {"@id"       "ex:bob"
                                              "ex:letter" ["e" "f" "g" "h"]
                                              "ex:num"    [3 5 7 9]}]}
                            {:meta {:time true}})]
        (testing "and the result is correct"
          (is (= [{"@type"     "ex:User",
                   "ex:letter" ["e" "f" "g" "h"],
                   "ex:num"    [3 5 7 9],
                   "@id"       "ex:alice"}
                  {"@type"     "ex:User",
                   "ex:letter" ["e" "f" "g" "h"],
                   "ex:num"    [3 5 7 9],
                   "@id"       "ex:bob"}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"}
                                     "where"    [{"@id" "?s" "@type" "ex:User"}]
                                     "select"   {"?s" ["*"]}}))))))))

(deftest ^:integration upsert-cancels-identical-pairs-in-novelty
  (testing "Upsert cancels identical retract/assert pairs at same t in novelty"
    (let [conn (test-utils/create-conn)
          ledger-name "tx/upsert-cancel-pairs"
          _  @(fluree/create conn ledger-name)
          ctx  {"ex" "http://example.org/ns/"
                "schema" "http://schema.org/"}
          _  @(fluree/insert! conn ledger-name
                              {"@context" ctx
                               "@graph"   [{"@id"         "ex:alice"
                                            "@type"        "ex:User"
                                            "schema:name"  "Alice"
                                            "ex:nums"      [1 2]}]})
          ;; Upsert unchanged schema:name and add one new ex:nums value
          db2  @(fluree/upsert! conn ledger-name
                                {"@context" ctx
                                 "@graph"   [{"@id"         "ex:alice"
                                              "schema:name"  "Alice2"
                                              "ex:nums"      [1 2 3]}]})
          spot (get-in db2 [:novelty :spot])
          s    (iri/encode-iri db2 "http://example.org/ns/alice")
          p-name (iri/encode-iri db2 "http://schema.org/name")
          p-nums (iri/encode-iri db2 "http://example.org/ns/nums")
          alice-flakes (filter #(= s (flake/s %)) spot)
          name-flakes (filter #(= p-name (flake/p %)) alice-flakes)
          nums-flakes (filter #(= p-nums (flake/p %)) alice-flakes)]
      (is (= 3 (count name-flakes))
          "schema:name asserts Alice, then retracts Alice and asserts Alice2 - so three flakes total")
      (is (= 3 (count nums-flakes))
          "ex:nums went from [1 2] to [1 2 3] so only an assertions for total of 3 flakes"))))

(deftest upsert-and-commit
  (let [conn    @(fluree/connect-memory)
        _db0 @(fluree/create conn "tx/upsert")

        _db1 @(fluree/insert! conn "tx/upsert" sample-insert-txn)
        db2  @(fluree/upsert! conn "tx/upsert" sample-upsert-txn)]
    (testing "upsert! commits the data"
      (is (= [{"@type" "ex:User",
               "schema:age" 42,
               "ex:nums" [4 5 6],
               "schema:name" "Alice2",
               "@id" "ex:alice"}
              {"@type" "ex:User",
               "schema:age" 22,
               "ex:nums" [4 5 6],
               "schema:name" "Bob2",
               "@id" "ex:bob"}
              {"schema:name" "Jane2",
               "ex:nums" [4 5 6],
               "@id" "ex:jane"}]
             @(fluree/query db2
                            {"@context" {"ex" "http://example.org/ns/"
                                         "schema" "http://schema.org/"}
                             "select" {"?id" ["*"]}
                             "where" {"@id" "?id"
                                      "schema:name" "?name"}})))
      (is (= 2 (:t db2))))))
