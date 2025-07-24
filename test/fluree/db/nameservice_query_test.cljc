(ns fluree.db.nameservice-query-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing]])
            [babashka.fs :refer [with-temp-dir]]
            [fluree.db.api :as api]
            [fluree.db.json-ld.iri :as iri]))

(deftest nameservice-query-test
  (testing "Nameservice query functionality"
    (let [conn @(api/connect-memory {})]
      (try
        ;; Create multiple ledgers with some data
        (testing "Create ledgers and insert data"
          ;; Create first ledger with some data
          @(api/create conn "ledger-one" {})
          @(api/insert! conn "ledger-one"
                        {"@context" {"test" "http://example.org/test#"}
                         "@graph" [{"@id" "test:person1"
                                    "@type" "Person"
                                    "name" "Alice"}]})

          ;; Create second ledger with different data
          @(api/create conn "ledger-two" {})
          @(api/insert! conn "ledger-two"
                        {"@context" {"test" "http://example.org/test#"}
                         "@graph" [{"@id" "test:person2"
                                    "@type" "Person"
                                    "name" "Bob"}]})

          ;; Create third ledger with more data (multiple commits)
          @(api/create conn "ledger-three" {})
          @(api/insert! conn "ledger-three"
                        {"@context" {"test" "http://example.org/test#"}
                         "@graph" [{"@id" "test:person3"
                                    "@type" "Person"
                                    "name" "Charlie"}]})
          @(api/insert! conn "ledger-three"
                        {"@context" {"test" "http://example.org/test#"}
                         "@graph" [{"@id" "test:person4"
                                    "@type" "Person"
                                    "name" "David"}]}))

        (testing "Query all nameservice records"
          (let [query {"select" ["?s" "?p" "?o"]
                       "where" [{"@id" "?s" "?p" "?o"}]}
                result @(api/query-nameservice conn query {})]
            ;; Should have data from all ledgers plus temporary ledger metadata
            (is (> (count result) 10) "Should have multiple records")

            ;; Check that we have nameservice-specific data by looking for ledger references
            (let [ledger-records (filter (fn [[_ p _]]
                                           (= p "https://ns.flur.ee/ledger#ledger"))
                                         result)]
              (is (>= (count ledger-records) 3) "Should have at least 3 ledger records"))))

        (testing "Query for specific ledger information"
          (let [query {"@context" {"f" iri/f-ns}
                       "select" {"?ns" ["f:ledger" "f:branch" "f:t"]}
                       "where" [{"@id" "?ns"
                                 "@type" "f:Database"}]}
                result @(api/query-nameservice conn query {})]
            ;; Should return information about our ledgers
            (is (>= (count result) 3) "Should find at least 3 database records")))

        (testing "Query for ledgers by branch"
          (let [query {"@context" {"f" iri/f-ns}
                       "select" ["?ledger"]
                       "where" [{"@id" "?ns"
                                 "f:ledger" "?ledger"
                                 "f:branch" "main"}]}
                result @(api/query-nameservice conn query {})]
            ;; Should find our ledgers on main branch
            (is (>= (count result) 3) "Should find ledgers on main branch")

            ;; Check that we have the expected ledger names
            (let [ledger-names (set (map first result))]
              (is (= ledger-names #{"ledger-three" "ledger-one" "ledger-two"})
                  "Should find all ledgers on main branch"))))

        (testing "Query for ledgers with higher t values"
          ;; ledger-three should have t=1 since we did two inserts
          (let [query {"@context" {"f" iri/f-ns}
                       "select" ["?ledger" "?t"]
                       "where" [{"@id" "?ns"
                                 "f:ledger" "?ledger"
                                 "f:t" "?t"}]}
                result @(api/query-nameservice conn query {})]
            (is (>= (count result) 3) "Should find t values for ledgers")

            ;; Check that ledger-three has a higher t value
            (let [ledger-three-result (filter #(= (first %) "ledger-three") result)]
              (is (= (count ledger-three-result) 1) "Should find ledger-three")
              (when (seq ledger-three-result)
                (let [t-value (second (first ledger-three-result))]
                  (is (>= t-value 1) "ledger-three should have t >= 1"))))))

        (testing "Query with no results"
          (let [query {"@context" {"f" iri/f-ns}
                       "select" ["?ledger"]
                       "where" [{"@id" "?ns"
                                 "f:ledger" "?ledger"
                                 "f:branch" "nonexistent-branch"}]}
                result @(api/query-nameservice conn query {})]
            (is (= (count result) 0) "Should return no results for nonexistent branch")))

        (finally
          ;; Clean up connection
          @(api/disconnect conn))))))

(deftest nameservice-query-file-storage-test
  (testing "Nameservice query with file storage"
    (with-temp-dir [storage-path {}]
      (let [conn @(api/connect-file {:storage-path (str storage-path)})]
        (try
          ;; Create a ledger with file storage
          @(api/create conn "file-ledger" {})
          @(api/insert! conn "file-ledger"
                        {"@context" {"test" "http://example.org/test#"}
                         "@graph" [{"@id" "test:file-person"
                                    "@type" "Person"
                                    "name" "File User"}]})

          ;; Query the file-based nameservice
          (let [query {"@context" {"f" iri/f-ns}
                       "select" ["?ledger" "?t"]
                       "where" [{"@id" "?ns"
                                 "f:ledger" "?ledger"
                                 "f:t" "?t"}]}
                result @(api/query-nameservice conn query {})]
            (is (>= (count result) 1) "Should find file-based ledger")

            ;; Verify we found our file ledger
            (let [file-ledger-result (filter #(= (first %) "file-ledger") result)]
              (is (= (count file-ledger-result) 1) "Should find file-ledger")))

          (finally
            @(api/disconnect conn)))))))