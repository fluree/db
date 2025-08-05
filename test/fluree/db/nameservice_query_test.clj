(ns fluree.db.nameservice-query-test
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.json-ld.iri :as iri]))

(deftest nameservice-query-test
  (testing "Nameservice query functionality"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create multiple ledgers with some data
        (testing "Create ledgers and insert data"
          ;; Create first ledger with some data
          @(fluree/create conn "ledger-one" {})
          @(fluree/insert! conn "ledger-one"
                           {"@context" {"test" "http://example.org/test#"}
                            "@graph" [{"@id" "test:person1"
                                       "@type" "Person"
                                       "name" "Alice"}]})

          ;; Create second ledger with different data
          @(fluree/create conn "ledger-two" {})
          @(fluree/insert! conn "ledger-two"
                           {"@context" {"test" "http://example.org/test#"}
                            "@graph" [{"@id" "test:person2"
                                       "@type" "Person"
                                       "name" "Bob"}]})

          ;; Create third ledger with more data (multiple commits)
          @(fluree/create conn "ledger-three" {})
          @(fluree/insert! conn "ledger-three"
                           {"@context" {"test" "http://example.org/test#"}
                            "@graph" [{"@id" "test:person3"
                                       "@type" "Person"
                                       "name" "Charlie"}]})
          @(fluree/insert! conn "ledger-three"
                           {"@context" {"test" "http://example.org/test#"}
                            "@graph" [{"@id" "test:person4"
                                       "@type" "Person"
                                       "name" "David"}]}))

        (testing "Query all nameservice records"
          (let [query {"select" ["?s" "?p" "?o"]
                       "where" [{"@id" "?s" "?p" "?o"}]}
                result @(fluree/query-nameservice conn query {})]
            ;; Should have data from all ledgers plus nameservice metadata (at least 3 ledgers created)
            (is (>= (count result) 9) "Should have at least 9 records from 3 ledgers and their metadata")

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
                result @(fluree/query-nameservice conn query {})]
            ;; Should return information about our ledgers
            (is (>= (count result) 3) "Should find at least 3 database records")))

        (testing "Query for ledgers by branch"
          (let [query {"@context" {"f" iri/f-ns}
                       "select" ["?ledger"]
                       "where" [{"@id" "?ns"
                                 "f:ledger" "?ledger"
                                 "f:branch" "main"}]}
                result @(fluree/query-nameservice conn query {})]
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
                result @(fluree/query-nameservice conn query {})]
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
                result @(fluree/query-nameservice conn query {})]
            (is (= (count result) 0) "Should return no results for nonexistent branch")))

        (finally
          ;; Clean up connection
          @(fluree/disconnect conn))))))

(deftest nameservice-query-file-storage-test
  (testing "Nameservice query with file storage"
    (with-temp-dir [storage-path {}]
      (let [conn @(fluree/connect-file {:storage-path (str storage-path)})]
          ;; Create a ledger with file storage
        @(fluree/create conn "file-ledger" {})
        @(fluree/insert! conn "file-ledger"
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
              result @(fluree/query-nameservice conn query {})]
          (is (>= (count result) 1) "Should find file-based ledger")

            ;; Verify we found our file ledger
          (let [file-ledger-result (filter #(= (first %) "file-ledger") result)]
            (is (= (count file-ledger-result) 1) "Should find file-ledger")))
        @(fluree/disconnect conn)))))

(deftest nameservice-slash-ledger-names-test
  (testing "Nameservice with ledger names containing '/' characters"
    (with-temp-dir [storage-path {}]
      (let [conn @(fluree/connect-file {:storage-path (str storage-path)})]
        (try
          ;; Create ledgers with '/' in their names
          (testing "Create ledgers with slash in names"
            @(fluree/create conn "tenant1/customers" {})
            @(fluree/create conn "tenant1/products" {})
            @(fluree/create conn "tenant2/orders" {})

            ;; Insert some data
            @(fluree/insert! conn "tenant1/customers"
                             {"@context" {"test" "http://example.org/test#"}
                              "@graph" [{"@id" "test:customer1"
                                         "@type" "Customer"
                                         "name" "ACME Corp"}]})

            @(fluree/insert! conn "tenant1/products"
                             {"@context" {"test" "http://example.org/test#"}
                              "@graph" [{"@id" "test:product1"
                                         "@type" "Product"
                                         "name" "Widget"}]})

            @(fluree/insert! conn "tenant2/orders"
                             {"@context" {"test" "http://example.org/test#"}
                              "@graph" [{"@id" "test:order1"
                                         "@type" "Order"
                                         "total" 100}]}))

          (testing "Query all nameservice records with slash-named ledgers"
            (let [query {"@context" {"f" iri/f-ns}
                         "select" ["?ledger"]
                         "where" [{"@id" "?ns"
                                   "f:ledger" "?ledger"}]}
                  result @(fluree/query-nameservice conn query {})]
              ;; Should find all three ledgers with slashes
              (is (>= (count result) 3) "Should find at least 3 ledgers")

              ;; Check that we have the expected ledger names
              (let [ledger-names (set (map first result))]
                (is (contains? ledger-names "tenant1/customers") "Should find tenant1/customers")
                (is (contains? ledger-names "tenant1/products") "Should find tenant1/products")
                (is (contains? ledger-names "tenant2/orders") "Should find tenant2/orders"))))

          (testing "Query specific tenant ledgers"
            ;; Query for tenant1 ledgers by prefix
            (let [query {"@context" {"f" iri/f-ns}
                         "select" ["?ledger"]
                         "where" [{"@id" "?ns"
                                   "f:ledger" "?ledger"}]}
                  all-results @(fluree/query-nameservice conn query {})
                  ;; Filter for tenant1 ledgers
                  tenant1-results (filter #(str/starts-with? (first %) "tenant1/") all-results)]
              (is (= (count tenant1-results) 2) "Should find 2 tenant1 ledgers")

              (let [ledger-names (set (map first tenant1-results))]
                (is (= ledger-names #{"tenant1/customers" "tenant1/products"})
                    "Should find only tenant1 ledgers"))))

          (testing "Verify file system structure"
            ;; Check that subdirectories were created correctly
            (let [ns-dir (io/file (str storage-path) "ns@v1")
                  tenant1-dir (io/file ns-dir "tenant1")
                  tenant2-dir (io/file ns-dir "tenant2")]
              (is (.exists ns-dir) "ns@v1 directory should exist")
              (is (.exists tenant1-dir) "tenant1 subdirectory should exist")
              (is (.exists tenant2-dir) "tenant2 subdirectory should exist")

              ;; Check for nameservice files
              (let [customer-file (io/file ns-dir "tenant1/customers@main.json")
                    products-file (io/file ns-dir "tenant1/products@main.json")
                    orders-file (io/file ns-dir "tenant2/orders@main.json")]
                (is (.exists customer-file) "Customer nameservice file should exist")
                (is (.exists products-file) "Products nameservice file should exist")
                (is (.exists orders-file) "Orders nameservice file should exist"))))

          (finally
            @(fluree/disconnect conn)))))))