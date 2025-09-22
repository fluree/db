(ns fluree.db.transact.list-container-serialization-test
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration list-container-serialization-test
  (testing "Transaction with @list container should serialize correctly to disk"
    (with-temp-dir [test-dir {}]
      (let [test-dir-str (str test-dir)
            ;; Create first connection with file storage
            conn1        @(fluree/connect-file {:storage-path test-dir-str})
            ledger-alias "crm/test"
            db0          @(fluree/create conn1 ledger-alias)

            ;; Transaction with @list container (reproducing the issue)
            txn          {"@context" {"crm" "https://data.flur.ee/SampleUnifiedCRMModel/"
                                      "crm:companyIds" {"@container" "@list"}}
                          "insert" [{"@id" "crm:contact/contact-final"
                                     "@type" ["crm:Contact"]
                                     "crm:companyIds" ["company-final"]}]}

            ;; Stage and commit the transaction
            db1          @(fluree/update db0 txn)
            _            @(fluree/commit! conn1 db1)

            ;; Create second connection to test loading from disk
            conn2        @(fluree/connect-file {:storage-path test-dir-str})

            ;; Try to load the database - this should not fail
            loaded-db    @(fluree/load conn2 ledger-alias)

            ;; Query to verify data was loaded correctly
            query        {"@context" {"crm" "https://data.flur.ee/SampleUnifiedCRMModel/"}
                          :select {"crm:contact/contact-final" ["*"]}}
            result       @(fluree/query loaded-db query)]

        ;; Verify the data was correctly saved and loaded
        (is (not (nil? loaded-db)) "Database should load successfully from disk")
        (is (= 1 (count result)) "Should have one contact record")
        ;; When querying a single-value list, Fluree returns the value directly, not as a list
        (is (= "company-final" (-> result first (get "crm:companyIds")))
            "Single list value should be returned directly")

        ;; Clean up
        @(fluree/disconnect conn2)))))

(deftest ^:integration list-container-multiple-values-test
  (testing "Transaction with @list container containing multiple values"
    (with-temp-dir [test-dir {}]
      (let [test-dir-str (str test-dir)
            conn1        @(fluree/connect-file {:storage-path test-dir-str})
            ledger-alias "test/lists"
            db0          @(fluree/create conn1 ledger-alias)

            ;; Transaction with @list containing multiple values
            txn          {"@context" [test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"
                                       "ex:orderedItems" {"@container" "@list"}}]
                          "insert" {"id" "ex:thing1"
                                    "ex:orderedItems" ["first" "second" "third"]}}

            db1          @(fluree/update db0 txn)
            _            @(fluree/commit! conn1 db1)

            _ @(fluree/disconnect conn1)

            ;; Load with new connection
            conn2        @(fluree/connect-file {:storage-path test-dir-str})
            loaded-db    @(fluree/load conn2 ledger-alias)

            query        {"@context" [test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"}]
                          :select {"ex:thing1" ["*"]}}
            result       @(fluree/query loaded-db query)]

        (is (not (nil? loaded-db)) "Database should load successfully")
        (is (= ["first" "second" "third"]
               (-> result first (get "ex:orderedItems")))
            "Ordered list values should be preserved")

        @(fluree/disconnect conn2)))))

(deftest ^:integration list-container-with-objects-test
  (testing "Transaction with @list container containing object references"
    (with-temp-dir [test-dir {}]
      (let [test-dir-str (str test-dir)
            conn1        @(fluree/connect-file {:storage-path test-dir-str})
            ledger-alias "test/list-objects"
            db0          @(fluree/create conn1 ledger-alias)

            ;; Transaction with @list containing object references
            txn          {"@context" [test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"
                                       "ex:orderedFriends" {"@container" "@list"}}]
                          "insert" [{"id" "ex:alice"
                                     "schema:name" "Alice"}
                                    {"id" "ex:bob"
                                     "schema:name" "Bob"}
                                    {"id" "ex:charlie"
                                     "schema:name" "Charlie"
                                     "ex:orderedFriends" [{"id" "ex:alice"}
                                                          {"id" "ex:bob"}]}]}

            db1          @(fluree/update db0 txn)
            _            @(fluree/commit! conn1 db1)

            _ @(fluree/disconnect conn1)

            ;; Load with new connection
            conn2        @(fluree/connect-file {:storage-path test-dir-str})
            loaded-db    @(fluree/load conn2 ledger-alias)

            query        {"@context" [test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"}]
                          :select {"ex:charlie" ["*" {"ex:orderedFriends" ["*"]}]}}
            result       @(fluree/query loaded-db query)]

        (is (not (nil? loaded-db)) "Database should load successfully")
        (is (= 2 (count (-> result first (get "ex:orderedFriends"))))
            "Should have two ordered friends")
        (is (= "Alice" (-> result first (get "ex:orderedFriends") first (get "schema:name")))
            "First friend should be Alice")
        (is (= "Bob" (-> result first (get "ex:orderedFriends") second (get "schema:name")))
            "Second friend should be Bob")

        @(fluree/disconnect conn2)))))