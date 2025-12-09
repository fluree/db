(ns fluree.db.api.create-test
  "Tests for ledger creation validation and behavior"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util :as util]))

(deftest create-ledger-name-validation
  (testing "Ledger creation name validation"
    (let [conn (test-utils/create-conn)]

      (testing "rejects ledger names containing ':' character"
        (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: invalid:name"
               (try
                 (fluree/create conn "invalid:name")
                 (catch Exception e
                   (-> (test-utils/unwrap-error-signal e) ex-message))))
            "Should reject name with colon")

        (try
          (fluree/create conn "test:branch")
          (is false "Should have thrown exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :db/invalid-ledger-name (-> (test-utils/unwrap-error-signal e) ex-data :error))
                "Should return correct error code")))

        (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: test:feature:v2"
               (try
                 (fluree/create conn "test:feature:v2")
                 (catch Exception e
                   (-> (test-utils/unwrap-error-signal e) ex-message))))
            "Should reject name with multiple colons"))

      (testing "accepts valid ledger names"
        (is (not (util/exception? @(fluree/create conn "valid-name")))
            "Should accept name with hyphen")

        (is (not (util/exception? @(fluree/create conn "valid_name")))
            "Should accept name with underscore")

        (is (not (util/exception? @(fluree/create conn "tenant/database")))
            "Should accept name with slash")

        (is (not (util/exception? @(fluree/create conn "my-ledger-2024")))
            "Should accept alphanumeric with special chars"))

      (testing "automatically appends ':main' branch to valid names"
        (let [db @(fluree/create conn "auto-branch-test")]
          (is (= "auto-branch-test:main" (get-in db [:commit :alias]))
              "Should append :main to ledger name"))))))

(deftest create-with-txn-ledger-name-validation
  (testing "create-with-txn ledger name validation"
    (let [conn (test-utils/create-conn)]

      (testing "rejects ledger names containing ':' character"
        (let [txn-with-colon {"@context" {"ex" "http://example.org/"}
                              "ledger" "invalid:name"
                              "insert" {"@id" "ex:test" "ex:value" 1}}]
          (is (util/exception? @(fluree/create-with-txn conn txn-with-colon))
              "Should reject name with colon"))

        (let [txn-with-branch {"@context" {"ex" "http://example.org/"}
                               "ledger" "test:branch"
                               "insert" {"@id" "ex:test" "ex:value" 1}}
              result @(fluree/create-with-txn conn txn-with-branch)]
          (is (util/exception? result)
              "Should reject name with branch")
          (is (= :db/invalid-ledger-name
                 (-> result ex-data :error))
              "Should return correct error code")))

      (testing "accepts valid ledger names and creates with initial data"
        (let [db @(fluree/create-with-txn conn
                                          {"@context" {"ex" "http://example.org/"}
                                           "ledger" "txn-test"
                                           "insert" {"@id" "ex:alice" "ex:age" 42}})]
          (is (= "txn-test:main" (get-in db [:commit :alias]))
              "Should create ledger with :main branch")

          ;; Verify the initial data was inserted
          (let [result @(fluree/query db
                                      {"@context" {"ex" "http://example.org/"}
                                       "select" {"ex:alice" ["*"]}})]
            (is (= 42 (-> result first (get "ex:age")))
                "Should have inserted initial data")))))))

(deftest edge-case-validation
  (testing "Edge cases for ledger name validation"
    (let [conn (test-utils/create-conn)]

      (testing "empty colon cases"
        (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: :"
               (try
                 (fluree/create conn ":")
                 (catch Exception e
                   (-> (test-utils/unwrap-error-signal e) ex-message))))
            "Should reject single colon")

        (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: :branch"
               (try
                 (fluree/create conn ":branch")
                 (catch Exception e
                   (-> (test-utils/unwrap-error-signal e) ex-message))))
            "Should reject name starting with colon")

        (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: ledger:"
               (try
                 (fluree/create conn "ledger:")
                 (catch Exception e
                   (-> (test-utils/unwrap-error-signal e) ex-message))))
            "Should reject name ending with colon"))

      (testing "special characters that ARE allowed"
        (is (not (util/exception? @(fluree/create conn "ledger.with.dots")))
            "Should accept dots")

        (is (not (util/exception? @(fluree/create conn "ledger-with-dashes")))
            "Should accept dashes")

        (is (not (util/exception? @(fluree/create conn "ledger_with_underscores")))
            "Should accept underscores")

        (is (not (util/exception? @(fluree/create conn "org/department/project")))
            "Should accept multiple slashes")))))

(deftest duplicate-ledger-creation
  (testing "Cannot create duplicate ledgers"
    (let [conn (test-utils/create-conn)
          ledger-name "unique-test"]

      ;; First creation should succeed
      (is (not (util/exception? @(fluree/create conn ledger-name)))
          "First creation should succeed")

      ;; Second creation with same name should fail
      (is (util/exception? @(fluree/create conn ledger-name))
          "Duplicate creation should fail")

      ;; Trying with explicit :main should be rejected by validation
      (is (= "Ledger name cannot contain ':' character. Branches must be created separately. Provided: unique-test:main"
             (try
               (fluree/create conn (str ledger-name ":main"))
               (catch Exception e
                 (-> (test-utils/unwrap-error-signal e) ex-message))))
          "Should reject explicit :main branch in name"))))
