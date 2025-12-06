(ns fluree.db.policy.allow-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration allow-true-test
  (testing "f:allow true provides unconditional allow without query execution"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/allow-true-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [{"@id"         "ex:alice"
                               "@type"       "ex:User"
                               "schema:name" "Alice"
                               "schema:ssn"  "111-11-1111"}
                              {"@id"         "ex:bob"
                               "@type"       "ex:User"
                               "schema:name" "Bob"
                               "schema:ssn"  "222-22-2222"}
                              ;; Policy with f:allow true - unconditional allow
                              {"@id"      "ex:allowAllView"
                               "@type"    ["f:AccessPolicy" "ex:OpenPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "f:allow true allows access to all data"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/OpenPolicy"] nil)]
          (is (= [["ex:alice" "Alice" "111-11-1111"]
                  ["ex:bob" "Bob" "222-22-2222"]]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   ["?s" "?name" "?ssn"]
                    "where"    {"@id"         "?s"
                                "@type"       "ex:User"
                                "schema:name" "?name"
                                "schema:ssn"  "?ssn"}
                    "orderBy"  "?s"}))))))))

(deftest ^:integration allow-false-test
  (testing "f:allow false provides unconditional deny without query execution"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/allow-false-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [{"@id"         "ex:alice"
                               "@type"       "ex:User"
                               "schema:name" "Alice"
                               "schema:ssn"  "111-11-1111"}
                              ;; Policy with f:allow false on ssn property - unconditional deny
                              ;; f:required ensures only this policy is evaluated for ssn
                              {"@id"             "ex:denySsn"
                               "@type"           ["f:AccessPolicy" "ex:RestrictedPolicy"]
                               "f:onProperty"    {"@id" "schema:ssn"}
                               "f:action"        {"@id" "f:view"}
                               "f:required"      true
                               "f:allow"         false}
                              ;; Default allow for everything else
                              {"@id"      "ex:allowOther"
                               "@type"    ["f:AccessPolicy" "ex:RestrictedPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "f:allow false denies access to specific property"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/RestrictedPolicy"] nil)]
          (is (= [{"@id"         "ex:alice"
                   "@type"       "ex:User"
                   "schema:name" "Alice"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:User"}}))
              "ssn should not appear in results due to f:allow false"))))))

(deftest ^:integration allow-precedence-test
  (testing "f:allow takes precedence over f:query"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/allow-precedence-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [{"@id"         "ex:alice"
                               "@type"       "ex:User"
                               "schema:name" "Alice"}
                              ;; Policy with both f:allow and f:query - f:allow should win
                              {"@id"      "ex:allowWithQuery"
                               "@type"    ["f:AccessPolicy" "ex:MixedPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true
                               ;; This query would normally deny, but f:allow true takes precedence
                               "f:query"  {"@type"  "@json"
                                           "@value" {"where" {"@id" "ex:nonexistent"}}}}]})]

      (testing "f:allow true overrides f:query"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/MixedPolicy"] nil)]
          (is (= [{"@id"         "ex:alice"
                   "@type"       "ex:User"
                   "schema:name" "Alice"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:User"}}))
              "f:allow true should allow access even with a failing f:query"))))))
