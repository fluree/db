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

(deftest ^:integration on-property-with-query-test
  (testing "f:onProperty supports queries to dynamically determine restricted properties"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-property-query-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; Data
                              {"@id"         "ex:alice"
                               "@type"       "ex:User"
                               "schema:name" "Alice"
                               "schema:email" "alice@example.org"
                               "schema:ssn"  "111-11-1111"}
                              ;; Property metadata - marks which properties are sensitive
                              {"@id"            "schema:ssn"
                               "ex:isSensitive" true}
                              {"@id"            "schema:email"
                               "ex:isSensitive" true}
                              ;; Policy that restricts sensitive properties using a query
                              {"@id"          "ex:sensitivePropertyPolicy"
                               "@type"        ["f:AccessPolicy" "ex:SensitivePolicy"]
                               "f:action"     {"@id" "f:view"}
                               "f:required"   true
                               ;; Use query to find which properties are sensitive
                               "f:onProperty" {"@type"  "@json"
                                               "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                         "where"    {"@id"            "?$this"
                                                                     "ex:isSensitive" true}}}
                               "f:allow"      false}
                              ;; Default allow for non-sensitive properties
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:SensitivePolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "query-based onProperty restricts dynamically determined properties"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/SensitivePolicy"] nil)]
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
              "ssn and email should be hidden because they are marked as sensitive"))))))

(deftest ^:integration on-property-mixed-iri-and-query-test
  (testing "f:onProperty supports mixing static IRIs and queries in the same policy"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-property-mixed-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; User data with multiple sensitive fields
                              {"@id"            "ex:alice"
                               "@type"          "ex:User"
                               "schema:name"    "Alice"
                               "schema:email"   "alice@example.org"
                               "schema:ssn"     "111-11-1111"
                               "ex:secretCode"  "ABC123"
                               "ex:internalId"  "INT-001"}
                              ;; Mark some properties as sensitive via metadata
                              {"@id"            "schema:email"
                               "ex:isSensitive" true}
                              {"@id"            "ex:secretCode"
                               "ex:isSensitive" true}
                              ;; Policy that restricts properties using BOTH:
                              ;; 1. Static IRI: schema:ssn (always restricted)
                              ;; 2. Query: find properties marked ex:isSensitive
                              {"@id"          "ex:mixedPropertyPolicy"
                               "@type"        ["f:AccessPolicy" "ex:MixedPolicy"]
                               "f:action"     {"@id" "f:view"}
                               "f:required"   true
                               "f:onProperty" [;; Static IRI - always restrict SSN
                                               {"@id" "schema:ssn"}
                                               ;; Query - dynamically find sensitive properties
                                               {"@type"  "@json"
                                                "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                          "where"    {"@id"            "?$this"
                                                                      "ex:isSensitive" true}}}]
                               "f:allow"      false}
                              ;; Default allow for non-restricted properties
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:MixedPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "mixed static IRI and query in onProperty restricts all matching properties"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/MixedPolicy"] nil)]
          (is (= [{"@id"           "ex:alice"
                   "@type"         "ex:User"
                   "schema:name"   "Alice"
                   "ex:internalId" "INT-001"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:User"}}))
              "ssn (static), email and secretCode (query) should be hidden; name and internalId visible"))))))

(deftest ^:integration on-class-restriction-test
  (testing "f:onClass restricts data based on class membership with query condition"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-class-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; Verified user (has verification flag)
                              {"@id"          "ex:alice"
                               "@type"        "ex:User"
                               "schema:name"  "Alice"
                               "ex:verified"  true}
                              ;; Unverified user (no verification flag)
                              {"@id"         "ex:bob"
                               "@type"       "ex:User"
                               "schema:name" "Bob"}
                              ;; Policy that restricts Users - only verified users visible
                              {"@id"        "ex:userRestriction"
                               "@type"      ["f:AccessPolicy" "ex:RestrictedPolicy"]
                               "f:onClass"  {"@id" "ex:User"}
                               "f:action"   {"@id" "f:view"}
                               "f:required" true
                               ;; Only allow access to verified users
                               "f:query"    {"@type"  "@json"
                                             "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                       "where"    {"@id"         "?$this"
                                                                   "ex:verified" true}}}}
                              ;; Default allow for everything else
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:RestrictedPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "onClass with query restricts access based on class + condition"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/RestrictedPolicy"] nil)]
          ;; Only verified users should be visible
          (is (= [{"@id"         "ex:alice"
                   "@type"       "ex:User"
                   "schema:name" "Alice"
                   "ex:verified" true}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:User"}}))
              "Only verified users should be visible due to onClass query restriction"))))))

(deftest ^:integration on-class-allow-boolean-test
  (testing "f:onClass with f:allow boolean for class-wide allow/deny"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-class-allow-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; Internal user
                              {"@id"         "ex:alice"
                               "@type"       "ex:InternalUser"
                               "schema:name" "Alice"
                               "ex:role"     "admin"}
                              ;; External user
                              {"@id"         "ex:bob"
                               "@type"       "ex:ExternalUser"
                               "schema:name" "Bob"
                               "ex:company"  "Acme"}
                              ;; Policy: deny all InternalUser data
                              {"@id"        "ex:denyInternalUsers"
                               "@type"      ["f:AccessPolicy" "ex:ExternalPolicy"]
                               "f:onClass"  {"@id" "ex:InternalUser"}
                               "f:action"   {"@id" "f:view"}
                               "f:required" true
                               "f:allow"    false}
                              ;; Default allow for everything else
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:ExternalPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "f:onClass with f:allow false denies entire class"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/ExternalPolicy"] nil)]
          ;; InternalUsers should be completely hidden
          (is (= []
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:InternalUser"}}))
              "InternalUser data should be completely hidden")

          ;; ExternalUsers should have full access
          (is (= [{"@id"        "ex:bob"
                   "@type"      "ex:ExternalUser"
                   "schema:name" "Bob"
                   "ex:company" "Acme"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:ExternalUser"}}))
              "ExternalUser data should be fully visible"))))))

(deftest ^:integration on-class-restricts-unique-property-test
  (testing "f:onClass restriction hides properties that only exist on restricted class"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-class-unique-prop-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; SecretAgent has a unique property (clearanceLevel)
                              {"@id"              "ex:bond"
                               "@type"            "ex:SecretAgent"
                               "schema:name"      "James Bond"
                               "ex:clearanceLevel" "top-secret"}
                              ;; Regular person (no clearanceLevel)
                              {"@id"         "ex:alice"
                               "@type"       "ex:Person"
                               "schema:name" "Alice"}
                              ;; Policy: deny all SecretAgent data
                              {"@id"        "ex:denySecretAgents"
                               "@type"      ["f:AccessPolicy" "ex:PublicPolicy"]
                               "f:onClass"  {"@id" "ex:SecretAgent"}
                               "f:action"   {"@id" "f:view"}
                               "f:required" true
                               "f:allow"    false}
                              ;; Default allow for everything else
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:PublicPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "querying for property unique to restricted class returns nothing"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/PublicPolicy"] nil)]
          ;; Query for clearanceLevel - should return nothing since it only exists on SecretAgent
          (is (= []
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   ["?s" "?level"]
                    "where"    {"@id"               "?s"
                                "ex:clearanceLevel" "?level"}}))
              "clearanceLevel should not be visible since it only exists on restricted SecretAgent class")

          ;; But regular Person data should still be visible
          (is (= [{"@id"         "ex:alice"
                   "@type"       "ex:Person"
                   "schema:name" "Alice"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:Person"}}))
              "Person data should still be fully visible"))))))

(deftest ^:integration on-class-shared-property-test
  (testing "f:onClass restriction on shared property only hides values from restricted class"
    (let [conn (test-utils/create-conn)
          db0  @(fluree/create conn "policy/on-class-shared-prop-test")
          db   @(fluree/update
                 db0
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "insert"   [;; Employee (will be restricted)
                              {"@id"         "ex:alice"
                               "@type"       "ex:Employee"
                               "schema:name" "Alice Employee"
                               "ex:salary"   100000}
                              {"@id"         "ex:bob"
                               "@type"       "ex:Employee"
                               "schema:name" "Bob Employee"
                               "ex:salary"   90000}
                              ;; Customer (not restricted) - shares schema:name property
                              {"@id"         "ex:carol"
                               "@type"       "ex:Customer"
                               "schema:name" "Carol Customer"
                               "ex:loyalty"  "gold"}
                              {"@id"         "ex:dan"
                               "@type"       "ex:Customer"
                               "schema:name" "Dan Customer"
                               "ex:loyalty"  "silver"}
                              ;; Policy: deny all Employee data (internal company data)
                              {"@id"        "ex:denyEmployees"
                               "@type"      ["f:AccessPolicy" "ex:CustomerPortalPolicy"]
                               "f:onClass"  {"@id" "ex:Employee"}
                               "f:action"   {"@id" "f:view"}
                               "f:required" true
                               "f:allow"    false}
                              ;; Default allow for everything else
                              {"@id"      "ex:defaultAllow"
                               "@type"    ["f:AccessPolicy" "ex:CustomerPortalPolicy"]
                               "f:action" {"@id" "f:view"}
                               "f:allow"  true}]})]

      (testing "query for shared property only returns values from unrestricted class"
        (let [policy-db @(fluree/wrap-class-policy db ["http://example.org/ns/CustomerPortalPolicy"] nil)]
          ;; Query for all schema:name values - should only get Customer names
          (is (= [["ex:carol" "Carol Customer"]
                  ["ex:dan" "Dan Customer"]]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   ["?s" "?name"]
                    "where"    {"@id"         "?s"
                                "schema:name" "?name"}
                    "orderBy"  "?s"}))
              "Only Customer names should be visible; Employee names should be hidden")

          ;; Query for just name values - Employee names should not appear
          (is (= ["Carol Customer" "Dan Customer"]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   "?name"
                    "where"    {"@id"         "?s"
                                "schema:name" "?name"}
                    "orderBy"  "?name"}))
              "Only Customer names should appear; Alice Employee and Bob Employee should be hidden")

          ;; Verify Employee data is completely hidden
          (is (= []
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:Employee"}}))
              "Employee data should be completely hidden")

          ;; Verify Customer data is fully visible
          (is (= [{"@id"         "ex:carol"
                   "@type"       "ex:Customer"
                   "schema:name" "Carol Customer"
                   "ex:loyalty"  "gold"}
                  {"@id"         "ex:dan"
                   "@type"       "ex:Customer"
                   "schema:name" "Dan Customer"
                   "ex:loyalty"  "silver"}]
                 @(fluree/query
                   policy-db
                   {"@context" {"ex"     "http://example.org/ns/"
                                "schema" "http://schema.org/"}
                    "select"   {"?s" ["*"]}
                    "where"    {"@id"   "?s"
                                "@type" "ex:Customer"}
                    "orderBy"  "?s"}))
              "Customer data should be fully visible"))))))
