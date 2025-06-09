(ns fluree.db.policy.policy-class-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration class-policy-query
  (testing "Policy class based query tests."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "policy/class-policy-query")
          root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db        @(fluree/stage
                      (fluree/db ledger)
                      {"@context" {"ex"     "http://example.org/ns/"
                                   "schema" "http://schema.org/"
                                   "f"      "https://ns.flur.ee/ledger#"}
                       "insert"   [{"@id"              "ex:alice",
                                    "@type"            "ex:User",
                                    "schema:name"      "Alice"
                                    "schema:email"     "alice@flur.ee"
                                    "schema:birthDate" "2022-08-17"
                                    "schema:ssn"       "111-11-1111"}
                                   {"@id"              "ex:john",
                                    "@type"            "ex:User",
                                    "schema:name"      "John"
                                    "schema:email"     "john@flur.ee"
                                    "schema:birthDate" "2021-08-17"
                                    "schema:ssn"       "888-88-8888"}
                                   {"@id"                  "ex:widget",
                                    "@type"                "ex:Product",
                                    "schema:name"          "Widget"
                                    "schema:price"         99.99
                                    "schema:priceCurrency" "USD"}
                                   {"@id" root-did}
                                   ;; assign alice-did to "ex:EmployeePolicy" and also link the did to "ex:alice" via "ex:user"
                                   {"@id"           alice-did
                                    "f:policyClass" [{"@id" "ex:EmployeePolicy"}]
                                    "ex:user"       {"@id" "ex:alice"}}
                                   ;; embedded policy
                                   {"@id"          "ex:ssnRestriction"
                                    "@type"        ["f:AccessPolicy" "ex:EmployeePolicy"]
                                    "f:required"   true
                                    "f:targetProperty" [{"@id" "schema:ssn"}]
                                    "f:action"     {"@id" "f:view"}
                                    "f:query"      {"@type"  "@json"
                                                    "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                              "where"    {"@id"     "?$identity"
                                                                          "ex:user" {"@id" "?$this"}}}}}
                                   {"@id"      "ex:defaultAllowView"
                                    "@type"    ["f:AccessPolicy" "ex:EmployeePolicy"]
                                    "f:action" {"@id" "f:view"}
                                    "f:query"  {"@type"  "@json"
                                                "@value" {}}}]})]
      (testing "setting a policy class and passing a values-map with the user's identity"
        (let [policy-db @(fluree/wrap-class-policy db
                                                   ["http://example.org/ns/EmployeePolicy"]
                                                   ;; presumably values like this would come from upstream
                                                   ;; application or identity provider
                                                   ["?$identity" [alice-did]])]

          (testing "with direct select binding restricts"
            (is (= [["ex:alice" "111-11-1111"]]
                   @(fluree/query
                     policy-db
                     {"@context" {"ex"     "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "select"   ["?s" "?ssn"]
                      "where"    {"@id"        "?s"
                                  "@type"      "ex:User"
                                  "schema:ssn" "?ssn"}}))
                "ex:john should not show up in results"))

          (testing "with where-clause match of restricted data"
            (is (= []
                   @(fluree/query
                     policy-db
                     {"@context" {"ex"     "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "select"   "?s"
                      "where"    {"@id"        "?s"
                                  "schema:ssn" "888-88-8888"}}))
                "ex:john has ssn 888-88-8888, so should results should be empty"))

          (testing "in a graph crawl restricts"
            (is (= [{"@id"              "ex:alice",
                     "@type"            "ex:User",
                     "schema:name"      "Alice"
                     "schema:email"     "alice@flur.ee"
                     "schema:birthDate" "2022-08-17"
                     "schema:ssn"       "111-11-1111"}
                    {"@id"              "ex:john",
                     "@type"            "ex:User",
                     "schema:name"      "John"
                     "schema:email"     "john@flur.ee"
                     "schema:birthDate" "2021-08-17"}]
                   @(fluree/query
                     policy-db
                     {"@context" {"ex"     "http://example.org/ns/"
                                  "schema" "http://schema.org/"}
                      "select"   {"?s" ["*"]}
                      "where"    {"@id"   "?s"
                                  "@type" "ex:User"}})))))))))
