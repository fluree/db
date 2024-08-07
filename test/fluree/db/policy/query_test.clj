(ns fluree.db.policy.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.did :as did]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration property-policy-query-enforcement
  (testing "Global restrictions on properties"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "policy/property-policy-query-enforcement")
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
                                   ;; assign root-did to "ex:rootRole"
                                   {"@id" root-did}
                                   ;; assign alice-did to "ex:userRole" and also link the did to "ex:alice" via "ex:user"
                                   {"@id"     alice-did
                                    "ex:user" {"@id" "ex:alice"}}]})

          policy    [{"@context"     {"ex"     "http://example.org/ns/"
                                      "schema" "http://schema.org/"
                                      "f"      "https://ns.flur.ee/ledger#"}
                      "@id"          "ex:ssnRestriction"
                      "@type"        ["f:AccessPolicy"]
                      "f:onProperty" [{"@id" "schema:ssn"}]
                      "f:action"     {"@id" "f:view"}
                      "f:query"      {"@type"  "@json"
                                      "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                "where"    {"@id"     "?$identity"
                                                            "ex:user" {"@id" "?$this"}}}}}]

          policy-db @(fluree/wrap-policy db policy true
                                         {"?$identity" {"@value" alice-did
                                                        "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}})]

      (testing " with direct select binding restricts"
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

      (testing " with where-clause match of restricted data"
        (is (= []
               @(fluree/query
                 policy-db
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"}
                  "select"   "?s"
                  "where"    {"@id"        "?s"
                              "schema:ssn" "888-88-8888"}}))
            "ex:john has ssn 888-88-8888, so should results should be empty"))

      (testing " in a graph crawl restricts"
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
                              "@type" "ex:User"}})))))))


(deftest ^:integration class-policy-query-enforcement
  (testing "Restrict an entire class for viewing via relationship "
    (let [conn            (test-utils/create-conn)
          ledger          @(fluree/create conn "policy/class-policy-query-enforcement")
          root-did        (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did       (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          john-did        (:id (did/private->did-map "d0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c99"))
          db              @(fluree/stage
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
                                          "schema:price"         99.99M
                                          "schema:priceCurrency" "USD"
                                          "ex:internalId"        "widget-1234"
                                          "ex:priorYearSales"    10000000
                                          "ex:priorYearCurrency" "USD"}
                                         ;; assign root-did to "ex:rootRole"
                                         {"@id" root-did}
                                         ;; assign alice-did to "ex:userRole" and also link the did to "ex:alice" via "ex:user"
                                         {"@id"      alice-did
                                          "ex:user"  {"@id" "ex:alice"}
                                          "ex:level" 3}
                                         {"@id"               john-did
                                          "ex:user"           {"@id" "ex:john"}
                                          "ex:productManager" {"@id" "ex:widget"}}]})

          policy          {"@context"  {"ex"     "http://example.org/ns/"
                                        "schema" "http://schema.org/"
                                        "f"      "https://ns.flur.ee/ledger#"}
                           "@id"       "ex:productPropertyRestriction"
                           "@type"     ["f:AccessPolicy"]
                           "f:onClass" [{"@id" "ex:Product"}]
                           "f:action"  {"@id" "f:view"}
                           "f:query"   {"@type"  "@json"
                                        "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                  "where"    [{"@id"               "?$identity"
                                                               "ex:productManager" {"@id" "?$this"}}]}}}
          john-policy-db  @(fluree/wrap-policy
                            db policy true
                            {"?$identity" {"@value" john-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}})

          alice-policy-db @(fluree/wrap-policy
                            db policy true
                            {"?$identity" {"@value" alice-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}})]

      (testing " and values binding has user with policy relationship"
        (is (= [{"@id"                  "ex:widget",
                 "@type"                "ex:Product",
                 "schema:name"          "Widget"
                 "schema:price"         99.99M
                 "schema:priceCurrency" "USD"
                 "ex:internalId"        "widget-1234"
                 "ex:priorYearSales"    10000000
                 "ex:priorYearCurrency" "USD"}]
               @(fluree/query
                 john-policy-db
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "select"   {"?s" ["*"]}
                  "where"    {"@id"   "?s"
                              "@type" "ex:Product"}}))))

      (testing " and values binding has user without policy relationship"
        (is (= []
               @(fluree/query
                 alice-policy-db
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "select"   {"?s" ["*"]}
                  "where"    {"@id"   "?s"
                              "@type" "ex:Product"}})))))))
