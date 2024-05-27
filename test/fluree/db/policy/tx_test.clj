(ns fluree.db.policy.tx-test
  (:require [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

;; TODO - test with multiple properties and classes on same policy

(deftest ^:integration property-policy-tx-enforcement
  (testing "Restrict an entire class for modification"
    (let [conn              (test-utils/create-conn)
          ledger            @(fluree/create conn "policy/property-policy-tx-enforcement")
          root-did          (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did         (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          john-did          (:id (did/private->did-map "d0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c99"))
          db                @(fluree/stage
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
                                           ;; assign root-did to "ex:rootRole"
                                           {"@id" root-did}
                                           ;; assign alice-did to "ex:userRole" and also link the did to "ex:alice" via "ex:user"
                                           {"@id"      alice-did
                                            "ex:user"  {"@id" "ex:alice"}
                                            "ex:level" 3}
                                           {"@id"     john-did
                                            "ex:user" {"@id" "ex:john"}}]})

          policy            {"@context"     {"ex"     "http://example.org/ns/"
                                             "schema" "http://schema.org/"
                                             "f"      "https://ns.flur.ee/ledger#"}
                             "@id"          "ex:emailPropertyRestriction"
                             "@type"        ["f:AccessPolicy"]
                             "f:onProperty" [{"@id" "schema:email"}]
                             "f:action"     [{"@id" "f:view"}, {"@id" "f:modify"}]
                             "f:exMessage"  "Only users can update their own emails."
                             "f:query"      {"@type"  "@json"
                                             "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                       "where"    [{"@id"     "?$identity"
                                                                    "ex:user" {"@id" "?$this"}}]}}}

          john-params       {"?$identity" {"@value" john-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}}

          alice-params      {"?$identity" {"@value" alice-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}}

          john-allowed      @(fluree/stage
                              @(fluree/wrap-policy db policy true john-params)
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "where"    {"@id"          "ex:john"
                                           "schema:email" "?email"}
                               "delete"   {"@id"          "ex:john"
                                           "schema:email" "?email"}
                               "insert"   {"@id"          "ex:john",
                                           "schema:email" "updatedEmail@flur.ee"}})

          alice-not-allowed @(fluree/stage
                              @(fluree/wrap-policy db policy true alice-params)
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "where"    {"@id"          "ex:john"
                                           "schema:email" "?email"}
                               "delete"   {"@id"          "ex:john"
                                           "schema:email" "?email"}
                               "insert"   {"@id"          "ex:john",
                                           "schema:email" "updatedEmail@flur.ee"}})]

      (is (util/exception? alice-not-allowed))

      (is (= "Only users can update their own emails."
             (ex-message alice-not-allowed)))

      (is (not (util/exception? john-allowed))))))


(deftest ^:integration class-policy-tx-enforcement
  (testing "Restrict an entire class for modification"
    (let [conn              (test-utils/create-conn)
          ledger            @(fluree/create conn "policy/class-policy-tx-enforcement")
          root-did          (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did         (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          john-did          (:id (did/private->did-map "d0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c99"))
          db                @(fluree/stage
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

          policy            {"@context"    {"ex"     "http://example.org/ns/"
                                            "schema" "http://schema.org/"
                                            "f"      "https://ns.flur.ee/ledger#"}
                             "@id"         "ex:productClassRestriction"
                             "@type"       ["f:AccessPolicy"]
                             "f:onClass"   [{"@id" "ex:Product"}]
                             "f:action"    [{"@id" "f:view"}, {"@id" "f:modify"}]
                             "f:exMessage" "Only products managed by the user can be modified."
                             "f:query"     {"@type"  "@json"
                                            "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                      "where"    [{"@id"               "?$identity"
                                                                   "ex:productManager" {"@id" "?$this"}}]}}}

          john-params       {"?$identity" {"@value" john-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}}

          alice-params      {"?$identity" {"@value" alice-did
                                           "@type"  "http://www.w3.org/2001/XMLSchema#anyURI"}}

          john-allowed      @(fluree/stage
                              @(fluree/wrap-policy db policy true john-params)
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "where"    {"@id"         "ex:widget"
                                           "schema:name" "?oldName"}
                               "delete"   {"@id"         "ex:widget"
                                           "schema:name" "?oldName"}
                               "insert"   {"@id"         "ex:widget",
                                           "schema:name" "Widget - Updated"}})
          alice-not-allowed @(fluree/stage
                              @(fluree/wrap-policy db policy true alice-params)
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "where"    {"@id"         "ex:widget"
                                           "schema:name" "?oldName"}
                               "delete"   {"@id"         "ex:widget"
                                           "schema:name" "?oldName"}
                               "insert"   {"@id"         "ex:widget",
                                           "schema:name" "Widget - Updated"}})]

      (is (util/exception? alice-not-allowed))

      (is (= "Only products managed by the user can be modified."
             (ex-message alice-not-allowed)))

      (is (not (util/exception? john-allowed))))))