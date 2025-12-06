(ns fluree.db.policy.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration property-policy-query-enforcement
  (testing "Global restrictions on properties"
    (let [conn      (test-utils/create-conn)
          db0 @(fluree/create conn "policy/property-policy-query-enforcement")
          root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db        @(fluree/update
                      db0
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

          policy    {"@context" {"ex"     "http://example.org/ns/"
                                 "schema" "http://schema.org/"
                                 "f"      "https://ns.flur.ee/ledger#"}
                     "@graph"   [{"@id"          "ex:ssnRestriction"
                                  "@type"        ["f:AccessPolicy"]
                                  "f:required"   true
                                  "f:targetProperty" [{"@id" "schema:ssn"}]
                                  "f:action"     {"@id" "f:view"}
                                  "f:query"      {"@type"  "@json"
                                                  "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                            "where"    {"@id"     "?$identity"
                                                                        "ex:user" {"@id" "?$this"}}}}}
                                 {"@id"      "ex:defaultAllowView"
                                  "@type"    ["f:AccessPolicy"]
                                  "f:action" {"@id" "f:view"}
                                  "f:query"  {"@type"  "@json"
                                              "@value" {}}}]}

          policy-db @(fluree/wrap-policy db policy ["?$identity" [{"@value" alice-did "@type"  "@id"}]])]

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
                              "@type" "ex:User"}})))))))

(deftest ^:integration class-policy-query-enforcement
  (testing "Restrict an entire class for viewing via relationship "
    (let [conn            (test-utils/create-conn)
          db0 @(fluree/create conn "policy/class-policy-query-enforcement")
          root-did        (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did       (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          john-did        (:id (did/private->did-map "d0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c99"))
          db              @(fluree/update
                            db0
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

          policy          {"@context" {"ex"     "http://example.org/ns/"
                                       "schema" "http://schema.org/"
                                       "f"      "https://ns.flur.ee/ledger#"}
                           "@graph"   [{"@id"       "ex:productPropertyRestriction"
                                        "@type"     ["f:AccessPolicy"]
                                        "f:required" true
                                        "f:onSubject"
                                        {"@type" "@json"
                                         "@value"
                                         {"@context" {"ex" "http://example.org/ns/"}
                                          "where" [{"@id" "?$target" "@type" {"@id" "ex:Product"}}]}}
                                        "f:action"  {"@id" "f:view"}
                                        "f:query"   {"@type"  "@json"
                                                     "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                               "where"    [{"@id"               "?$identity"
                                                                            "ex:productManager" {"@id" "?$this"}}]}}}
                                       {"@id"      "ex:defaultAllowView"
                                        "@type"    ["f:AccessPolicy"]
                                        "f:action" {"@id" "f:view"}
                                        "f:query"  {"@type"  "@json"
                                                    "@value" {}}}]}
          john-policy-db  @(fluree/wrap-policy
                            db policy
                            ["?$identity" [{"@value" john-did "@type"  "@id"}]])

          alice-policy-db @(fluree/wrap-policy
                            db policy
                            ["?$identity" [{"@value" alice-did "@type"  "@id"}]])]

      (testing "and values binding has user with policy relationship"
        (is (= [{"@id"                  "ex:widget",
                 "@type"                "ex:Product",
                 "schema:name"          "Widget"
                 "schema:price"         99.99
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

      (testing "and values binding has user without policy relationship"
        (is (= []
               @(fluree/query
                 alice-policy-db
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"
                              "f"      "https://ns.flur.ee/ledger#"}
                  "select"   {"?s" ["*"]}
                  "where"    {"@id"   "?s"
                              "@type" "ex:Product"}})))))))

(deftest ^:integration class-policy-default-allow
  (testing "Class policy only with default allow both true and false behavior "
    (let [conn         (test-utils/create-conn)
          db0 @(fluree/create conn "policy/class-policy-default-test")
          db           @(fluree/update
                         db0
                         {"@context" {"ex" "http://example.org/ns/"
                                      "f"  "https://ns.flur.ee/ledger#"}
                          "insert"   [{"@id"               "ex:data-0",
                                       "@type"             "ex:Data",
                                       "ex:classification" 0}
                                      {"@id"               "ex:data-1",
                                       "@type"             "ex:Data",
                                       "ex:classification" 1}
                                      {"@id"               "ex:data-2",
                                       "@type"             "ex:Data",
                                       "ex:classification" 2}
                                      ;; note below is of class ex:Other, not ex:Data
                                      {"@id"               "ex:other",
                                       "@type"             "ex:Other",
                                       "ex:classification" -99}
                                      ;; a node that refers to items in ex:Data which, if
                                      ;; pulled in a graph crawl, should still be restricted
                                      {"@id"          "ex:referred",
                                       "@type"        "ex:Referrer",
                                       "ex:referData" [{"@id" "ex:data-0"}
                                                       {"@id" "ex:data-1"}
                                                       {"@id" "ex:data-2"}]}]})

          policy       [{"@context"  {"ex" "http://example.org/ns/"
                                      "f"  "https://ns.flur.ee/ledger#"}
                         "@id"       "ex:unclassRestriction"
                         "@type"     ["f:AccessPolicy", "ex:UnclassPolicy"]
                         "f:required" true
                         "f:targetSubject"
                         {"@type" "@json"
                          "@value"
                          {"@context" {"ex" "http://example.org/ns/"}
                           "where" [{"@id" "?$target" "@type" {"@id" "ex:Data"}}]}}
                         "f:action"  [{"@id" "f:view"}, {"@id" "f:modify"}]
                         "f:query"   {"@type"  "@json"
                                      "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                "where"    [{"@id"               "?$this"
                                                             "ex:classification" "?c"}
                                                            ["filter", "(< ?c 1)"]]}}}]
          policy-allow @(fluree/wrap-policy db (conj policy {"@context" {"ex" "http://example.org/ns/"
                                                                         "f"  "https://ns.flur.ee/ledger#"}
                                                             "@id"      "ex:defaultAllowView"
                                                             "@type"    ["f:AccessPolicy"]
                                                             "f:action" {"@id" "f:view"}
                                                             "f:query"  {"@type"  "@json"
                                                                         "@value" {}}}))

          policy-deny  @(fluree/wrap-policy db policy)

          data-query   {"@context" {"ex" "http://example.org/ns/"},
                        "where"    {"@id"   "?s",
                                    "@type" "ex:Data"},
                        "select"   {"?s" ["*"]}}
          other-query  {"@context" {"ex" "http://example.org/ns/"},
                        "where"    {"@id"   "?s",
                                    "@type" "ex:Other"},
                        "select"   {"?s" ["*"]}}

          refer-query  {"@context" {"ex" "http://example.org/ns/"},
                        "where"    {"@id"   "?s",
                                    "@type" "ex:Referrer"},
                        "select"   {"?s" ["*" {"ex:referData" ["*"]}]}}]

      (testing "with policy default allow? set to true"
        (is (= [{"@id"               "ex:data-0",
                 "@type"             "ex:Data",
                 "ex:classification" 0}]
               @(fluree/query policy-allow data-query)))

        (is (= [{"@id"               "ex:other",
                 "@type"             "ex:Other",
                 "ex:classification" -99}]
               @(fluree/query policy-allow other-query))
            "ex:Other class should not be restricted")

        (is (= [{"@id"          "ex:referred"
                 "@type"        "ex:Referrer"
                 "ex:referData" [{"@id"               "ex:data-0"
                                  "@type"             "ex:Data"
                                  "ex:classification" 0}]}]
               @(fluree/query policy-allow refer-query))
            "in graph crawl ex:Data is still restricted"))

      (testing "with policy default allow? set to false"
        (is (= [{"@id"               "ex:data-0",
                 "@type"             "ex:Data",
                 "ex:classification" 0}]
               @(fluree/query policy-deny data-query)))

        (is (= []
               @(fluree/query policy-deny other-query))
            "ex:Other class should be restricted")))))

(deftest policy-values-test
  (let [conn @(fluree/connect-memory)
        db @(fluree/create-with-txn conn {"@context" [test-utils/default-str-context]
                                          "ledger" "policy/values"
                                          "insert"
                                          [{"@id" "usa:wi"
                                            "@type" "usa:state"
                                            "ex:name" "Wisconsin"
                                            "ex:capitol" "Madison"}
                                           {"@id" "usa:ny"
                                            "@type" "usa:state"
                                            "ex:name" "New York"
                                            "ex:capitol" "Albany"}
                                           {"@id" "usa:nc"
                                            "@type" "usa:state"
                                            "ex:name" "North Carolina"
                                            "ex:capitol" "Charlotte"}
                                           {"@id" "usa:co"
                                            "@type" "usa:state"
                                            "ex:name" "Colorado"
                                            "ex:capitol" "Denver"}
                                           {"@id" "usa:pr"
                                            "@type" "usa:territory"
                                            "ex:name" "Puerto Rico"
                                            "ex:capitol" "San Juan"}]})]
    (testing "no policyValues returns all results"
      (is (= ["Colorado" "New York" "North Carolina" "Puerto Rico" "Wisconsin"]
             @(fluree/query db {"@context" test-utils/default-str-context
                                "where" [{"@id" "?state" "ex:name" "?name"}]
                                "select" "?name"
                                "opts" {"policy"
                                        {"@id" "ex:mystatepolicy"
                                         "@type" ["f:AccessPolicy" "ex:StatePolicy"]
                                         "f:action" {"@id" "f:view"}
                                         "f:query" {"@type" "@json"
                                                    "@value"
                                                    {"where" [{"@id" "?$this" "ex:capitol" "?capitol"}]}}}}}))))
    (testing "a single policyValues value constrains results to corresponding value"
      (is (= ["Wisconsin"]
             @(fluree/query db {"@context" test-utils/default-str-context
                                "where" [{"@id" "?state" "ex:name" "?name"}]
                                "select" "?name"
                                "opts" {"policy"
                                        {"@id" "ex:mystatepolicy"
                                         "@type" ["f:AccessPolicy" "ex:StatePolicy"]
                                         "f:action" {"@id" "f:view"}
                                         "f:query" {"@type" "@json"
                                                    "@value"
                                                    {"where" [{"@id" "?$this" "ex:capitol" "?capitol"}]}}}
                                        "policyValues" ["?capitol" ["Madison"]]}}))))
    (testing "multiple policyValues values constrains results to corresponding values"
      (is (= ["Puerto Rico" "Wisconsin"]
             @(fluree/query db {"@context" test-utils/default-str-context
                                "where" [{"@id" "?state" "ex:name" "?name"}]
                                "select" "?name"
                                "opts" {"policy"
                                        {"@id" "ex:mystatepolicy"
                                         "@type" ["f:AccessPolicy" "ex:StatePolicy"]
                                         "f:action" {"@id" "f:view"}
                                         "f:query" {"@type" "@json"
                                                    "@value"
                                                    {"where" [{"@id" "?$this" "ex:capitol" "?capitol"}]}}}
                                        "policyValues" ["?capitol" ["Madison" "San Juan"]]}}))))
    (testing "multiple vars and multiple values constrains results to corresponding values"
      (is (= ["Wisconsin"]
             @(fluree/query db {"@context" test-utils/default-str-context
                                "where" [{"@id" "?state" "ex:name" "?name"}]
                                "select" "?name"
                                "opts" {"policy"
                                        {"f:action" {"@id" "f:view"}
                                         "f:query" {"@type" "@json"
                                                    "@value"
                                                    {"where" [{"@id" "?$this"
                                                               "@type" "?type"
                                                               "ex:capitol" "?capitol"}]}}}
                                        "policyValues" [["?type" "?capitol"]
                                                        [[{"@value" "usa:state" "@type" "@id"} "Madison"]]]}}))))
    (testing "pre-existing values clause compose with supplied policyValues"
      (is (= ["Wisconsin"]
             @(fluree/query db {"@context" test-utils/default-str-context
                                "where" [{"@id" "?state" "ex:name" "?name"}]
                                "select" "?name"
                                "opts" {"policy"
                                        {"f:action" {"@id" "f:view"}
                                         "f:query" {"@type" "@json"
                                                    "@value"
                                                    {"where" [{"@id" "?$this"
                                                               "@type" "?type"
                                                               "ex:capitol" "?capitol"}]
                                                     "values" ["?type" [{"@value" "usa:state" "@type" "@id"}]]}}}
                                        "policyValues" ["?capitol" ["Madison"]]}}))))))

(deftest ^:integration property-policy-nil-query
  (testing "Restrict properties based on policy"
    (let [conn            (test-utils/create-conn)
          db0 @(fluree/create conn "policy/property-policy-nil-query")
          db              @(fluree/update
                            db0
                            {"@context" {"ex"     "http://example.org/ns/"
                                         "schema" "http://schema.org/"
                                         "f"      "https://ns.flur.ee/ledger#"}
                             "insert"   [{"@id"              "ex:alice",
                                          "@type"            "ex:User",
                                          "schema:name"      "Alice"
                                          "schema:ssn"       "111-11-1111"}
                                         {"@id"              "ex:john",
                                          "@type"            "ex:User",
                                          "schema:name"      "John"
                                          "schema:ssn"       "888-88-8888"}]})

          policy          {"@context" {"ex"     "http://example.org/ns/"
                                       "schema" "http://schema.org/"
                                       "f"      "https://ns.flur.ee/ledger#"}
                           "@graph"   [{"@id"      "ex:defaultAllowView"
                                        "@type"    ["f:AccessPolicy"]
                                        "f:action" {"@id" "f:view"}
                                        "f:query"  {"@type"  "@json"
                                                    "@value" {}}}
                                       {"@id"       "ex:restrictAllSSNs"
                                        "@type"     ["f:AccessPolicy"]
                                        "f:required" true
                                        "f:targetProperty" [{"@id" "schema:ssn"}]
                                        "f:action"  {"@id" "f:view"}}]}
          policy-db  @(fluree/wrap-policy db policy)]

      (testing "and values binding has user with policy relationship"
        (is (= [{"@id" "ex:alice",
                 "@type" "ex:User",
                 "schema:name" "Alice"}
                {"@id" "ex:john",
                 "@type" "ex:User",
                 "schema:name" "John"}]
               @(fluree/query
                 policy-db
                 {"@context" {"ex"     "http://example.org/ns/"
                              "schema" "http://schema.org/"}
                  "select"   {"?s" ["*"]}
                  "where"    {"@id"   "?s"
                              "@type" "ex:User"}})))))))

(deftest ^:integration default-allow-option-test
  (testing "The :default-allow option allows access when no policies apply"
    (let [conn @(fluree/connect-memory)
          db   @(fluree/create-with-txn
                 conn
                 {"@context" {"ex" "http://example.org/ns/"
                              "f"  "https://ns.flur.ee/ledger#"}
                  "ledger"   "policy/default-allow-test"
                  "insert"   [{"@id"               "ex:public-data"
                               "@type"             "ex:Public"
                               "ex:name"           "Public Info"}
                              {"@id"               "ex:secret-data"
                               "@type"             "ex:Secret"
                               "ex:name"           "Secret Info"
                               "ex:classification" "top-secret"}]})

          ;; Policy that only applies to ex:Secret class - denies unless classification is "public"
          deny-secret-policy {"@context" {"ex" "http://example.org/ns/"
                                          "f"  "https://ns.flur.ee/ledger#"}
                              "@id"      "ex:secretRestriction"
                              "@type"    "f:AccessPolicy"
                              "f:targetSubject"
                              {"@type"  "@json"
                               "@value" {"@context" {"ex" "http://example.org/ns/"}
                                         "where"    [{"@id" "?$target" "@type" {"@id" "ex:Secret"}}]}}
                              "f:action" {"@id" "f:view"}
                              ;; This query will fail (return false) for top-secret data
                              "f:query"  {"@type"  "@json"
                                          "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                    "where"    [{"@id"               "?$this"
                                                                 "ex:classification" "public"}]}}}

          public-query {"@context" {"ex" "http://example.org/ns/"}
                        "select"   {"?s" ["*"]}
                        "where"    {"@id" "?s" "@type" "ex:Public"}}

          secret-query {"@context" {"ex" "http://example.org/ns/"}
                        "select"   {"?s" ["*"]}
                        "where"    {"@id" "?s" "@type" "ex:Secret"}}]

      (testing "without default-allow, unmatched data is denied"
        (let [policy-db @(fluree/wrap-policy db deny-secret-policy)]
          ;; ex:Public has no policy targeting it, so it's denied (default deny)
          (is (= []
                 @(fluree/query policy-db public-query))
              "Public data denied because no policy applies")
          ;; ex:Secret has a policy but the query returns false (top-secret != public)
          (is (= []
                 @(fluree/query policy-db secret-query))
              "Secret data denied because policy query returns false")))

      (testing "with default-allow true, unmatched data is allowed"
        (let [policy-db @(fluree/wrap-policy db deny-secret-policy nil true)]
          ;; ex:Public has no policy targeting it, so default-allow kicks in
          (is (= [{"@id"     "ex:public-data"
                   "@type"   "ex:Public"
                   "ex:name" "Public Info"}]
                 @(fluree/query policy-db public-query))
              "Public data allowed because default-allow is true")
          ;; ex:Secret has a policy that evaluates to false, so still denied
          (is (= []
                 @(fluree/query policy-db secret-query))
              "Secret data still denied because policy exists and returns false"))))))
