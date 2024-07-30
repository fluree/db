(ns fluree.db.reasoner.owl2rl-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

;; base set of data for reasoning tests
(def reasoning-db-data
  {"@context" {"ex" "http://example.org/"}
   "insert"   [{"@id"         "ex:brian"
                "ex:name"     "Brian"
                "ex:uncle"    {"@id" "ex:jim"}
                "ex:siblings"  [{"@id" "ex:laura"} {"@id" "ex:bob"}]
                "ex:children" [{"@id" "ex:alice"}]
                "ex:address"  {"ex:country" {"@id" "ex:Canada"}}
                "ex:age"      42
                "ex:parents"  [{"@id"        "ex:carol"
                                "ex:name"    "Carol"
                                "ex:age"     72
                                "ex:address" {"ex:country" {"@id" "ex:Singapore"}}
                                "ex:brother" {"@id" "ex:mike"}
                                "ex:parents" [{"@id"     "ex:cheryl"
                                               "ex:name" "Cheryl"}]}]}
               {"@id"     "ex:laura"
                "ex:name" "Laura"}
               {"@id"       "ex:bob"
                "ex:name"   "Bob"
                "ex:gender" {"@id" "ex:Male"}}
               {"@id"       "ex:jim"
                "ex:name"   "Jim"
                "ex:spouse" {"@id" "ex:janine"}}
               {"@id"       "ex:janine"
                "ex:name"   "Janine"
                "ex:gender" {"@id" "ex:Female"}}
               {"@id"       "ex:mike"
                "ex:name"   "Mike"
                "ex:spouse" {"@id" "ex:holly"}}
               {"@id"       "ex:holly"
                "ex:name"   "Holly"
                "ex:gender" {"@id" "ex:Female"}}]})

(deftest ^:integration equality-tests
  (testing "owl equality semantics tests eq-sym and eq-trans"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/basic-owl" nil)
          db-base @(fluree/stage (fluree/db ledger)
                                 {"@context" {"ex" "http://example.org/"}
                                  "insert"   [{"@id"        "ex:carol"
                                               "ex:zipCode" 60657}
                                              {"@id"        "ex:carol-lynn"
                                               "ex:zipCode" 12345}]})]
      (testing "Testing explicit owl:sameAs declaration"
        (let [db-same     @(fluree/stage db-base
                                         {"@context" {"ex"   "http://example.org/"
                                                      "owl"  "http://www.w3.org/2002/07/owl#"
                                                      "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                          "insert"   {"@id"        "ex:carol"
                                                      "owl:sameAs" {"@id" "ex:carol-lynn"}}})

              db-reasoned @(fluree/reason db-same :owl2rl)]

          (is (= (list "ex:carol" "ex:carol-lynn")
                 (sort
                   @(fluree/query db-reasoned
                                  {:context {"ex"  "http://example.org/"
                                             "owl" "http://www.w3.org/2002/07/owl#"}
                                   :select  "?same"
                                   :where   {"@id"        "ex:carol-lynn"
                                             "owl:sameAs" "?same"}})))
              "ex:carol-lynn should be deemed the same as ex:carol")))

      (testing "Testing owl:sameAs passed along as a reasoned rule"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          [{"@context"   {"ex"  "http://example.org/"
                                                          "owl" "http://www.w3.org/2002/07/owl#"}
                                            "@id"        "ex:carol"
                                            "owl:sameAs" {"@id" "ex:carol-lynn"}}])]
          
          (is (= (list "ex:carol" "ex:carol-lynn")
                 (sort
                   @(fluree/query db-reasoned
                                  {:context {"ex"  "http://example.org/"
                                             "owl" "http://www.w3.org/2002/07/owl#"}
                                   :select  "?same"
                                   :where   {"@id"        "ex:carol-lynn"
                                             "owl:sameAs" "?same"}})))
              "ex:carol-lynn should be deemed the same as ex:carol")))


      (testing "Testing owl:sameAs transitivity (eq-trans)"
        (let [ledger      @(fluree/create conn "reasoner/eq-trans" nil)
              db-same     @(fluree/stage (fluree/db ledger)
                                         {"@context" {"ex"   "http://example.org/"
                                                      "owl"  "http://www.w3.org/2002/07/owl#"
                                                      "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                          "insert"   [{"@id"        "ex:carol1"
                                                       "owl:sameAs" {"@id" "ex:carol2"}}
                                                      {"@id"        "ex:carol2"
                                                       "owl:sameAs" {"@id" "ex:carol3"}}
                                                      {"@id"        "ex:carol3"
                                                       "owl:sameAs" {"@id" "ex:carol4"}}]})
              db-reasoned @(fluree/reason db-same :owl2rl)]

          (is (= (list "ex:carol1" "ex:carol2" "ex:carol3" "ex:carol4")
                 (sort
                   @(fluree/query db-reasoned
                                  {:context {"ex"  "http://example.org/"
                                             "owl" "http://www.w3.org/2002/07/owl#"}
                                   :select  "?same"
                                   :where   {"@id"        "ex:carol1"
                                             "owl:sameAs" "?same"}})))
              "ex:carol1 should be sameAs all other carols")

          (is (= (list "ex:carol1" "ex:carol2" "ex:carol3" "ex:carol4")
                 (sort
                   @(fluree/query db-reasoned
                                  {:context {"ex"  "http://example.org/"
                                             "owl" "http://www.w3.org/2002/07/owl#"}
                                   :select  "?same"
                                   :where   {"@id"        "ex:carol2"
                                             "owl:sameAs" "?same"}})))
              "ex:carol2 should be sameAs all other carols")))

      ;; Most documentation recommends not using owl:sameAs for properties
      ;; and for now eq-rep-p is not supported. It would act like owl:equivalentProperty
      (testing "Testing owl:sameAs (eq-rep-s, eq-rep-o)"
        (let [ledger        @(fluree/create conn "reasoner/eq-rep" nil)
              db-same       @(fluree/stage (fluree/db ledger)
                                           {"@context" {"ex"   "http://example.org/"
                                                        "owl"  "http://www.w3.org/2002/07/owl#"
                                                        "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                            "insert"   [{"@id"        "ex:carol1"
                                                         "ex:name"    "Carol1"
                                                         "ex:age"     72
                                                         ;; make ex:carol1 sameAs ex:carol2, ex:carol2 sameAs ex:carol1
                                                         "owl:sameAs" {"@id" "ex:carol2"}
                                                         ;; note ex:friend is one of sameAs values, so will expand
                                                         "ex:friend"  {"@id" "ex:carol3"}}
                                                        {"@id"        "ex:carol2"
                                                         "ex:name"    "Carol2"
                                                         "ex:favFood" {"@id" "ex:pizza"}
                                                         ;; make ex:carol2 sameAs ex:carol3, ex:carol3 sameAs ex:carol2
                                                         "owl:sameAs" {"@id" "ex:carol3"}}
                                                        {"@id"        "ex:carol3"
                                                         "ex:name"    "Carol3"
                                                         "ex:favFood" {"@id" "ex:pizza"}
                                                         ;; make ex:carol3 sameAs ex:carol4, ex:carol4 sameAs ex:carol3
                                                         "owl:sameAs" {"@id" "ex:carol4"}}
                                                        {"@id"       "ex:carol4"
                                                         "ex:name"   "Carol4"
                                                         "ex:favNum" 42}]})
              db-reasoned   @(fluree/reason db-same :owl2rl)
              merged-result {"@id"        nil
                             "ex:name"    ["Carol1" "Carol2" "Carol3" "Carol4"]
                             "ex:age"     72
                             "ex:favFood" {"@id" "ex:pizza"}
                             "ex:friend"  [{"@id" "ex:carol1"}
                                           {"@id" "ex:carol2"}
                                           {"@id" "ex:carol3"}
                                           {"@id" "ex:carol4"}]
                             "ex:favNum"  42
                             "owl:sameAs" [{"@id" "ex:carol1"} {"@id" "ex:carol2"} {"@id" "ex:carol3"} {"@id" "ex:carol4"}]}]

          (is (= [(merge merged-result {"@id" "ex:carol1"})]
                 @(fluree/query db-reasoned
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  {"ex:carol1" ["*"]}}))
              "ex:carol1 have all other carols' properties and values")

          (is (= [(merge merged-result {"@id" "ex:carol2"})]
                 @(fluree/query db-reasoned
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  {"ex:carol2" ["*"]}}))
              "ex:carol2 have all other carols' properties and values")

          (is (= [(merge merged-result {"@id" "ex:carol3"})]
                 @(fluree/query db-reasoned
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  {"ex:carol3" ["*"]}}))
              "ex:carol3 have all other carols' properties and values")

          (is (= [(merge merged-result {"@id" "ex:carol4"})]
                 @(fluree/query db-reasoned
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  {"ex:carol4" ["*"]}}))
              "ex:carol4 have all other carols' properties and values"))))))

(deftest ^:integration domain-and-range
  (testing "rdfs:domain and rdfs:range tests"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/basic-owl" nil)
          db-base @(fluree/stage (fluree/db ledger) reasoning-db-data)]

      (testing "Testing rdfs:domain - rule: prp-dom"
        (let [db-prp-dom @(fluree/reason
                           db-base :owl2rl
                           [{"@context"    {"ex"   "http://example.org/"
                                            "owl"  "http://www.w3.org/2002/07/owl#"
                                            "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                             "@id"         "ex:parents"
                             "@type"       ["owl:ObjectProperty"]
                             "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"}]}])]

          (is (= (list "ex:Child" "ex:Person")
                 (sort
                  @(fluree/query db-prp-dom
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?t"
                                  :where   {"@id"   "ex:brian"
                                            "@type" "?t"}})))
              "ex:brian should be of type ex:Person and ex:Child")

          (is (= ["ex:brian" "ex:carol"]
                 @(fluree/query db-prp-dom
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Child"}}))
              "ex:brian and ex:carol should be the only subjects of type ex:Child")))

      (testing "Testing rdfs:range - rule: prp-rng"
        (let [db-prp-rng @(fluree/reason
                           db-base :owl2rl
                           [{"@context"   {"ex"   "http://example.org/"
                                           "owl"  "http://www.w3.org/2002/07/owl#"
                                           "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                             "@id"        "ex:parents"
                             "@type"      ["owl:ObjectProperty"]
                             "rdfs:range" [{"@id" "ex:Person"} {"@id" "ex:parents"}]}])]

          (is (= (list "ex:Person" "ex:parents")
                 (sort
                  @(fluree/query db-prp-rng
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?t"
                                  :where   {"@id"   "ex:carol"
                                            "@type" "?t"}})))
              "ex:carol should be of type ex:Person and ex:parents")

          (is (= ["ex:carol" "ex:cheryl"]
                 @(fluree/query db-prp-rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:parents"}}))
              "ex:carol and ex:cheryl should be the only subjects of type ex:parents")))


      (testing "Testing multiple rules rdfs:domain + rdfs:range - rules: prp-dom & prp-rng"
        (let [db-prp-dom+rng @(fluree/reason
                               db-base :owl2rl
                               [{"@context"    {"ex"   "http://example.org/"
                                                "owl"  "http://www.w3.org/2002/07/owl#"
                                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                 "@id"         "ex:parents"
                                 "@type"       ["owl:ObjectProperty"]
                                 "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"} {"@id" "ex:Human"}]
                                 "rdfs:range"  [{"@id" "ex:Person"} {"@id" "ex:parents"}]}])]

          (is (= ["ex:brian" "ex:carol"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Child"}}))
              "ex:brian and ex:carol should be the only subjects of type ex:Child")

          (is (= ["ex:carol" "ex:cheryl"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:parents"}}))
              "ex:carol and ex:cheryl should be the only subjects of type ex:parents")

          (is (= (list "ex:brian" "ex:carol" "ex:cheryl")
                 (sort
                  @(fluree/query db-prp-dom+rng
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "ex:brian, ex:carol, and ex:cheryl should be of type ex:Person")))

      (testing "Testing multiple rules from multiple db sources rdfs:domain + rdfs:range - rules: prp-dom & prp-rng"
        (let [domain-rule {"@context"    {"ex"   "http://example.org/"
                                          "owl"  "http://www.w3.org/2002/07/owl#"
                                          "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                           "@id"         "ex:parents"
                           "@type"       ["owl:ObjectProperty"]
                           "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"} {"@id" "ex:Human"}]}
              range-rule {"@context"    {"ex"   "http://example.org/"
                                         "owl"  "http://www.w3.org/2002/07/owl#"
                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                          "@id"         "ex:parents"
                          "@type"       ["owl:ObjectProperty"]
                          "rdfs:range"  [{"@id" "ex:Person"} {"@id" "ex:parents"}]}

              domain-rule-ledger @(fluree/create conn "reasoner/domain-rule")
              domain-rule-db     @(fluree/stage (fluree/db domain-rule-ledger) {"insert" [domain-rule]})

              range-rule-ledger @(fluree/create conn "reasoner/range-rule")
              range-rule-db     @(fluree/stage (fluree/db range-rule-ledger) {"insert" [range-rule]})
              
              db-prp-dom+rng @(fluree/reason
                               db-base :owl2rl
                               [domain-rule-db range-rule-db])]

          (is (= ["ex:brian" "ex:carol"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Child"}}))
              "ex:brian and ex:carol should be the only subjects of type ex:Child")

          (is (= ["ex:carol" "ex:cheryl"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:parents"}}))
              "ex:carol and ex:cheryl should be the only subjects of type ex:parents")

          (is (= (list "ex:brian" "ex:carol" "ex:cheryl")
                 (sort
                  @(fluree/query db-prp-dom+rng
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "ex:brian, ex:carol, and ex:cheryl should be of type ex:Person")))

      (testing "Testing multiple rules from multiple graph sources rdfs:domain + rdfs:range - rules: prp-dom & prp-rng"
        (let [domain-rule {"@context"    {"ex"   "http://example.org/"
                                          "owl"  "http://www.w3.org/2002/07/owl#"
                                          "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                           "@id"         "ex:parents"
                           "@type"       ["owl:ObjectProperty"]
                           "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"} {"@id" "ex:Human"}]}
              range-rule {"@context"    {"ex"   "http://example.org/"
                                         "owl"  "http://www.w3.org/2002/07/owl#"
                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                          "@id"         "ex:parents"
                          "@type"       ["owl:ObjectProperty"]
                          "rdfs:range"  [{"@id" "ex:Person"} {"@id" "ex:parents"}]}

              db-prp-dom+rng @(fluree/reason
                               db-base :owl2rl
                               [domain-rule range-rule])]

          (is (= ["ex:brian" "ex:carol"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Child"}}))
              "ex:brian and ex:carol should be the only subjects of type ex:Child")

          (is (= ["ex:carol" "ex:cheryl"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:parents"}}))
              "ex:carol and ex:cheryl should be the only subjects of type ex:parents")

          (is (= (list "ex:brian" "ex:carol" "ex:cheryl")
                 (sort
                  @(fluree/query db-prp-dom+rng
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "ex:brian, ex:carol, and ex:cheryl should be of type ex:Person")))

      (testing "Testing multiple rules from multiple types of sources rdfs:domain + rdfs:range - rules: prp-dom & prp-rng"
        (let [domain-rule {"@context"    {"ex"   "http://example.org/"
                                          "owl"  "http://www.w3.org/2002/07/owl#"
                                          "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                           "@id"         "ex:parents"
                           "@type"       ["owl:ObjectProperty"]
                           "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"} {"@id" "ex:Human"}]}
              range-rule {"@context"    {"ex"   "http://example.org/"
                                         "owl"  "http://www.w3.org/2002/07/owl#"
                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                          "@id"         "ex:parents"
                          "@type"       ["owl:ObjectProperty"]
                          "rdfs:range"  [{"@id" "ex:Person"} {"@id" "ex:parents"}]}

              domain-rule-ledger @(fluree/create conn "reasoner/domain-rule-mixed")
              domain-rule-db     @(fluree/stage (fluree/db domain-rule-ledger) {"insert" [domain-rule]})

              db-prp-dom+rng @(fluree/reason
                               db-base :owl2rl
                               [domain-rule-db range-rule])]

          (is (= ["ex:brian" "ex:carol"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Child"}}))
              "ex:brian and ex:carol should be the only subjects of type ex:Child")

          (is (= ["ex:carol" "ex:cheryl"]
                 @(fluree/query db-prp-dom+rng
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:parents"}}))
              "ex:carol and ex:cheryl should be the only subjects of type ex:parents")

          (is (= (list "ex:brian" "ex:carol" "ex:cheryl")
                 (sort
                  @(fluree/query db-prp-dom+rng
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "ex:brian, ex:carol, and ex:cheryl should be of type ex:Person"))))))

(deftest ^:integration functional-properties
  (testing "owl:FunctionalProperty tests"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/basic-owl" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"       "ex:brian"
                                                   "ex:mother" [{"@id" "ex:carol"} {"@id" "ex:carol2"}]}
                                                  {"@id"       "ex:ralph"
                                                   "ex:mother" [{"@id" "ex:anne"} {"@id" "ex:anne2"}]}]})
          db-reasoned @(fluree/reason
                         db-base :owl2rl
                         [{"@context" {"ex"  "http://example.org/"
                                       "owl" "http://www.w3.org/2002/07/owl#"}
                           "@id"      "ex:mother"
                           "@type"    ["owl:ObjectProperty" "owl:FunctionalProperty"]}])]
      
      (is (= (list "ex:carol" "ex:carol2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:carol"
                                         "owl:sameAs" "?same"}})))
          "ex:carol should be deemed the same as ex:carol2")

      (is (= (list "ex:carol" "ex:carol2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:carol2"
                                         "owl:sameAs" "?same"}})))
          "ex:carol2 should be deemed the same as ex:carol")

      (is (= (list "ex:anne" "ex:anne2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:anne"
                                         "owl:sameAs" "?same"}})))
          "ex:anne2 should be deemed the same as ex:anne")

      (is (= (list "ex:anne" "ex:anne2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:anne2"
                                         "owl:sameAs" "?same"}})))
          "ex:anne should be deemed the same as ex:anne2"))))

(deftest ^:integration inverse-functional-properties
  (testing "owl:InverseFunctionalProperty tests"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/basic-owl" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"      "ex:brian"
                                                   "ex:email" "brian@example.org"}
                                                  {"@id"      "ex:brian2"
                                                   "ex:email" "brian@example.org"}
                                                  {"@id"      "ex:ralph"
                                                   "ex:email" "ralph@example.org"}
                                                  {"@id"      "ex:ralph2"
                                                   "ex:email" "ralph@example.org"}]})
          db-reasoned @(fluree/reason
                         db-base :owl2rl
                         [{"@context" {"ex"  "http://example.org/"
                                       "owl" "http://www.w3.org/2002/07/owl#"}
                           "@id"      "ex:email"
                           "@type"    ["owl:ObjectProperty" "owl:InverseFunctionalProperty"]}])]
      
      (is (= (list "ex:brian" "ex:brian2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:brian"
                                         "owl:sameAs" "?same"}})))
          "ex:carol should be deemed the same as ex:carol2")

      (is (= (list "ex:ralph" "ex:ralph2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?same"
                               :where   {"@id"        "ex:ralph"
                                         "owl:sameAs" "?same"}})))
          "ex:carol2 should be deemed the same as ex:carol"))))

(deftest ^:integration symetric-properties
  (testing "owl:SymetricProperty tests"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/basic-owl" nil)
          db-base @(fluree/stage (fluree/db ledger) reasoning-db-data)]

      (testing "Testing owl:SymetricProperty - rule: prp-symp"
        (let [db-livesWith @(fluree/stage db-base
                                          {"@context" {"ex"   "http://example.org/"
                                                       "owl"  "http://www.w3.org/2002/07/owl#"
                                                       "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                           "insert"   {"@id"          "ex:person-a"
                                                       "ex:livesWith" {"@id" "ex:person-b"}}})

              db-prp-symp  @(fluree/reason
                              db-livesWith :owl2rl
                              [{"@context" {"ex"   "http://example.org/"
                                            "owl"  "http://www.w3.org/2002/07/owl#"
                                            "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                "@id"      "ex:livesWith"
                                "@type"    ["owl:ObjectProperty" "owl:SymetricProperty"]}])]
          
          (is (= ["ex:person-a"]
                 @(fluree/query db-prp-symp
                                {:context {"ex" "http://example.org/"}
                                 :select  "?x"
                                 :where   {"@id"          "ex:person-b"
                                           "ex:livesWith" "?x"}}))
              "ex:person-b should also live with ex:person-a"))))))

(deftest ^:integration transitive-properties
  (testing "owl:TransitiveProperty tests  - rule: prp-trp"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "reasoner/basic-owl" nil)
          db-base      @(fluree/stage (fluree/db ledger) reasoning-db-data)
          db-livesWith @(fluree/stage db-base
                                      {"@context" {"ex"   "http://example.org/"
                                                   "owl"  "http://www.w3.org/2002/07/owl#"
                                                   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                       "insert"   [{"@id"          "ex:person-a"
                                                    "ex:livesWith" {"@id" "ex:person-b"}}
                                                   {"@id"          "ex:person-b"
                                                    "ex:livesWith" {"@id" "ex:person-c"}}
                                                   {"@id"          "ex:person-c"
                                                    "ex:livesWith" {"@id" "ex:person-d"}}]})

          db-prp-trp   @(fluree/reason
                          db-livesWith :owl2rl
                          [{"@context" {"ex"   "http://example.org/"
                                        "owl"  "http://www.w3.org/2002/07/owl#"
                                        "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                            "@id"      "ex:livesWith"
                            "@type"    ["owl:ObjectProperty" "owl:TransitiveProperty"]}])]
      
      (is (= (list "ex:person-b" "ex:person-c" "ex:person-d")
             (sort
               @(fluree/query db-prp-trp
                              {:context {"ex" "http://example.org/"}
                               :select  "?people"
                               :where   {"@id"          "ex:person-a"
                                         "ex:livesWith" "?people"}})))
          "ex:person-a should also live with ex:person-c and d (transitive)"))))

(deftest ^:integration owl2rl-rdfs-subPropertyOf
  (testing "rdfs:subPropertyOf tests  - rule: prp-spo1"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/owl2rl-rdfs-subPropertyOf" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"       "ex:bob"
                                                   "ex:mother" {"@id" "ex:alice-mom"}
                                                   "ex:father" {"@id" "ex:greg-dad"}}]})
          db-reasoned @(fluree/reason
                         db-base :owl2rl
                         [{"@context"           {"ex"   "http://example.org/"
                                                 "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                                                 "owl"  "http://www.w3.org/2002/07/owl#"}
                           "@id"                "ex:mother"
                           "@type"              ["owl:ObjectProperty"]
                           "rdfs:subPropertyOf" {"@id" "ex:parents"}}
                          {"@context"           {"ex"   "http://example.org/"
                                                 "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                                                 "owl"  "http://www.w3.org/2002/07/owl#"}
                           "@id"                "ex:father"
                           "@type"              ["owl:ObjectProperty"]
                           "rdfs:subPropertyOf" {"@id" "ex:parents"}}])]

      (is (= (list "ex:alice-mom" "ex:greg-dad")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex" "http://example.org/"}
                               :select  "?parents"
                               :where   {"@id"       "ex:bob"
                                         "ex:parents" "?parents"}})))
          "all values from ex:mother and ex:father are now show for ex:parents"))))

(deftest ^:integration prop-chain-axiom
  (testing "owl:propertyChainAxiom tests  - rule: prp-spo2"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/basic-owl" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"       "ex:person-a"
                                                   "ex:parents" [{"@id" "ex:mom"} {"@id" "ex:dad"}]}
                                                  {"@id"       "ex:mom"
                                                   "ex:parents" [{"@id" "ex:mom-mom"} {"@id" "ex:mom-dad"}]}
                                                  {"@id"       "ex:dad"
                                                   "ex:parents" [{"@id" "ex:dad-mom"} {"@id" "ex:dad-dad"}]}
                                                  {"@id"       "ex:mom-mom"
                                                   "ex:parents" [{"@id" "ex:mom-mom-mom"} {"@id" "ex:mom-mom-dad"}]}]})
          db-reasoned @(fluree/reason
                         db-base :owl2rl
                         [{"@context"               {"ex"  "http://example.org/"
                                                     "owl" "http://www.w3.org/2002/07/owl#"}
                           "@id"                    "ex:grandparent"
                           "@type"                  ["owl:ObjectProperty"]
                           "owl:propertyChainAxiom" {"@list" [{"@id" "ex:parents"} {"@id" "ex:parents"}]}}
                          {"@context"               {"ex"  "http://example.org/"
                                                     "owl" "http://www.w3.org/2002/07/owl#"}
                           "@id"                    "ex:greatGrandparent"
                           "@type"                  ["owl:ObjectProperty"]
                           "owl:propertyChainAxiom" {"@list" [{"@id" "ex:parents"} {"@id" "ex:parents"} {"@id" "ex:parents"}]}}])]
      
      (is (= (list "ex:dad-dad" "ex:dad-mom" "ex:mom-dad" "ex:mom-mom")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex" "http://example.org/"}
                               :select  "?people"
                               :where   {"@id"            "ex:person-a"
                                         "ex:grandparent" "?people"}})))
          "all four of ex:person-a's grandparents should be found")

      (is (= (list "ex:mom-mom-dad" "ex:mom-mom-mom")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex" "http://example.org/"}
                               :select  "?people"
                               :where   {"@id"            "ex:mom"
                                         "ex:grandparent" "?people"}})))
          "all two of ex:mom's grandparents should be found")

      (is (= (list "ex:mom-mom-dad" "ex:mom-mom-mom")
             (sort @(fluree/query db-reasoned
                                  {:context {"ex" "http://example.org/"}
                                   :select  "?people"
                                   :where   {"@id"                 "ex:person-a"
                                             "ex:greatGrandparent" "?people"}})))
          "all two of ex:person-a's great grandparents should be found"))))

(deftest ^:integration inverseOf-properties
  (testing "owl:inverseOf tests"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/basic-owl" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"       "ex:son"
                                                   "ex:parents" [{"@id" "ex:mom"} {"@id" "ex:dad"}]}
                                                  {"@id"       "ex:mom"
                                                   "ex:parents" [{"@id" "ex:mom-mom"} {"@id" "ex:mom-dad"}]}
                                                  {"@id"      "ex:alice"
                                                   "ex:child" {"@id" "ex:bob"}}]})

          db-reasoned @(fluree/reason db-base :owl2rl
                                      [{"@context"      {"ex"   "http://example.org/"
                                                         "owl"  "http://www.w3.org/2002/07/owl#"
                                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                        "@id"           "ex:child"
                                        "@type"         ["owl:ObjectProperty"]
                                        "owl:inverseOf" {"@id" "ex:parents"}}])]

      (is (= ["ex:son"]
             @(fluree/query db-reasoned
                            {:context {"ex" "http://example.org/"}
                             :select  "?x"
                             :where   {"@id"      "ex:mom"
                                       "ex:child" "?x"}}))
          "ex:son should be the child of ex:mom")

      (is (= ["ex:mom"]
             @(fluree/query db-reasoned
                            {:context {"ex" "http://example.org/"}
                             :select  "?x"
                             :where   {"@id"      "ex:mom-mom"
                                       "ex:child" "?x"}}))
          "ex:mom should be the child of ex:mom-mom")

      (is (= ["ex:alice"]
             @(fluree/query db-reasoned
                            {:context {"ex" "http://example.org/"}
                             :select  "?x"
                             :where   {"@id"       "ex:bob"
                                       "ex:parents" "?x"}}))
          "ex:alice should be the parent of ex:bob"))))

(deftest ^:integration hasKey-properties
  (testing "owl:hasKey tests"
    (let [conn        (test-utils/create-conn)
          ledger      @(fluree/create conn "reasoner/basic-owl" nil)
          db-base     @(fluree/stage (fluree/db ledger)
                                     {"@context" {"ex" "http://example.org/"}
                                      "insert"   [{"@id"                "ex:brian"
                                                   "@type"              ["ex:RegisteredPatient"]
                                                   "ex:hasWaitingListN" "123-45-6789"}
                                                  {"@id"                "ex:brian2"
                                                   "@type"              ["ex:RegisteredPatient"]
                                                   "ex:hasWaitingListN" "123-45-6789"}
                                                  {"@id"                "ex:bob"
                                                   "@type"              ["ex:RegisteredPatient"]
                                                   "ex:hasWaitingListN" "444-44-4444"}

                                                  {"@id"            "ex:t1"
                                                   "@type"          ["ex:Transplantation"]
                                                   "ex:donorId"     {"@id" "ex:brian"}
                                                   "ex:recipientId" {"@id" "ex:alice"}
                                                   "ex:ofOrgan"     "liver"}
                                                  {"@id"            "ex:t2"
                                                   "@type"          ["ex:Transplantation"]
                                                   "ex:donorId"     {"@id" "ex:brian"}
                                                   "ex:recipientId" {"@id" "ex:alice"}
                                                   "ex:ofOrgan"     "liver"}
                                                  {"@id"            "ex:t3"
                                                   "@type"          ["ex:Transplantation"]
                                                   "ex:donorId"     {"@id" "ex:brian"}
                                                   "ex:recipientId" {"@id" "ex:alice"}
                                                   "ex:ofOrgan"     "heart"}
                                                  {"@id"            "ex:t4"
                                                   "@type"          ["ex:Transplantation"]
                                                   "ex:donorId"     {"@id" "ex:bob"}
                                                   "ex:recipientId" {"@id" "ex:alice"}
                                                   "ex:ofOrgan"     "liver"}]})

          db-reasoned @(fluree/reason db-base :owl2rl
                                      [{"@context"   {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                        "@id"        "ex:RegisteredPatient"
                                        "@type"      ["owl:ObjectProperty"]
                                        "owl:hasKey" {"@id" "ex:hasWaitingListN"}} ;; single value
                                       {"@context"   {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                        "@id"        "ex:Transplantation"
                                        "@type"      ["owl:ObjectProperty"]
                                        "owl:hasKey" [{"@list" [{"@id" "ex:donorId"} ;; multi as @list
                                                                {"@id" "ex:recipientId"}
                                                                {"@id" "ex:ofOrgan"}]}]}])]

      (is (= (list "ex:brian" "ex:brian2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?x"
                               :where   {"@id"        "ex:brian"
                                         "owl:sameAs" "?x"}})))
          "ex:brian should be the same as ex:brian2 because ex:hasWaitingListN is identical")

      (is (= (list "ex:t1" "ex:t2")
             (sort
               @(fluree/query db-reasoned
                              {:context {"ex"  "http://example.org/"
                                         "owl" "http://www.w3.org/2002/07/owl#"}
                               :select  "?x"
                               :where   {"@id"        "ex:t1"
                                         "owl:sameAs" "?x"}})))
          "ex:t1 should be same as ex:t2 because ex:donorId, ex:recipientId, and ex:ofOrgan are identical"))))
