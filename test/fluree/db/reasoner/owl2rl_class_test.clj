(ns fluree.db.reasoner.owl2rl-class-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

;; tests for OWL class reasoning rules

(deftest ^:integration rdfs-subclassof
  (testing "adding subClassOf declarations within the reasoning data set"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base   @(fluree/update (fluree/db ledger)
                                    {"@context" {"ex" "http://example.org/"}
                                     "insert"   [{"@id"     "ex:brian"
                                                  "@type"   "ex:Person"
                                                  "ex:name" "Brian"}
                                                 {"@id"     "ex:laura"
                                                  "@type"   "ex:Employee"
                                                  "ex:name" "Laura"}
                                                 {"@id"     "ex:alice"
                                                  "@type"   "ex:Human"
                                                  "ex:name" "Alice"}]})

          db-reason @(fluree/reason db-base :owl2rl
                                    [{"@context"        {"owl"  "http://www.w3.org/2002/07/owl#"
                                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                                                         "ex"   "http://example.org/"}
                                      "@id"             "ex:Employee"
                                      "@type"           ["owl:Class"]
                                      "rdfs:subClassOf" {"@id" "ex:Person"}}
                                     {"@context"        {"owl"  "http://www.w3.org/2002/07/owl#"
                                                         "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                                                         "ex"   "http://example.org/"}
                                      "@id"             "ex:Person"
                                      "@type"           ["owl:Class"]
                                      "rdfs:subClassOf" {"@id" "ex:Human"}}])]

      (is (= ["ex:Human"]
             @(fluree/query db-reason {:context {"ex"   "http://example.org/"
                                                 "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                       :select  "?subclasses"
                                       :where   {"@id"             "ex:Person"
                                                 "rdfs:subClassOf" "?subclasses"}}))
          "The subClassOf triple should be inserted into the current db")

      (is (= (list "ex:brian" "ex:laura")
             (sort
              @(fluree/query db-reason
                             {:context {"ex" "http://example.org/"}
                              :select  "?s"
                              :where   {"@id"   "?s"
                                        "@type" "ex:Person"}})))
          "ex:brian, ex:laura should be of type ex:Person")

      (is (= (list "ex:alice" "ex:brian" "ex:laura")
             (sort
              @(fluree/query db-reason
                             {:context {"ex" "http://example.org/"}
                              :select  "?s"
                              :where   {"@id"   "?s"
                                        "@type" "ex:Human"}})))
          "ex:brian, ex:laura, ex:alice should be of type ex:Human"))))

(deftest ^:integration owl-equivalent-class
  (testing "owl:equivalentClass test"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"     "ex:brian"
                                                "@type"   "ex:Person"
                                                "ex:name" "Brian"}
                                               {"@id"     "ex:laura"
                                                "@type"   "ex:Human"
                                                "ex:name" "Laura"}
                                               {"@id"     "ex:alice"
                                                "@type"   "ex:HumanBeing"
                                                "ex:name" "Alice"}]})]

      (testing "Testing single owl:equivalentClass declaration"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Human"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" {"@id" "ex:Person"}}])]
          (is (= (list "ex:brian" "ex:laura")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "both ex:brian and ex:laura should be of type ex:Person")

          (is (= (list "ex:brian" "ex:laura")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Human"}})))
              "both ex:brian and ex:laura should be of type ex:Human")))

      (testing "Testing multiple owl:equivalentClass declaration"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Person"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" [{"@id" "ex:Human"} {"@id" "ex:HumanBeing"}]}])]
          (is (= (list "ex:alice" "ex:brian" "ex:laura")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Person"}})))
              "ex:brian, ex:laura and ex:alice should be of type ex:Person")

          (is (= (list "ex:alice" "ex:brian" "ex:laura")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Human"}})))
              "ex:brian, ex:laura and ex:alice should be of type ex:Human")

          (is (= (list "ex:alice" "ex:brian" "ex:laura")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:HumanBeing"}})))
              "ex:brian, ex:laura and ex:alice should be of type ex:HumanBeing"))))))

(deftest ^:integration owl-restriction-has-value
  (testing "owl:Restriction hasValue test - cls-hv1, cls-hv2"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-cls-hv" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"           "ex:alice"
                                                "@type"         "ex:Person"
                                                "ex:hasAccount" true
                                                "ex:age"        21}
                                               {"@id"           "ex:bob"
                                                "@type"         "ex:Person"
                                                "ex:hasAccount" false
                                                "ex:age"        12}
                                               {"@id"   "ex:susan"
                                                "@type" ["ex:Person" "ex:Customer"]}]})]

      (testing "Testing single owl:Restriction for a property value"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Customer"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" [{"@type"          "owl:Restriction"
                                                                 "owl:onProperty" {"@id" "ex:hasAccount"}
                                                                 "owl:hasValue"   true}]}])]
          (is (= (list "ex:alice" "ex:susan")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Customer"}})))
              "ex:alice has property ex:hasAccount with value true, ex:susan was explicitly declared as ex:Customer")

          (is (= (list "ex:alice" "ex:susan")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"           "?s"
                                            "ex:hasAccount" true}})))
              "ex:susan should have ex:hasAccount: true inferred based on declared class.")))

      (testing "Testing single owl:Restriction where property value is not an IRI"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Customer"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" [{"@type"          "owl:Restriction"
                                                                 "owl:onProperty" "ex:hasAccount" ;; OOPS! should be an IRI
                                                                 "owl:hasValue"   true}]}])]
          (is (= ["ex:susan"]
                 @(fluree/query db-equiv
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Customer"}}))
              "Rule warning should be logged and no inference should be made."))))))

(deftest ^:integration owl-restriction-some-values-from,
  (testing "owl:Restriction owl:someValuesFrom test - cls-svf1, cls-svf2"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-cls-svf" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"   "ex:winery1"
                                                "@type" "ex:Winery"}
                                               {"@id"   "ex:textile-company"
                                                "@type" "ex:TextileFactory"}
                                               {"@id"   "ex:winery2"
                                                "@type" "ex:Winery"}
                                               {"@id"   "ex:maybe-winery"
                                                "@type" "ex:MaybeWinery"}
                                               {"@id"         "ex:maybe-a-wine"
                                                "@type"       "ex:Product"
                                                "ex:hasMaker" [{"@id" "ex:winery1"}
                                                               {"@id" "ex:textile-company"}]}
                                               {"@id"         "ex:a-wine-1"
                                                "@type"       "ex:Product"
                                                "ex:hasMaker" [{"@id" "ex:winery1"}
                                                               {"@id" "ex:winery2"}]}
                                               {"@id"         "ex:a-wine-2"
                                                "@type"       "ex:Product"
                                                "ex:hasMaker" {"@id" "ex:winery2"}}
                                               {"@id"         "ex:not-a-wine-1"
                                                "@type"       "ex:Product"
                                                "ex:hasMaker" {"@id" "ex:textile-company"}}]})]

      (testing "Testing single owl:Restriction someValuesFrom for a property value"
        (let [db-some-val @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Wine"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"              "owl:Restriction"
                                                                    "owl:onProperty"     {"@id" "ex:hasMaker"}
                                                                    "owl:someValuesFrom" {"@id" "ex:Winery"}}]}])]
          (is (= (list "ex:a-wine-1" "ex:a-wine-2" "ex:maybe-a-wine")
                 (sort
                  @(fluree/query db-some-val
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Wine"}})))
              "only one hasMaker must be a winery to qualify as an ex:Wine")))

      (testing "Testing single owl:Restriction someValuesFrom with owl:oneOf value"
        (let [db-some-val @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Wine"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"              "owl:Restriction"
                                                                    "owl:onProperty"     {"@id" "ex:hasMaker"}
                                                                    "owl:someValuesFrom" {"@type"     "owl:Class"
                                                                                          "owl:oneOf" {"@list" [{"@id" "ex:winery2"}
                                                                                                                {"@id" "ex:winery1"}]}}}]}])]
          (is (= (list "ex:a-wine-1" "ex:a-wine-2" "ex:maybe-a-wine")
                 (sort
                  @(fluree/query db-some-val
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Wine"}})))
              "hasMaker ref can no be of either ex:Winery or ex:TextileFactory to qualify as an ex:Wine")))

      (testing "Testing single owl:Restriction allValuesFrom for a property value"
        (let [db-all-val @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Wine"
                                           "@type"               ["owl:Class"]
                                           "owl:equivalentClass" [{"@type"             "owl:Restriction"
                                                                   "owl:onProperty"    {"@id" "ex:hasMaker"}
                                                                   "owl:allValuesFrom" {"@id" "ex:Winery"}}]}])]
          (is (= (list "ex:textile-company" "ex:winery1" "ex:winery2")
                 (sort
                  @(fluree/query db-all-val
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Winery"}})))
              "because every hasMaker must be a winery, they are all wineries"))))))

(deftest ^:integration owl-max-cardinality
  (testing "owl:maxCardinality test - rule cls-maxc2 (cls-maxc1 is 'false' and ignored)"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-max-card" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"       "ex:brian"
                                                "@type"     "ex:Person"
                                                "ex:mother" [{"@id" "ex:carol"}
                                                             {"@id" "ex:carol2"}
                                                             {"@id" "ex:carol3"}]}]})]

      (testing "Testing owl:maxCardinality=1 declaration (rule cls-maxc2)"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Person"
                                         "owl:equivalentClass" [{"@type"              ["owl:Restriction"]
                                                                 "owl:onProperty"     {"@id" "ex:mother"}
                                                                 "owl:maxCardinality" 1}]}])]

          (is (= (list "ex:carol" "ex:carol2" "ex:carol3")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol"
                                            "owl:sameAs" "?same"}})))
              "ex:carol2 and ex:carol3 should be deemed the same as ex:carol")

          (is (= (list "ex:carol" "ex:carol2" "ex:carol3")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol2"
                                            "owl:sameAs" "?same"}})))
              "ex:carol and ex:carol3 should be deemed the same as ex:carol2")

          (is (= (list "ex:carol" "ex:carol2" "ex:carol3")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol3"
                                            "owl:sameAs" "?same"}})))
              "ex:carol and ex:carol2 should be deemed the same as ex:carol3")))

      (testing "Testing owl:maxCardinality > 1"
        (let [db-42 @(fluree/reason db-base :owl2rl
                                    [{"@context"           {"owl" "http://www.w3.org/2002/07/owl#"
                                                            "ex"  "http://example.org/"}
                                      "@id"                "ex:Human"
                                      "@type"              ["owl:Class"]
                                      "owl:onProperty"     {"@id" "ex:mother"}
                                      "owl:maxCardinality" 42}])]
          (is (= []
                 @(fluree/query db-42
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  "?s"
                                 :where   {"@id"        "ex:carol"
                                           "owl:sameAs" "?same"}}))
              "with maxCardinality > 1, no inferences can be made"))))))

(deftest ^:integration owl-max-qual-cardinality
  (testing "owl:maxQualifiedCardinality test - rules cls-maxqc3, cls-maxqc4"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-max-qual-card" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"       "ex:brian"
                                                "@type"     "ex:Person"
                                                "ex:mother" [{"@id"   "ex:carol"
                                                              "@type" "ex:Parent"}
                                                             {"@id"   "ex:carol2"
                                                              "@type" "ex:Parent"}
                                                             {"@id"   "ex:carol-not"
                                                              "@type" "ex:NotParent"}]}]})]

      (testing "Testing owl:maxQualifiedCardinality=1 declaration (rule cls-maxqc3)"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Person"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" [{"@type"                       ["owl:Restriction"]
                                                                 "owl:onProperty"              {"@id" "ex:mother"}
                                                                 "owl:onClass"                 {"@id" "ex:Parent"}
                                                                 "owl:maxQualifiedCardinality" 1}]}])]
          (is (= (list "ex:carol" "ex:carol2")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol"
                                            "owl:sameAs" "?same"}})))
              "ex:carol and ex:carol2 should be sameAs because their classes are same as owl:onClass restriction")

          (is (= (list "ex:carol" "ex:carol2")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol2"
                                            "owl:sameAs" "?same"}})))
              "ex:carol and ex:carol2 should be sameAs because their classes are same as owl:onClass restriction")

          (is (= []
                 @(fluree/query db-equiv
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  "?same"
                                 :where   {"@id"        "ex:carol-not"
                                           "owl:sameAs" "?same"}}))
              "ex:carol-not is a different class and therefore not equivalent to anyone else")))

      (testing "Testing owl:maxQualifiedCardinality=1 declaration where owl:onClass = owl:Thing (rule cls-maxqc4)"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                "ex"  "http://example.org/"}
                                         "@id"                 "ex:Person"
                                         "@type"               ["owl:Class"]
                                         "owl:equivalentClass" [{"@type"                       ["owl:Restriction"]
                                                                 "owl:onProperty"              {"@id" "ex:mother"}
                                                                 "owl:onClass"                 {"@id" "owl:Thing"}
                                                                 "owl:maxQualifiedCardinality" 1}]}])]
          (is (= (list "ex:carol" "ex:carol-not" "ex:carol2")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol"
                                            "owl:sameAs" "?same"}})))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")

          (is (= (list "ex:carol" "ex:carol-not" "ex:carol2")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol2"
                                            "owl:sameAs" "?same"}})))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")

          (is (= (list "ex:carol" "ex:carol-not" "ex:carol2")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex"  "http://example.org/"
                                            "owl" "http://www.w3.org/2002/07/owl#"}
                                  :select  "?same"
                                  :where   {"@id"        "ex:carol-not"
                                            "owl:sameAs" "?same"}})))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")))

      (testing "Testing owl:maxQualifiedCardinality > 1"
        (let [db-42 @(fluree/reason db-base :owl2rl
                                    [{"@context"                    {"owl" "http://www.w3.org/2002/07/owl#"
                                                                     "ex"  "http://example.org/"}
                                      "@id"                         "ex:Human"
                                      "@type"                       ["owl:Class"]
                                      "owl:onProperty"              {"@id" "ex:mother"}
                                      "owl:onClass"                 {"@id" "owl:Thing"}
                                      "owl:maxQualifiedCardinality" 42}])]
          (is (= []
                 @(fluree/query db-42
                                {:context {"ex"  "http://example.org/"
                                           "owl" "http://www.w3.org/2002/07/owl#"}
                                 :select  "?s"
                                 :where   {"@id"        "ex:carol"
                                           "owl:sameAs" "?same"}}))
              "with maxQualifiedCardinality > 1, no inferences can be made"))))))

(deftest ^:integration owl-one-of
  (testing "owl:oneOf test - rule cls-oo"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-one-of" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"      "ex:widget"
                                                "ex:color" {"@id" "ex:Red"}}
                                               {"@id" "ex:Green"}
                                               {"@id" "ex:Blue"}
                                               {"@id" "ex:Red"}]})]

      (testing "Testing owl:oneOf simple declaration as list"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context" {"owl" "http://www.w3.org/2002/07/owl#"
                                                     "ex"  "http://example.org/"}
                                         "@graph"   [{"@id"                 "ex:RedOrGreen"
                                                      "@type"               ["owl:Class"]
                                                      "owl:equivalentClass" [{"@type"     "owl:Class"
                                                                              "owl:oneOf" {"@list" [{"@id" "ex:Red"}
                                                                                                    {"@id" "ex:Green"}]}}]}
                                                     {"@id"                 "ex:RedOrBlue"
                                                      "@type"               ["owl:Class"]
                                                      "owl:equivalentClass" [{"@type"     "owl:Class"
                                                                              "owl:oneOf" {"@list" [{"@id" "ex:Red"}
                                                                                                    {"@id" "ex:Blue"}]}}]}]}])]
          (is (= (list "ex:Green" "ex:Red")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:RedOrGreen"}}))))

          (is (= (list "ex:Blue" "ex:Red")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:RedOrBlue"}}))))))

      (testing "Testing owl:oneOf simple declaration as multi-cardinality"
        (let [db-equiv @(fluree/reason db-base :owl2rl
                                       [{"@context" {"owl" "http://www.w3.org/2002/07/owl#"
                                                     "ex"  "http://example.org/"}
                                         "@graph"   [{"@id"                 "ex:RedOrGreen"
                                                      "@type"               ["owl:Class"]
                                                      "owl:equivalentClass" [{"@type"     "owl:Class"
                                                                              "owl:oneOf" [{"@id" "ex:Red"}
                                                                                           {"@id" "ex:Green"}]}]}
                                                     {"@id"                 "ex:RedOrBlue"
                                                      "@type"               ["owl:Class"]
                                                      "owl:equivalentClass" [{"@type"     "owl:Class"
                                                                              "owl:oneOf" [{"@id" "ex:Red"}
                                                                                           {"@id" "ex:Blue"}]}]}]}])]
          (is (= (list "ex:Green" "ex:Red")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:RedOrGreen"}}))))

          (is (= (list "ex:Blue" "ex:Red")
                 (sort
                  @(fluree/query db-equiv
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:RedOrBlue"}})))))))))

(deftest ^:integration owl-intersection-of
  (testing "owl:intersectionOf tests - rules cls-int1, cls-int2, scm-int"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-intersection" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"   "ex:carol"
                                                "@type" ["ex:Woman" "ex:Parent"]}
                                               {"@id"    "ex:alice"
                                                "@type"  "ex:Woman"
                                                "ex:age" 21}
                                               {"@id"   "ex:bob"
                                                "@type" ["ex:Parent" "ex:Father"]}
                                               {"@id"   "ex:jen"
                                                "@type" ["ex:Mother"]}]})]

      (testing "Testing owl:intersectionOf declaration"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Mother"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"              ["owl:Class"]
                                                                    "owl:intersectionOf" {"@list" [{"@id" "ex:Woman"}
                                                                                                   {"@id" "ex:Parent"}]}}]}])]

          (is (= (list "ex:carol" "ex:jen")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Mother"}})))
              "ex:carol has explicit types ex:Woman and ex:Parent, so should be inferred as ex:Mother, ex:jen is explicitly declared as ex:Mother")

          (is (= (list "ex:alice" "ex:carol" "ex:jen")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Woman"}})))
              "ex:carol and ex:alice has explicit type ex:Woman and ex:jen is inferred from being ex:Mother")

          (is (= (list "ex:Parent" "ex:Woman")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex"   "http://example.org/"
                                            "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                  :select  "?subclasses"
                                  :where   {"@id"             "ex:Mother"
                                            "rdfs:subClassOf" "?subclasses"}})))
              "ex:Woman and ex:Parent should now be subclasses of ex:Mother - rule scm-int")))

      (testing "Testing owl:intersectionOf with nested hasValue"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Woman-21"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"              ["owl:Class"]
                                                                    "owl:intersectionOf" {"@list" [{"@id" "ex:Woman"}
                                                                                                   {"@type"          ["owl:Restriction"]
                                                                                                    "owl:onProperty" {"@id" "ex:age"}
                                                                                                    "owl:hasValue"   21}]}}]}])]

          (is (= ["ex:alice"]
                 @(fluree/query db-reasoned
                                {:context {"ex" "http://example.org/"}
                                 :select  "?s"
                                 :where   {"@id"   "?s"
                                           "@type" "ex:Woman-21"}}))
              "ex:alice is the only ex:Woman where ex:age is 21"))))))

(deftest ^:integration owl-union-of
  (testing "owl:unionOf tests - rules cls-uni, scm-uni"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-union" nil)
          db-base @(fluree/update (fluree/db ledger)
                                  {"@context" {"ex" "http://example.org/"}
                                   "insert"   [{"@id"   "ex:carol"
                                                "@type" "ex:Mother"}
                                               {"@id"   "ex:Alice"
                                                "@type" "ex:Woman"}
                                               {"@id"   "ex:bob"
                                                "@type" "ex:Father"}
                                               {"@id"         "ex:sue"
                                                "@type"       "ex:Woman"
                                                "ex:isParent" true}]})]

      (testing "Testing owl:unionOf declaration"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Parent"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"       ["owl:Class"]
                                                                    "owl:unionOf" {"@list" [{"@id" "ex:Mother"}
                                                                                            {"@id" "ex:Father"}]}}]}])]

          (is (= (list "ex:bob" "ex:carol")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Parent"}})))
              "ex:carol (ex:Mother) and ex:bob (ex:Father) should be inferred as ex:Parent - rule cls-uni")

          (is (= (list "ex:Father" "ex:Mother")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex"   "http://example.org/"
                                            "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                  :select  "?s"
                                  :where   {"@id"             "?s"
                                            "rdfs:subClassOf" {"@id" "ex:Parent"}}})))
              "ex:carol (ex:Mother) and ex:bob (ex:Father) should be inferred as ex:Parent - rule scm-uni")))

      (testing "Testing owl:unionOf declaration with nested hasValue"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Parent"
                                            "@type"               ["owl:Class"]
                                            "owl:equivalentClass" [{"@type"       ["owl:Class"]
                                                                    "owl:unionOf" {"@list" [{"@id" "ex:Mother"}
                                                                                            {"@id" "ex:Father"}
                                                                                            {"@type"          ["owl:Restriction"]
                                                                                             "owl:onProperty" {"@id" "ex:isParent"}
                                                                                             "owl:hasValue"   true}]}}]}])]

          (is (= (list "ex:bob" "ex:carol" "ex:sue")
                 (sort
                  @(fluree/query db-reasoned
                                 {:context {"ex" "http://example.org/"}
                                  :select  "?s"
                                  :where   {"@id"   "?s"
                                            "@type" "ex:Parent"}})))
              "ex:sue (because ex:isParent=true), ex:carol (because ex:Mother) and ex:bob (because ex:Father)"))))))
