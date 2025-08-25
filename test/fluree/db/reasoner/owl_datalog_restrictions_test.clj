(ns fluree.db.reasoner.owl-datalog-restrictions-test
  "Tests for OWL-Datalog reasoner restriction features (allValuesFrom, hasValue, multi-restrictions, etc.)"
  (:require [clojure.test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration allValuesFrom-forward-entailment-test
  (testing "Universal restrictions (owl:allValuesFrom) - forward entailment"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/allvalues-forward" nil)

          ;; Formulation ≡ Specification ∩ ∀(isMemberOf)⁻.Ingredient
          ;; If x is Formulation and y isMemberOf x, then y must be Ingredient
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                 "ex:Formulation"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@id" "ex:Specification"}
                                                                                       {"@type"             "owl:Restriction"
                                                                                        "owl:onProperty"    {"@type"         "owl:ObjectProperty"
                                                                                                             "owl:inverseOf" {"@id" "ex:isMemberOf"}}
                                                                                        "owl:allValuesFrom" {"@id" "ex:Ingredient"}}]}}}
                                {"@id" "ex:Specification" "@type" "owl:Class"}
                                {"@id" "ex:Ingredient" "@type" "owl:Class"}
                                {"@id" "ex:isMemberOf" "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data: f is a Formulation, y isMemberOf f
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [{"@id"   "ex:f"
                                      "@type" ["ex:Formulation" "ex:Specification"]}
                                     {"@id"         "ex:y"
                                      "ex:isMemberOf" {"@id" "ex:f"}}
                                     {"@id"         "ex:z"
                                      "ex:isMemberOf" {"@id" "ex:f"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Forward entailment: y isMemberOf Formulation => y is Ingredient"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:y"
                                                      "@type" "?type"}}))
                       "ex:Ingredient")
            "y should be inferred as Ingredient"))

      (testing "Multiple members all become Ingredients"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:z"
                                                      "@type" "?type"}}))
                       "ex:Ingredient")
            "z should also be inferred as Ingredient")))))

(deftest ^:integration same-property-multi-restrictions-test
  (testing "Same-property multi-restrictions in one intersection"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/multi-same-prop" nil)

          ;; DrugProduct ≡ ManufacturedItem ∩ SubstanceDefinition ∩ 
          ;;                ∃isCategorizedBy.DosageForm ∩ 
          ;;                ∃isCategorizedBy.RouteOfAdministration ∩ 
          ;;                ∃conformsTo.Formulation
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                 "ex:DrugProduct"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@id" "ex:ManufacturedItem"}
                                                                                       {"@id" "ex:SubstanceDefinition"}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:isCategorizedBy"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:DosageForm"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:isCategorizedBy"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:RouteOfAdministration"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:conformsTo"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:Formulation"}}]}}}
                                {"@id" "ex:ManufacturedItem" "@type" "owl:Class"}
                                {"@id" "ex:SubstanceDefinition" "@type" "owl:Class"}
                                {"@id" "ex:DosageForm" "@type" "owl:Class"}
                                {"@id" "ex:RouteOfAdministration" "@type" "owl:Class"}
                                {"@id" "ex:Formulation" "@type" "owl:Class"}
                                {"@id" "ex:isCategorizedBy" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:conformsTo" "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data: product1 meets ALL criteria, product2 only meets some
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; Complete product - has everything
                                     {"@id"   "ex:product1"
                                      "@type" ["ex:ManufacturedItem" "ex:SubstanceDefinition"]
                                      "ex:isCategorizedBy" [{"@id" "ex:dosage1" "@type" "ex:DosageForm"}
                                                            {"@id" "ex:route1" "@type" "ex:RouteOfAdministration"}]
                                      "ex:conformsTo" {"@id" "ex:formulation1" "@type" "ex:Formulation"}}

                                    ;; Incomplete - missing RouteOfAdministration categorization
                                     {"@id"   "ex:product2"
                                      "@type" ["ex:ManufacturedItem" "ex:SubstanceDefinition"]
                                      "ex:isCategorizedBy" {"@id" "ex:dosage2" "@type" "ex:DosageForm"}
                                      "ex:conformsTo" {"@id" "ex:formulation2" "@type" "ex:Formulation"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Entity meeting ALL restrictions becomes DrugProduct"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:product1"
                                                      "@type" "?type"}}))
                       "ex:DrugProduct")
            "product1 should be inferred as DrugProduct"))

      (testing "Entity missing one restriction is NOT DrugProduct"
        (is (not (contains? (set @(fluree/query db-reasoned
                                                {:context {"ex" "http://example.org/"}
                                                 :select  "?type"
                                                 :where   {"@id"   "ex:product2"
                                                           "@type" "?type"}}))
                            "ex:DrugProduct"))
            "product2 should NOT be inferred as DrugProduct (missing RouteOfAdministration)")))))

(deftest ^:integration hasValue-restrictions-test
  (testing "HasValue restrictions (owl:hasValue)"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/hasvalue" nil)

          ;; Example: Magnitude class defined by having a specific unit value
          ;; KilogramMagnitude ≡ Magnitude ∩ ∃hasUnit.{kg}
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                 "ex:KilogramMagnitude"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@id" "ex:Magnitude"}
                                                                                       {"@type"         "owl:Restriction"
                                                                                        "owl:onProperty" {"@id" "ex:hasUnit"}
                                                                                        "owl:hasValue"   {"@id" "ex:kg"}}]}}}
                                {"@id" "ex:Magnitude" "@type" "owl:Class"}
                                {"@id" "ex:hasUnit" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:kg" "@type" "ex:Unit"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; magnitude1 has kg unit
                                     {"@id"   "ex:magnitude1"
                                      "@type" "ex:Magnitude"
                                      "ex:hasUnit" {"@id" "ex:kg"}}

                                    ;; magnitude2 has different unit
                                     {"@id"   "ex:magnitude2"
                                      "@type" "ex:Magnitude"
                                      "ex:hasUnit" {"@id" "ex:lb"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Entity with specific hasValue becomes specialized class"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:magnitude1"
                                                      "@type" "?type"}}))
                       "ex:KilogramMagnitude")
            "magnitude1 should be inferred as KilogramMagnitude"))

      (testing "Entity with different value is NOT specialized class"
        (is (not (contains? (set @(fluree/query db-reasoned
                                                {:context {"ex" "http://example.org/"}
                                                 :select  "?type"
                                                 :where   {"@id"   "ex:magnitude2"
                                                           "@type" "?type"}}))
                            "ex:KilogramMagnitude"))
            "magnitude2 should NOT be inferred as KilogramMagnitude")))))

(deftest ^:integration qualified-cardinality-test
  (testing "Qualified cardinalities (owl:qualifiedCardinality)"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/qual-card" nil)

          ;; Simplified: Ingredient must have exactly 1 substance
          ;; This test just ensures we can parse it - full cardinality reasoning is complex
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                 "ex:Ingredient"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@id" "ex:Component"}
                                                                                       {"@type"                       "owl:Restriction"
                                                                                        "owl:onProperty"              {"@id" "ex:hasSubstance"}
                                                                                        "owl:qualifiedCardinality"    1
                                                                                        "owl:onClass"                 {"@id" "ex:Substance"}}]}}}
                                {"@id" "ex:Component" "@type" "owl:Class"}
                                {"@id" "ex:Substance" "@type" "owl:Class"}
                                {"@id" "ex:hasSubstance" "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data - just ensure no errors for now
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [{"@id"   "ex:ingredient1"
                                      "@type" "ex:Component"
                                      "ex:hasSubstance" {"@id" "ex:substance1" "@type" "ex:Substance"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Qualified cardinality parsing doesn't error"
        ;; For now, just ensure the reasoning completes without error
        ;; Full cardinality checking would require counting and validation
        (is (some? db-reasoned) "Reasoning should complete without error")))))

(deftest ^:integration combined-restriction-patterns-test
  (testing "Combined restriction patterns"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/combined-pharma" nil)

          ;; Combining multiple patterns in one test
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; Pattern with allValuesFrom on inverse property
                                {"@id"                 "ex:Container"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"             "owl:Restriction"
                                                        "owl:onProperty"    {"@type"         "owl:ObjectProperty"
                                                                             "owl:inverseOf" {"@id" "ex:containedIn"}}
                                                        "owl:allValuesFrom" {"@id" "ex:Item"}}}

                                ;; Multiple restrictions on same property
                                {"@id"                 "ex:ComplexProduct"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasProperty"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:PropertyA"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasProperty"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:PropertyB"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasProperty"}
                                                                                        "owl:hasValue"       {"@id" "ex:specificValue"}}]}}}

                                {"@id" "ex:Item" "@type" "owl:Class"}
                                {"@id" "ex:PropertyA" "@type" "owl:Class"}
                                {"@id" "ex:PropertyB" "@type" "owl:Class"}
                                {"@id" "ex:containedIn" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:hasProperty" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:specificValue" "@type" "ex:Value"}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; Container with items
                                     {"@id" "ex:container1" "@type" "ex:Container"}
                                     {"@id" "ex:item1" "ex:containedIn" {"@id" "ex:container1"}}
                                     {"@id" "ex:item2" "ex:containedIn" {"@id" "ex:container1"}}

                                    ;; Complex product with all required properties
                                     {"@id" "ex:complex1"
                                      "ex:hasProperty" [{"@id" "ex:propA1" "@type" "ex:PropertyA"}
                                                        {"@id" "ex:propB1" "@type" "ex:PropertyB"}
                                                        {"@id" "ex:specificValue"}]}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "AllValuesFrom with inverse property"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:item1"
                                                      "@type" "?type"}}))
                       "ex:Item")
            "item1 should be inferred as Item")

        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:item2"
                                                      "@type" "?type"}}))
                       "ex:Item")
            "item2 should be inferred as Item"))

      (testing "Multiple restrictions with hasValue on same property"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:complex1"
                                                      "@type" "?type"}}))
                       "ex:ComplexProduct")
            "complex1 should be inferred as ComplexProduct")))))