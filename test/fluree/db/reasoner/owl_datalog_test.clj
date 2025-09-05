(ns fluree.db.reasoner.owl-datalog-test
  "Tests for owl-datalog reasoner - an extension of OWL 2 RL that supports
   additional constructs that are still expressible in Datalog:
   - Complex owl:equivalentClass with intersections containing restrictions
   - Blank node restrictions in class definitions
   - Enhanced someValuesFrom reasoning in equivalences
   - Property chains with complex restrictions
   
   Each deftest focuses on a specific construct to enable individual testing
   as we implement support for each feature."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration equivalentClass-with-intersection-test
  (testing "owl:equivalentClass with intersection of named class and restrictions"
    ;; This pattern is used extensively in gistPharma
    ;; Example: gist:DrugProduct ≡ (ManufacturedItem ∩ SubstanceDefinition ∩ ∃conformsTo.Formulation)
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/equiv-intersection" nil)

          ;; Define the ontology with complex equivalentClass
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                  "ex:DrugProduct"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Class"
                                                         "owl:intersectionOf" [{"@id" "ex:ManufacturedItem"}
                                                                               {"@id" "ex:SubstanceDefinition"}
                                                                               {"@type"              "owl:Restriction"
                                                                                "owl:onProperty"     {"@id" "ex:conformsTo"}
                                                                                "owl:someValuesFrom" {"@id" "ex:Formulation"}}]}}
                                {"@id"   "ex:ManufacturedItem"
                                 "@type" "owl:Class"}
                                {"@id"   "ex:SubstanceDefinition"
                                 "@type" "owl:Class"}
                                {"@id"   "ex:Formulation"
                                 "@type" "owl:Class"}
                                {"@id"   "ex:conformsTo"
                                 "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Add instance data that should be inferred as DrugProduct
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   {"@id"          "ex:aspirin-tablet"
                                     "@type"        ["ex:ManufacturedItem" "ex:SubstanceDefinition"]
                                     "ex:conformsTo" {"@id"   "ex:aspirin-formulation"
                                                      "@type" "ex:Formulation"}}}

          db-with-data @(fluree/update db-with-ontology instance-data)

          ;; Apply owl-datalog reasoning
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Instance with all intersection components should be inferred as DrugProduct"
        (is (= #{"ex:DrugProduct" "ex:ManufacturedItem" "ex:SubstanceDefinition"}
               (set @(fluree/query db-reasoned
                                   {:context {"ex" "http://example.org/"}
                                    :select  "?type"
                                    :where   {"@id"   "ex:aspirin-tablet"
                                              "@type" "?type"}})))
            "ex:aspirin-tablet should be inferred as ex:DrugProduct")))))

(deftest ^:integration someValuesFrom-in-equivalentClass-test
  (testing "owl:someValuesFrom in owl:equivalentClass definition"
    ;; Pattern: Class ≡ ∃property.ValueClass
    ;; If x has property y and y is of type ValueClass, then x is of type Class
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/somevalues-equiv" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                  "ex:Parent"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@id" "ex:hasChild"}
                                                         "owl:someValuesFrom" {"@id" "ex:Person"}}}
                                {"@id"   "ex:Person"
                                 "@type" "owl:Class"}
                                {"@id"   "ex:hasChild"
                                 "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [{"@id"        "ex:john"
                                      "ex:hasChild" {"@id" "ex:mary"}}
                                     {"@id"   "ex:mary"
                                      "@type" "ex:Person"}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Individual with property pointing to correct type should be inferred as Parent"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:john"
                                                      "@type" "?type"}}))
                       "ex:Parent")
            "ex:john should be inferred as ex:Parent")))))

(deftest ^:integration blank-node-restrictions-test
  (testing "Blank node restrictions in class definitions"
    ;; Pattern used in gist where restrictions are defined as blank nodes
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/blank-restrictions" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   {"@id"                 "ex:ValidatedProduct"
                                "@type"               "owl:Class"
                                "owl:equivalentClass" {"owl:intersectionOf" [{"@id" "ex:Product"}
                                                                            ;; Blank node restriction
                                                                             {"@type"              "owl:Restriction"
                                                                              "owl:onProperty"     {"@id" "ex:hasValidation"}
                                                                              "owl:someValuesFrom" {"@id" "ex:QualityCheck"}}]}}}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   {"@id"            "ex:product1"
                                     "@type"          "ex:Product"
                                     "ex:hasValidation" {"@id"   "ex:check1"
                                                         "@type" "ex:QualityCheck"}}}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Product with validation should be inferred as ValidatedProduct"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:product1"
                                                      "@type" "?type"}}))
                       "ex:ValidatedProduct")
            "ex:product1 should be inferred as ex:ValidatedProduct")))))

(deftest ^:integration nested-property-chains-test
  (testing "Property chains with complex class definitions"
    ;; Pattern: If x isDirectPartOf y and y conformsTo z, infer relationships
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/property-chains" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [{"@id"                     "ex:hasFormulation"
                                 "@type"                   "owl:ObjectProperty"
                                 "owl:propertyChainAxiom"  {"@list" [{"@id" "ex:isDirectPartOf"}
                                                                     {"@id" "ex:conformsTo"}]}}
                                {"@id"   "ex:FormulatedSubstance"
                                 "@type" "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Restriction"
                                                        "owl:onProperty"     {"@id" "ex:hasFormulation"}
                                                        "owl:someValuesFrom" {"@id" "ex:Formulation"}}}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [{"@id"              "ex:active-ingredient"
                                      "ex:isDirectPartOf" {"@id" "ex:drug-product"}}
                                     {"@id"          "ex:drug-product"
                                      "ex:conformsTo" {"@id"   "ex:formulation1"
                                                       "@type" "ex:Formulation"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Property chain should infer hasFormulation relationship"
        (is (= ["ex:formulation1"]
               @(fluree/query db-reasoned
                              {:context {"ex" "http://example.org/"}
                               :select  "?formulation"
                               :where   {"@id"             "ex:active-ingredient"
                                         "ex:hasFormulation" "?formulation"}}))
            "Property chain should infer ex:active-ingredient ex:hasFormulation ex:formulation1"))

      (testing "Inferred property should trigger class inference"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:active-ingredient"
                                                      "@type" "?type"}}))
                       "ex:FormulatedSubstance")
            "ex:active-ingredient should be inferred as ex:FormulatedSubstance")))))

(deftest ^:integration multiple-intersection-levels-test
  (testing "Multiple levels of intersection in equivalentClass"
    ;; Pattern from gist where intersections contain other intersections
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/nested-intersections" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   {"@id"                 "ex:ComplexProduct"
                                "@type"               "owl:Class"
                                "owl:equivalentClass" {"owl:intersectionOf"
                                                       [{"@id" "ex:BaseProduct"}
                                                        {"owl:intersectionOf"
                                                         [{"@type"              "owl:Restriction"
                                                           "owl:onProperty"     {"@id" "ex:hasComponent"}
                                                           "owl:someValuesFrom" {"@id" "ex:ActiveComponent"}}
                                                          {"@type"              "owl:Restriction"
                                                           "owl:onProperty"     {"@id" "ex:hasQuality"}
                                                           "owl:someValuesFrom" {"@id" "ex:HighQuality"}}]}]}}}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   {"@id"           "ex:product1"
                                     "@type"         "ex:BaseProduct"
                                     "ex:hasComponent" {"@id"   "ex:comp1"
                                                        "@type" "ex:ActiveComponent"}
                                     "ex:hasQuality"   {"@id"   "ex:quality1"
                                                        "@type" "ex:HighQuality"}}}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Instance satisfying nested intersections should be inferred as ComplexProduct"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:product1"
                                                      "@type" "?type"}}))
                       "ex:ComplexProduct")
            "ex:product1 should be inferred as ex:ComplexProduct")))))

(deftest ^:integration unionOf-in-class-expressions-test
  (testing "owl:unionOf in class definitions (disjunction as multiple rules)"
    ;; Pattern: Class ≡ (A ∪ B) means if x is A OR B, then x is Class
    ;; Also tests: Class ≡ ∃property.(A ∪ B)
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/union-of" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; DrugTarget is anything that's either a Protein or a Receptor
                                {"@id"                  "ex:DrugTarget"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"       "owl:Class"
                                                         "owl:unionOf" {"@list" [{"@id" "ex:Protein"}
                                                                                 {"@id" "ex:Receptor"}]}}}
                                ;; MedicationUser refers to some (Patient OR Participant)
                                {"@id"                  "ex:MedicationUser"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@id" "ex:refersTo"}
                                                         "owl:someValuesFrom" {"@type"       "owl:Class"
                                                                               "owl:unionOf" {"@list" [{"@id" "ex:Patient"}
                                                                                                       {"@id" "ex:Participant"}]}}}}
                                {"@id" "ex:Protein" "@type" "owl:Class"}
                                {"@id" "ex:Receptor" "@type" "owl:Class"}
                                {"@id" "ex:Patient" "@type" "owl:Class"}
                                {"@id" "ex:Participant" "@type" "owl:Class"}
                                {"@id" "ex:refersTo" "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; protein1 is a Protein, should be inferred as DrugTarget
                                     {"@id" "ex:protein1" "@type" "ex:Protein"}
                                   ;; receptor1 is a Receptor, should be inferred as DrugTarget
                                     {"@id" "ex:receptor1" "@type" "ex:Receptor"}
                                   ;; record1 refers to a Patient, should be inferred as MedicationUser
                                     {"@id" "ex:record1"
                                      "ex:refersTo" {"@id" "ex:patient1" "@type" "ex:Patient"}}
                                   ;; record2 refers to a Participant, should be inferred as MedicationUser
                                     {"@id" "ex:record2"
                                      "ex:refersTo" {"@id" "ex:participant1" "@type" "ex:Participant"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Union of named classes"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:protein1"
                                                      "@type" "?type"}}))
                       "ex:DrugTarget")
            "Protein should be inferred as DrugTarget")

        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:receptor1"
                                                      "@type" "?type"}}))
                       "ex:DrugTarget")
            "Receptor should be inferred as DrugTarget"))

      (testing "Union in someValuesFrom restriction"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:record1"
                                                      "@type" "?type"}}))
                       "ex:MedicationUser")
            "Record referring to Patient should be inferred as MedicationUser")

        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:record2"
                                                      "@type" "?type"}}))
                       "ex:MedicationUser")
            "Record referring to Participant should be inferred as MedicationUser")))))

(deftest ^:integration inverse-roles-in-restrictions-test
  (testing "Inverse roles in restrictions and property chains"
    ;; Pattern: ∃R⁻.C means "has something of type C that R's to this"
    ;; Example: Container ≡ ∃contains⁻.Product (anything that contains a Product is a Container)
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/inverse-roles" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; Container is anything that has something contained in it
                                {"@id"                  "ex:Container"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@type"         "owl:ObjectProperty"
                                                                               "owl:inverseOf" {"@id" "ex:containedIn"}}
                                                         "owl:someValuesFrom" {"@id" "ex:Product"}}}
                                ;; Supervised is something that has a Supervisor supervising it
                                {"@id"                  "ex:Supervised"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@type"         "owl:ObjectProperty"
                                                                               "owl:inverseOf" {"@id" "ex:supervises"}}
                                                         "owl:someValuesFrom" {"@id" "ex:Supervisor"}}}
                                ;; Property chain with inverse: hasSiblingItem = containedIn o containedIn⁻
                                ;; This means: things that share the same container
                                {"@id"                    "ex:hasSiblingItem"
                                 "@type"                  "owl:ObjectProperty"
                                 "owl:propertyChainAxiom" {"@list" [{"@id" "ex:containedIn"}
                                                                    {"@type"         "owl:ObjectProperty"
                                                                     "owl:inverseOf" {"@id" "ex:containedIn"}}]}}
                                {"@id" "ex:containedIn" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:supervises" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:Product" "@type" "owl:Class"}
                                {"@id" "ex:Supervisor" "@type" "owl:Class"}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; box1 doesn't explicitly say it's a Container
                                     {"@id" "ex:box1"}
                                   ;; product1 is contained in box1
                                     {"@id" "ex:product1"
                                      "@type" "ex:Product"
                                      "ex:containedIn" {"@id" "ex:box1"}}
                                   ;; employee1 doesn't explicitly say they're Supervised
                                     {"@id" "ex:employee1"}
                                   ;; manager1 supervises employee1
                                     {"@id" "ex:manager1"
                                      "@type" "ex:Supervisor"
                                      "ex:supervises" {"@id" "ex:employee1"}}
                                   ;; For property chain test
                                     {"@id" "ex:item1"
                                      "ex:containedIn" {"@id" "ex:box2"}}
                                     {"@id" "ex:item2"
                                      "ex:containedIn" {"@id" "ex:box2"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Inverse role in someValuesFrom"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:box1"
                                                      "@type" "?type"}}))
                       "ex:Container")
            "box1 should be inferred as Container because product1 is containedIn it")

        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:employee1"
                                                      "@type" "?type"}}))
                       "ex:Supervised")
            "employee1 should be inferred as Supervised because manager1 supervises them"))

      (testing "Inverse role in property chain"
        (is (= #{"ex:item1" "ex:item2"}
               (set @(fluree/query db-reasoned
                                   {:context {"ex" "http://example.org/"}
                                    :select  "?sibling"
                                    :where   {"@id"              "ex:item1"
                                              "ex:hasSiblingItem" "?sibling"}})))
            "Property chain with inverse should infer item1 hasSiblingItem item1 and item2 (things in same container)")))))