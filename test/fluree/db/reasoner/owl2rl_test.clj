(ns fluree.db.reasoner.owl2rl-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

;; base set of data for reasoning tests
(def reasoning-db-data
  {"@context" {"ex" "http://example.org/"}
   "insert"   [{"@id"         "ex:brian"
                "ex:name"     "Brian"
                "ex:uncle"    {"@id" "ex:jim"}
                "ex:sibling"  [{"@id" "ex:laura"} {"@id" "ex:bob"}]
                "ex:children" [{"@id" "ex:alice"}]
                "ex:address"  {"ex:country" {"@id" "ex:Canada"}}
                "ex:age"      42
                "ex:parent"   {"@id"        "ex:carol"
                               "ex:name"    "Carol"
                               "ex:age"     72
                               "ex:address" {"ex:country" {"@id" "ex:Singapore"}}
                               "ex:brother" {"@id" "ex:mike"}}
                "ex:mother"   [{"@id" "ex:carol"} {"@id" "ex:carol-lynn"}]}
               {"@id"     "ex:laura"
                "ex:name" "Laura"}
               {"@id"       "ex:bob"
                "ex:name"   "Bob"
                "ex:gender" {"@id" "ex:Male"}}]})

(deftest ^:integration equality-tests
  (testing "owl equality semantics tests eq-sym and eq-trans"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/basic-owl" nil)
          db-base @(fluree/stage (fluree/db ledger) reasoning-db-data)]
      (testing "Testing explicit owl:sameAs declaration"
        (let [db-same     @(fluree/stage db-base
                                         {"@context" {"ex"   "http://example.org/"
                                                      "owl"  "http://www.w3.org/2002/07/owl#"
                                                      "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                          "insert"   {"@id"        "ex:carol"
                                                      "owl:sameAs" {"@id" "ex:carol-lynn"}}})

              db-reasoned @(fluree/reason db-same :owl2rl)

              qry-sameAs  @(fluree/query db-reasoned
                                         {:context {"ex"  "http://example.org/"
                                                    "owl" "http://www.w3.org/2002/07/owl#"}
                                          :select  "?same"
                                          :where   {"@id"        "ex:carol-lynn",
                                                    "owl:sameAs" "?same"}})]

          (is (= #{"ex:carol"}
                 (set qry-sameAs))
              "ex:carol-lynn should be deemed the same as ex:carol")))

      (testing "Testing owl:sameAs passed along as a reasoned rule"
        (let [db-reasoned @(fluree/reason db-base :owl2rl
                                          {"@context"   {"ex"  "http://example.org/"
                                                         "owl" "http://www.w3.org/2002/07/owl#"}
                                           "@id"        "ex:carol"
                                           "owl:sameAs" {"@id" "ex:carol-lynn"}})
              qry-sameAs  @(fluree/query db-reasoned
                                         {:context {"ex"  "http://example.org/"
                                                    "owl" "http://www.w3.org/2002/07/owl#"}
                                          :select  "?same"
                                          :where   {"@id"        "ex:carol-lynn",
                                                    "owl:sameAs" "?same"}})]


          (is (= #{"ex:carol"}
                 (set qry-sameAs))
              "ex:carol-lynn should be deemed the same as ex:carol")))


      (testing "Testing owl:sameAs transitivity (eq-trans)"
        (let [db-same     @(fluree/stage db-base
                                         {"@context" {"ex"   "http://example.org/"
                                                      "owl"  "http://www.w3.org/2002/07/owl#"
                                                      "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                          "insert"   [{"@id"        "ex:carol"
                                                       "owl:sameAs" {"@id" "ex:carol1"}}
                                                      {"@id"        "ex:carol1"
                                                       "owl:sameAs" {"@id" "ex:carol2"}}
                                                      {"@id"        "ex:carol2"
                                                       "owl:sameAs" {"@id" "ex:carol3"}}
                                                      {"@id"        "ex:carol3"
                                                       "owl:sameAs" {"@id" "ex:carol4"}}]})
              db-reasoned @(fluree/reason db-same :owl2rl)
              qry-carol   @(fluree/query db-reasoned
                                         {:context {"ex"  "http://example.org/"
                                                    "owl" "http://www.w3.org/2002/07/owl#"}
                                          :select  "?same"
                                          :where   {"@id"        "ex:carol",
                                                    "owl:sameAs" "?same"}})
              qry-carol4  @(fluree/query db-reasoned
                                         {:context {"ex"  "http://example.org/"
                                                    "owl" "http://www.w3.org/2002/07/owl#"}
                                          :select  "?same"
                                          :where   {"@id"        "ex:carol4",
                                                    "owl:sameAs" "?same"}})]


          (is (= #{"ex:carol1" "ex:carol2" "ex:carol3" "ex:carol4"}
                 (set qry-carol))
              "ex:carol should be sameAs all other carols")

          (is (= #{"ex:carol" "ex:carol1" "ex:carol2" "ex:carol3"}
                 (set qry-carol4))
              "ex:carol4 should be sameAs all other carols"))))))


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
                              "@id"         "ex:parent"
                              "@type"       ["owl:ObjectProperty"]
                              "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"}]}])
              qry-subj   @(fluree/query db-prp-dom
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?t"
                                         :where   {"@id"   "ex:brian",
                                                   "@type" "?t"}})
              qry-types  @(fluree/query db-prp-dom
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Child"}})]


          (is (= #{"ex:Child" "ex:Person"}
                 (set qry-subj))
              "ex:brian should be of type ex:Person and ex:Child")

          (is (= #{"ex:brian"}
                 (set qry-types))
              "ex:brian should be the only subject of type ex:Child")))

      (testing "Testing rdfs:range - rule: prp-rng"
        (let [db-prp-rng @(fluree/reason
                            db-base :owl2rl
                            [{"@context"   {"ex"   "http://example.org/"
                                            "owl"  "http://www.w3.org/2002/07/owl#"
                                            "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                              "@id"        "ex:parent"
                              "@type"      ["owl:ObjectProperty"]
                              "rdfs:range" [{"@id" "ex:Person"} {"@id" "ex:Parent"}]}])
              qry-subj   @(fluree/query db-prp-rng
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?t"
                                         :where   {"@id"   "ex:carol",
                                                   "@type" "?t"}})
              qry-parent @(fluree/query db-prp-rng
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Parent"}})]


          (is (= #{"ex:Parent" "ex:Person"}
                 (set qry-subj))
              "ex:carol should be of type ex:Person and ex:Parent")

          (is (= #{"ex:carol"}
                 (set qry-parent))
              "ex:carol should be the only subject of type ex:Parent")))


      (testing "Testing multiple rules rdfs:domain + rdfs:range - rules: prp-dom & prp-rng"
        (let [db-prp-dom+rng @(fluree/reason
                                db-base :owl2rl
                                [{"@context"    {"ex"   "http://example.org/"
                                                 "owl"  "http://www.w3.org/2002/07/owl#"
                                                 "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                  "@id"         "ex:parent"
                                  "@type"       ["owl:ObjectProperty"]
                                  "rdfs:domain" [{"@id" "ex:Person"} {"@id" "ex:Child"} {"@id" "ex:Human"}]
                                  "rdfs:range"  [{"@id" "ex:Person"} {"@id" "ex:Parent"}]}])
              qry-child      @(fluree/query db-prp-dom+rng
                                            {:context {"ex" "http://example.org/"}
                                             :select  "?s"
                                             :where   {"@id"   "?s",
                                                       "@type" "ex:Child"}})
              qry-parent     @(fluree/query db-prp-dom+rng
                                            {:context {"ex" "http://example.org/"}
                                             :select  "?s"
                                             :where   {"@id"   "?s",
                                                       "@type" "ex:Parent"}})
              qry-person     @(fluree/query db-prp-dom+rng
                                            {:context {"ex" "http://example.org/"}
                                             :select  "?s"
                                             :where   {"@id"   "?s",
                                                       "@type" "ex:Person"}})]

          (is (= #{"ex:brian"}
                 (set qry-child))
              "ex:brian should be the only subject of type ex:Child")

          (is (= #{"ex:carol"}
                 (set qry-parent))
              "ex:carol should be the only subject of type ex:Parent")

          (is (= #{"ex:brian" "ex:carol"}
                 (set qry-person))
              "ex:brian and ex:carol should be of type ex:Person"))))))

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
                                "@type"    ["owl:ObjectProperty" "owl:SymetricProperty"]}])
              qry-sameAs   @(fluree/query db-prp-symp
                                          {:context {"ex" "http://example.org/"}
                                           :select  "?x"
                                           :where   {"@id"          "ex:person-b",
                                                     "ex:livesWith" "?x"}})]

          (is (= #{"ex:person-a"}
                 (set qry-sameAs))
              "ex:person-b should also live with ex:person-a"))))))

