(ns fluree.db.reasoner.owl-class-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

;; tests for OWL class reasoning rules

(deftest ^:integration equivalent-class
  (testing "owl:equivalentClass test"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base @(fluree/stage (fluree/db ledger)
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
        (let [db-equiv   @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "xsd" "http://www.w3.org/2001/XMLSchema#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Human",
                                           "@type"               ["owl:Class"],
                                           "owl:equivalentClass" {"@id" "ex:Person"}}])
              qry-person @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Person"}})
              qry-human  @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Human"}})]
          (is (= #{"ex:brian" "ex:laura"}
                 (set qry-person))
              "both ex:brian and ex:laura should be of type ex:Person")

          (is (= #{"ex:brian" "ex:laura"}
                 (set qry-human))
              "both ex:brian and ex:laura should be of type ex:Human")))

      (testing "Testing multiple owl:equivalentClass declaration"
        (let [db-equiv   @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "xsd" "http://www.w3.org/2001/XMLSchema#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Person",
                                           "@type"               ["owl:Class"],
                                           "owl:equivalentClass" [{"@id" "ex:Human"} {"@id" "ex:HumanBeing"}]}])
              qry-person @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Person"}})
              qry-human  @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Human"}})
              qry-humanb @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:HumanBeing"}})]
          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-person))
              "ex:brian, ex:laura and ex:alice should be of type ex:Person")

          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-human))
              "ex:brian, ex:laura and ex:alice should be of type ex:Human")

          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-humanb))
              "ex:brian, ex:laura and ex:alice should be of type ex:HumanBeing"))))))

