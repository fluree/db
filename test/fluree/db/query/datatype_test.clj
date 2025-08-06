(ns fluree.db.query.datatype-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util :refer [exception?]]))

(def context-edn
  {:id     "@id"
   :type   "@type"
   :schema "http://schema.org/"
   :ex     "http://example.org/ns/"
   :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
   :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})

(deftest ^:integration mixed-datatypes-test
  (let [conn   (test-utils/create-conn)
        db0 @(fluree/create conn "ledger/datatype")]
    (testing "Querying predicates with mixed datatypes"
      (let [mixed-db @(fluree/update db0
                                     {"insert"
                                      [{:context     context-edn
                                        :id          :ex/coco
                                        :type        :schema/Person
                                        :schema/name "Coco"}
                                       {:context     context-edn
                                        :id          :ex/halie
                                        :type        :schema/Person
                                        :schema/name "Halie"}
                                       {:context     context-edn
                                        :id          :ex/john
                                        :type        :schema/Person
                                        :schema/name 3}]})]
        (is (= [{:id          :ex/halie
                 :type        :schema/Person
                 :schema/name "Halie"}]
               @(fluree/query mixed-db
                              {:context context-edn
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name "Halie"}}))
            "only returns the data type queried")
        (is (= []
               @(fluree/query mixed-db
                              {:context context-edn
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name "a"}}))
            "does not return results without matching subjects")
        (is (= [{:id          :ex/john
                 :type        :schema/Person
                 :schema/name 3}]
               @(fluree/query mixed-db
                              {:context context-edn
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name 3}}))
            "only returns the data type queried")))))

(deftest ^:integration datatype-test
  (testing "querying with datatypes"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "people")
          db     @(fluree/update
                   db0
                   {"@context" [test-utils/default-context
                                {:ex    "http://example.org/ns/"
                                 :value "@value"
                                 :type  "@type"}]
                    "insert"
                    [{:id      :ex/homer
                      :ex/name "Homer"
                      :ex/age  36}
                     {:id      :ex/marge
                      :ex/name "Marge"
                      :ex/age  {:value 36
                                :type  :xsd/int}}
                     {:id      :ex/bart
                      :ex/name "Bart"
                      :ex/age  "forever 10"}]})]
      (testing "with literal values"
        (testing "specifying an explicit data type"
          (testing "compatible with the value"
            (let [query   {:context [test-utils/default-context
                                     {:ex    "http://example.org/ns/"
                                      :value "@value"
                                      :type  "@type"}]
                           :select  '[?name]
                           :where   '{:ex/name ?name
                                      :ex/age  {:value 36
                                                :type  :xsd/int}}}
                  results @(fluree/query db query)]
              (is (= [["Marge"]] results)
                  "should only return the matching items with the specified type")))
          (testing "not compatible with the value"
            (let [query   {:context [test-utils/default-context
                                     {:ex    "http://example.org/ns/"
                                      :value "@value"
                                      :type  "@type"}]
                           :select  '[?name]
                           :where   '{:ex/name ?name
                                      :ex/age  {:value 36
                                                :type  :xsd/string}}}
                  results @(fluree/query db query)]
              (is (exception? results)
                  "should return an error")))))
      (testing "bound to variables in 'bind' patterns"
        (testing "included datatype in query results"
          (let [query   {:context [test-utils/default-context
                                   {:ex "http://example.org/ns/"}]
                         :select  '[?name ?age ?dt]
                         :where   '[{:ex/name ?name
                                     :ex/age  ?age}
                                    [:bind ?dt (datatype ?age)]]}
                results @(fluree/query db query)]
            (is (= [["Bart" "forever 10" :xsd/string]
                    ["Homer" 36 :xsd/integer]
                    ["Marge" 36 :xsd/int]]
                   results))))
        (testing "filtered with the datatype function"
          (let [query   {:context [test-utils/default-context
                                   {:ex "http://example.org/ns/"}]
                         :select  '[?name ?age ?dt]
                         :where   '[{:ex/name ?name
                                     :ex/age  ?age}
                                    [:bind ?dt (datatype ?age)]
                                    [:filter (= (iri :xsd/integer) ?dt)]]}
                results @(fluree/query db query)]
            (is (= [["Homer" 36 :xsd/integer]]
                   results)))))
      (testing "filtered in value maps"
        (testing "with explicit type IRIs"
          (let [query   {:context [test-utils/default-context
                                   {:ex    "http://example.org/ns/"
                                    :value "@value"
                                    :type  "@type"}]
                         :select  '[?name ?age]
                         :where   '[{:ex/name ?name
                                     :ex/age  {:value ?age
                                               :type  :xsd/string}}]}
                results @(fluree/query db query)]
            (is (= [["Bart" "forever 10"]]
                   results))))
        (testing "with variable types"
          (let [query   {:context [test-utils/default-context
                                   {:ex    "http://example.org/ns/"
                                    :value "@value"
                                    :type  "@type"}]
                         :select  '[?name ?age ?ageType]
                         :where   '[{:ex/name ?name
                                     :ex/age  {:value ?age
                                               :type  ?ageType}}
                                    [:bind ?ageType (iri :xsd/int)]]}
                results @(fluree/query db query)]
            (is (= [["Marge" 36 :xsd/int]]
                   results))))))))

(deftest ^:integration json-datatype-test
  (testing "querying with @json datatype"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "json-test")
          db     @(fluree/update
                   db0
                   {"@context" {"ex"    "http://example.org/ns/"}
                    "insert"
                    [{"id"      "ex:doc1"
                      "ex:name" "Document 1"
                      "ex:data" {"@value" {"name" "John" "age" 30}
                                 "@type"  "@json"}}
                     {"id"      "ex:doc2"
                      "ex:name" "Document 2"
                      "ex:data" "plain string data"}]})]
      (testing "retrieving @json datatype using bind"
        (let [query   {"@context" {"ex" "http://example.org/ns/"
                                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
                       "select"  ["?name" "?data" "?dt"]
                       "where"   [{"ex:name" "?name"
                                   "ex:data" "?data"}
                                  ["bind" "?dt" "(datatype ?data)"]]}
              results @(fluree/query db query)]
          (is (= 2 (count results))
              "should return both documents")
          (is (some #(= ["Document 1" {"age" 30 "name" "John"} "@json"] %) results)
              "should include Document 1 with @json datatype")
          (is (some #(= ["Document 2" "plain string data" "xsd:string"] %) results)
              "should include Document 2 with string datatype")))
      (testing "filtering by @json datatype"
        (let [query   {"@context" {"ex" "http://example.org/ns/"}
                       "select"  ["?name" "?data"]
                       "where"   [{"ex:name" "?name"
                                   "ex:data" "?data"}
                                  ["bind" "?dt" "(datatype ?data)"]
                                  ["filter" "(= \"@json\" ?dt)"]]}
              results @(fluree/query db query)]
          (is (= 1 (count results))
              "should return only document with @json datatype")
          (is (= [["Document 1" {"age" 30 "name" "John"}]] results)
              "should return the correct document"))))))

;; Note this test is syntax for binding the @type directly to a variable that we discussed supporting but not currently working
(deftest ^:integration value-type-binding-test
  (testing "querying with @value and @type variable bindings"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "value-type-test")
          db     @(fluree/update
                   db0
                   {"@context" {"ex"  "http://example.org/ns/"
                                "xsd" "http://www.w3.org/2001/XMLSchema#"}
                    "insert"
                    [{"@id"     "ex:homer"
                      "ex:name" "Homer"
                      "ex:age"  36}
                     {"@id"     "ex:marge"
                      "ex:name" "Marge"
                      "ex:age"  {"@value" 36
                                 "@type"  "xsd:int"}}
                     {"@id"     "ex:bart"
                      "ex:name" "Bart"
                      "ex:age"  "forever 10"}]})]
      (testing "binding @type to a variable in JSON query"
        (let [query   {"@context" {"ex"  "http://example.org/ns/"
                                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
                       "select"  ["?name" "?age" "?ageType"]
                       "where"   [{"ex:name" "?name"
                                   "ex:age"  {"@value" "?age"
                                              "@type"  "?ageType"}}]}
              results @(fluree/query db query)]
          (is (= [["Bart" "forever 10" "xsd:string"]
                  ["Homer" 36 "xsd:integer"]
                  ["Marge" 36 "xsd:int"]]
                 results)))))))

(deftest ^:integration language-binding-test
  (testing "language binding with lang function"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "lang-test")
          db     @(fluree/update
                   db0
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"      "ex:greeting"
                      "ex:hello" {"@value" "Hello" "@language" "en"}}
                     {"@id"      "ex:salutation"
                      "ex:hello" {"@value" "Bonjour" "@language" "fr"}}]})]
      (testing "binding language to a variable with lang function"
        (let [query {"@context" {"ex" "http://example.org/ns/"}
                     "select"  ["?id" "?val" "?lang"]
                     "where"   [{"@id" "?id"
                                 "ex:hello" "?val"}
                                ["bind" "?lang" "(lang ?val)"]]}
              results @(fluree/query db query)]
          (is (= #{["ex:greeting" "Hello" "en"]
                   ["ex:salutation" "Bonjour" "fr"]}
                 (set results))
              "lang function should bind language correctly")))
      (testing "binding language to a variable with lang function"
        (let [query {"@context" {"ex" "http://example.org/ns/"}
                     "select"  ["?id" "?val" "?lang"]
                     "where"   [{"@id" "?id"
                                 "ex:hello" {"@value" "?val"
                                             "@language" "?lang"}}]}
              results @(fluree/query db query)]
          (is (= #{["ex:greeting" "Hello" "en"]
                   ["ex:salutation" "Bonjour" "fr"]}
                 (set results))
              "lang function should bind language correctly"))))))

(deftest ^:integration transaction-binding-test
  (testing "transaction (@t) binding with t as a variable"
    (let [conn   (test-utils/create-conn)
          db0    @(fluree/create conn "t-test")
          ;; First transaction
          db1    @(fluree/update
                   db0
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:alice"
                      "ex:name" "Alice"
                      "ex:age"  30}]})
          db1*   @(fluree/commit! conn db1)
          ;; Second transaction
          db2    @(fluree/update
                   db1*
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"      "ex:alice"
                      "ex:hobby" "Reading"}]})
          db2*   @(fluree/commit! conn db2)
          ;; Third transaction
          db3    @(fluree/update
                   db2*
                   {"@context" {"ex" "http://example.org/ns/"}
                    "insert"
                    [{"@id"     "ex:alice"
                      "ex:city" "Boston"}]})
          db3*   @(fluree/commit! conn db3)]
      (testing "binding transaction number to a variable"
        (let [query {"@context" {"ex" "http://example.org/ns/"}
                     "select"  ["?p" "?o" "?t"]
                     "where"   [{"@id" "ex:alice"
                                 "?p"  {"@value" "?o"
                                        "@t"     "?t"}}]}
              results @(fluree/query db3* query)]
          (is (= 4 (count results))
              "should return all predicates with their transaction numbers")
          (is (= #{1 2 3} (set (map #(nth % 2) results)))
              "should have data from 3 different transactions")
          ;; Verify specific data
          (is (some #(= ["ex:name" "Alice" 1] %) results)
              "name should be from transaction 1")
          (is (some #(= ["ex:age" 30 1] %) results)
              "age should be from transaction 1")
          (is (some #(= ["ex:hobby" "Reading" 2] %) results)
              "hobby should be from transaction 2")
          (is (some #(= ["ex:city" "Boston" 3] %) results)
              "city should be from transaction 3"))))))
