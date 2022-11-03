(ns fluree.db.query.subclass-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest subclass-test
  (testing "Subclass queries work."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subclass")
          db1    @(fluree/stage
                    ledger
                    {"@context"                  "https://schema.org",
                     "id"                        "https://www.wikidata.org/wiki/Q836821",
                     "type"                      ["Movie"],
                     "name"                      "The Hitchhiker's Guide to the Galaxy",
                     "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                     "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                     "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q3107329",
                                                  "type"   ["Book"],
                                                  "name"   "The Hitchhiker's Guide to the Galaxy",
                                                  "isbn"   "0-330-25864-8",
                                                  "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                            "@type" "Person"
                                                            "name"  "Douglas Adams"}}})
          ;; add CreativeWork class
          db2    @(fluree/stage
                    db1
                    {"@context"        {"schema" "http://schema.org/"
                                        "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
                     "@id"             "schema:CreativeWork",
                     "@type"           "rdfs:Class",
                     "rdfs:comment"    "The most generic kind of creative work, including books, movies, photographs, software programs, etc.",
                     "rdfs:label"      "CreativeWork",
                     "rdfs:subClassOf" {"@id" "schema:Thing"},
                     "schema:source"   {"@id" "http://www.w3.org/wiki/WebSchemas/SchemaDotOrgSources#source_rNews"}})

          ;; Make Book and Movie subclasses of CreativeWork
          db3    @(fluree/stage
                    db2
                    {"@context" {"schema" "http://schema.org/"
                                 "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
                     "@graph"   [{"@id"             "schema:Book",
                                  "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}
                                 {"@id"             "schema:Movie",
                                  "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}]})]

      (is (= @(fluree/query db3
                            {:select {'?s [:*]}
                             :where  [['?s :rdf/type :schema/CreativeWork]]})
             [{:id                               :wiki/Q836821,
               :rdf/type                         [:schema/Movie],
               :schema/name                      "The Hitchhiker's Guide to the Galaxy",
               :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
               :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
               :schema/isBasedOn                 {:id :wiki/Q3107329}}
              {:id            :wiki/Q3107329,
               :rdf/type      [:schema/Book],
               :schema/name   "The Hitchhiker's Guide to the Galaxy",
               :schema/isbn   "0-330-25864-8",
               :schema/author {:id :wiki/Q42}}])
          "CreativeWork query should return both Book and Movie"))))
