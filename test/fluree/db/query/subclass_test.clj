(ns fluree.db.query.subclass-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

;; TODO: this is fixed in another branch
(deftest ^:pending subclass-test
  (testing "Subclass queries work."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subclass")
          db1    @(fluree/stage2
                   (fluree/db ledger)
                   {"@context" "https://ns.flur.ee"
                    "insert"
                    {"@context"                  "https://schema.org"
                     "id"                        "https://www.wikidata.org/wiki/Q836821"
                     "type"                      ["Movie"]
                     "name"                      "The Hitchhiker's Guide to the Galaxy"
                     "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings"
                     "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N"
                     "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q3107329"
                                                  "type"   ["Book"]
                                                  "name"   "The Hitchhiker's Guide to the Galaxy"
                                                  "isbn"   "0-330-25864-8"
                                                  "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                            "@type" "Person"
                                                            "name"  "Douglas Adams"}}}})
          ;; add CreativeWork class
          db2    @(fluree/stage2
                   db1
                   {"@context" "https://ns.flur.ee"
                    "insert"
                    {"@context"        {"schema" "http://schema.org/"
                                        "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
                     "@id"             "schema:CreativeWork",
                     "@type"           "rdfs:Class",
                     "rdfs:comment"    "The most generic kind of creative work, including books, movies, photographs, software programs, etc.",
                     "rdfs:label"      "CreativeWork",
                     "rdfs:subClassOf" {"@id" "schema:Thing"},
                     "schema:source"   {"@id" "http://www.w3.org/wiki/WebSchemas/SchemaDotOrgSources#source_rNews"}}})

          ;; Make Book and Movie subclasses of CreativeWork
          db3    @(fluree/stage2
                   db2
                   {"@context" "https://ns.flur.ee"
                    "insert"
                    {"@context" {"schema" "http://schema.org/"
                                 "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
                     "@graph"   [{"@id"             "schema:Book",
                                  "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}
                                 {"@id"             "schema:Movie",
                                  "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}]}})]

      (is (= #{{:id                               :wiki/Q836821,
                :type                             :schema/Movie,
                :schema/name                      "The Hitchhiker's Guide to the Galaxy",
                :schema/disambiguatingDescription "2005 British-American comic science fiction film directed by Garth Jennings",
                :schema/titleEIDR                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                :schema/isBasedOn                 {:id :wiki/Q3107329}}
               {:id            :wiki/Q3107329,
                :type          :schema/Book,
                :schema/name   "The Hitchhiker's Guide to the Galaxy",
                :schema/isbn   "0-330-25864-8",
                :schema/author {:id :wiki/Q42}}}
             (set @(fluree/query db3
                                 {:select {'?s [:*]}
                                  :where  {:id '?s, :type :schema/CreativeWork}})))
          "CreativeWork query should return both Book and Movie"))))

(deftest ^:pending ^:integration subclass-inferencing-test
  (testing "issue core/48"
    (let [conn        (test-utils/create-conn
                       {:context      test-utils/default-str-context
                        :context-type :string})
          ledger-name "subclass-inferencing-test"
          ledger      @(fluree/create conn ledger-name)
          db0         (fluree/db ledger)
          db1         @(fluree/stage2
                        db0
                        {"@context" "https://ns.flur.ee"
                         "insert"
                         [{"@id"         "ex:freddy"
                           "@type"       "ex:Yeti"
                           "schema:name" "Freddy"}
                          {"@id"         "ex:letty"
                           "@type"       "ex:Yeti"
                           "schema:name" "Leticia"}
                          {"@id"         "ex:betty"
                           "@type"       "ex:Yeti"
                           "schema:name" "Betty"}
                          {"@id"         "ex:andrew"
                           "@type"       "schema:Person",
                           "schema:name" "Andrew Johnson"}]})
          db2         @(fluree/stage2
                        db1
                        {"@context" "https://ns.flur.ee"
                         "insert"
                         [{"@id"   "ex:Humanoid"
                           "@type" "rdfs:Class"}
                          {"@id"             "ex:Yeti"
                           "rdfs:subClassOf" {"@id" "ex:Humanoid"}}
                          {"@id"             "schema:Person"
                           "rdfs:subClassOf" {"@id" "ex:Humanoid"}}]})]
      (is (= #{{"id"          "ex:freddy"
                "type"        "ex:Yeti"
                "schema:name" "Freddy"}
               {"id"          "ex:letty"
                "type"        "ex:Yeti"
                "schema:name" "Leticia"}
               {"id"          "ex:betty"
                "type"        "ex:Yeti"
                "schema:name" "Betty"}
               {"id"          "ex:andrew"
                "type"        "schema:Person"
                "schema:name" "Andrew Johnson"}}
             (set @(fluree/query db2 {"where"  {"@id" "?s", "@type" "ex:Humanoid"}
                                      "select" {"?s" ["*"]}})))))))

(deftest ^:pending ^:integration subclass-inferencing-after-load-test
  (testing "issue core/48"
    (let [conn        (test-utils/create-conn
                       {:context      test-utils/default-str-context
                        :context-type :string})
          ledger-name "subclass-inferencing-test"
          ledger      @(fluree/create conn ledger-name)
          db0         (fluree/db ledger)
          db1         @(fluree/stage2
                        db0
                        {"@context" "https://ns.flur.ee"
                         "insert"
                         [{"@id"         "ex:freddy"
                           "@type"       "ex:Yeti"
                           "schema:name" "Freddy"}
                          {"@id"         "ex:letty"
                           "@type"       "ex:Yeti"
                           "schema:name" "Leticia"}
                          {"@id"         "ex:betty"
                           "@type"       "ex:Yeti"
                           "schema:name" "Betty"}
                          {"@id"         "ex:andrew"
                           "@type"       "schema:Person",
                           "schema:name" "Andrew Johnson"}]})
          db2         @(fluree/stage2
                        db1
                        {"@context" "https://ns.flur.ee"
                         "insert"
                         [{"@id"   "ex:Humanoid"
                           "@type" "rdfs:Class"}
                          {"@id"             "ex:Yeti"
                           "rdfs:subClassOf" {"@id" "ex:Humanoid"}}
                          {"@id"             "schema:Person"
                           "rdfs:subClassOf" {"@id" "ex:Humanoid"}}]})
          _db3        @(fluree/commit! ledger db2)
          db4         (-> conn (test-utils/retry-load ledger-name 100) fluree/db)]
      (is (= #{{"id"          "ex:freddy"
                "type"        "ex:Yeti"
                "schema:name" "Freddy"}
               {"id"          "ex:letty"
                "type"        "ex:Yeti"
                "schema:name" "Leticia"}
               {"id"          "ex:betty"
                "type"        "ex:Yeti"
                "schema:name" "Betty"}
               {"id"          "ex:andrew"
                "type"        "schema:Person"
                "schema:name" "Andrew Johnson"}}
             (set @(fluree/query db4 {"where"  {"@id" "?s", "@type" "ex:Humanoid"}
                                      "select" {"?s" ["*"]}})))))))
