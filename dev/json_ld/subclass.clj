(ns json-ld.subclass
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.did :as did]
            [fluree.db.connection :as connection]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]))

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     ;; ledger defaults used for newly created ledgers
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
                                :indexer {:reindex-min-bytes 100
                                          :reindex-max-bytes 10000000}
                                :context {:id     "@id"
                                          :type   "@type"
                                          :schema "http://schema.org/"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                          :wiki   "https://www.wikidata.org/wiki/"
                                          :skos   "http://www.w3.org/2008/05/skos#"
                                          :f      "https://ns.flur.ee/ledger#"}
                                :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))


  (def ledger @(fluree/create ipfs-conn "test/db1" {}))

  (def newdb
    @(fluree/stage
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
                                               "name"  "Douglas Adams"}}}))

  @(fluree/query newdb
                 {:select {'?s [:*]}
                  :where  [['?s :type :schema/Book]]})


  ;; add CreativeWork class
  (def db2 @(fluree/stage
              newdb
              {"@context"        {"schema" "http://schema.org/"
                                  "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
               "@id"             "schema:CreativeWork",
               "@type"           "rdfs:Class",
               "rdfs:comment"    "The most generic kind of creative work, including books, movies, photographs, software programs, etc.",
               "rdfs:label"      "CreativeWork",
               "rdfs:subClassOf" {"@id" "schema:Thing"},
               "schema:source"   {"@id" "http://www.w3.org/wiki/WebSchemas/SchemaDotOrgSources#source_rNews"}}))


  ;; Make Book and Movie subclasses of CreativeWork
  (def db3 @(fluree/stage
              db2
              {"@context" {"schema" "http://schema.org/"
                           "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"}
               "@graph"   [{"@id"             "schema:Book",
                            "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}
                           {"@id"             "schema:Movie",
                            "rdfs:subClassOf" {"@id" "schema:CreativeWork"}}]}
              ))

  ;; Query for CreativeWork
  @(fluree/query db3
                 {:select {'?s [:*]}
                  :where  [['?s :type :schema/CreativeWork]]})

  )
