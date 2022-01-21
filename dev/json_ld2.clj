(ns json-ld2
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.flakes :as jld-flakes]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.commit :as jld-commit]
            [fluree.json-ld :as json-ld]
            [alphabase.core :as alphabase]
            [fluree.db.util.json :as json]))



(comment

  (def default-methods {:ipfs {:endpoint "http://127.0.0.1:5001/"}
                        :s3   {:access-key ""
                               :region     ""}})


  (def config {:context {"schema" "http://schema.org/"
                         "wiki"   "https://www.wikidata.org/wiki/"}
               :did     {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                         :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                         :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}
               :name    "example"
               :push    (ipfs/default-push-fn nil)
               :publish (ipfs/default-publish-fn nil)
               :read    (ipfs/default-read-fn nil)})

  (def rdb (jld-db/load-db "fluree:ipns:k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" config))
rdb
  @(fdb/query rdb {:context "https://schema.org"
                   :select ["*", {"author" ["*"]}]
                   :from "https://www.wikidata.org/wiki/Q3107329"})
  (-> rdb :novelty :spot)

  (def mydb (jld-db/blank-db config))
  mydb
  (-> mydb :config)
  (-> mydb :schema :pred vals set)

  (def mydb2 (jld-tx/transact mydb {"@context"                  "https://schema.org",
                                    "@id"                       "https://www.wikidata.org/wiki/Q836821",
                                    "@type"                     "Movie",
                                    "name"                      "The Hitchhiker's Guide to the Galaxy",
                                    "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                    "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                    "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                                                 "@type"  "Book",
                                                                 "name"   "The Hitchhiker's Guide to the Galaxy",
                                                                 "isbn"   "0-330-25864-8",
                                                                 "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                                           "@type" "Person"
                                                                           "name"  "Douglas Adams"}}}))

  mydb2
  (-> mydb2 :config)
  (-> mydb2 :novelty :spot)
  (-> mydb2 :schema :pred vals set)

  (def mycommit (jld-commit/db mydb2 {:message "Initial commit"}))

  (def mydb3 (jld-tx/transact (:db-after mycommit)
                              {"@context" "https://schema.org",
                               "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                            "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                                            "commentCount" 42}]}))

  mydb3
  (def mycommit2 (jld-commit/db mydb3 {:message "Another commit"}))

  mycommit2

  {:id      "@id",
   "type"   "@type",
   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
   "schema" "http://schema.org/",
   "wiki"   "https://www.wikidata.org/wiki/"}


  {"@context"          ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"],
   "id"                "blah",
   "type"              ["VerifiableCredential" "Commit"],
   "issuer"            "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
   "issuanceDate"      "SOMEDATE",
   "prev"              "fluree:ipfs:kljljlkjkl"
   "service"           ["https://hub.flur.ee/my/account"]
   "snapshot"          "TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
   "origin"            "lkjljlkj"
   "governance"        {"rules" ["smartfunctions" "schema", "3of5 dids"]}
   "credentialSubject" {"@context" ["https://flur.ee/ns/block"
                                    {"id"               "@id",
                                     "type"             "@type",
                                     "rdfs"             "http://www.w3.org/2000/01/rdf-schema#",
                                     "schema"           "http://schema.org/",
                                     "wiki"             "https://www.wikidata.org/wiki/",
                                     "schema:isBasedOn" {"@type" "@id"},
                                     "schema:author"    {"@type" "@id"}}],
                        "type"     ["Commit"],
                        "branch"   "main",
                        "t"        2000,
                        "message"  "Initial commit",
                        "assert"   [{"type" "rdfs:Class", "id" "schema:Movie"}
                                    {"type" "rdfs:Class", "id" "schema:Book"}
                                    {"type" "rdfs:Class", "id" "schema:Person"}
                                    {"schema:isBasedOn"                 "wiki:Q3107329",
                                     "schema:titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                     "schema:disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                     "schema:name"                      "The Hitchhiker's Guide to the Galaxy",
                                     "type"                             "schema:Movie",
                                     "id"                               "wiki:Q836821"}
                                    {"schema:author" "wiki:Q42",
                                     "schema:isbn"   "0-330-25864-8",
                                     "schema:name"   "The Hitchhiker's Guide to the Galaxy",
                                     "type"          "schema:Book",
                                     "id"            "wiki:Q3107329"}
                                    {"schema:name" "Douglas Adams", "type" "schema:Person", "id" "wiki:Q42"}]},
   "proof"             {}}



  (def mydb3 (jld-tx/transact mydb2 {"@context" "https://schema.org",
                                     "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                                  "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                                                  "commentCount" 42}]}))

  (jld-commit/db mydb3 {:message "Another commit"})


  (def cr (jld-commit/db mydb2))
  cr

  )


(comment

  (String. (alphabase/base64->bytes "eyJoaSI6ICJ0aGVyZSIKICJ3aXRoIjogImxpbmUgYnJlYWsifQ=="))

  (json/stringify {"hi"   "there"
                   "with" "line break"})

  (json-ld/expand {"@context"          ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"],
                   "id"                "blah",
                   "type"              ["VerifiableCredential" "Commit"],
                   "issuer"            "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
                   "issuanceDate"      "SOMEDATE",
                   "credentialSubject" {"@context" ["https://flur.ee/ns/block"
                                                    {"id"               "@id",
                                                     "type"             "@type",
                                                     "rdfs"             "http://www.w3.org/2000/01/rdf-schema#",
                                                     "schema"           "http://schema.org/",
                                                     "wiki"             "https://www.wikidata.org/wiki/",
                                                     "schema:isBasedOn" {"@type" "@id"},
                                                     "schema:author"    {"@type" "@id"}}],
                                        "type"     ["Commit"],
                                        "branch"   "main",
                                        "t"        1,
                                        "message"  "Initial commit",
                                        "assert"   [{"type" "rdfs:Class", "id" "schema:Movie"}
                                                    {"type" "rdfs:Class", "id" "schema:Book"}
                                                    {"type" "rdfs:Class", "id" "schema:Person"}
                                                    {"schema:isBasedOn"                 "wiki:Q3107329",
                                                     "schema:titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                                     "schema:disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                                     "schema:name"                      "The Hitchhiker's Guide to the Galaxy",
                                                     "type"                             "schema:Movie",
                                                     "id"                               "wiki:Q836821"}
                                                    {"schema:author" "wiki:Q42",
                                                     "schema:isbn"   "0-330-25864-8",
                                                     "schema:name"   "The Hitchhiker's Guide to the Galaxy",
                                                     "type"          "schema:Book",
                                                     "id"            "wiki:Q3107329"}
                                                    {"schema:name" "Douglas Adams", "type" "schema:Person", "id" "wiki:Q42"}]},
                   "proof"             {}}
                  )

  (json/parse "{\n           \"\\u20ac\": \"Euro Sign\",\n                   \"\\r\": \"Carriage Return\",\n           \"\\u000a\": \"Newline\",\n                   \"1\": \"One\",\n           \"\\u0080\": \"Control\\u007f\",\n                   \"\\ud83d\\ude02\": \"Smiley\",\n           \"\\u00f6\": \"Latin Small Letter O With Diaeresis\",\n                   \"\\ufb33\": \"Hebrew Letter Dalet With Dagesh\",\n           \"</script>\": \"Browser Challenge\"\n           }"
              false)

  )