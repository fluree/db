(ns json-ld
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.flakes :as jld-flakes]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]))

(comment

  (def thedb (jld-db/blank-db "hello"))

  (def thedb2 (commit mydb "Added schema data"))

  (push thedb2)                                            ;; immutably stored somewhere




  (def mydb (jld-db/blank-db "hello"))
  (def mydb (jld-tx/transact
              thedb
              {"@context"                  "https://schema.org",
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


  @(fdb/query mydb {:context {"wiki" "https://www.wikidata.org/wiki/"
                              "schema" "http://schema.org/"}
                    :select  ["*", {"isBasedOn" ["*"]}]
                    :from    "https://www.wikidata.org/wiki/Q836821"})

  (def mydb2 (jld-tx/transact mydb {"@context" "https://schema.org",
                                    "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                                 "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                                                 "commentCount" 42}]}))

  (jld-tx/commit mydb)
  (jld-tx/commit mydb2)

  @(fdb/query mydb2 {:select ["*"]
                     :from   "https://www.wikidata.org/wiki/Q836821"
                     })

  (def updated-db (transact orig-db {}))
  ;; publishing goes to (a) consensus for further publishing, (b) write to local system, (c)
  (publish updated-db {:context         {:fluree "https://flur.ee/ns/block"}
                       :id              "some-id-hash?"
                       :type            [:fluree/FlureeBlock]
                       :fluree/method   :ipfs
                       :fluree/snapshot true
                       :fluree/service  "https://yyyyy"
                       :fluree/db       updated-db
                       :ipfs/folder     "x"
                       :ipfs/name       "mydb"})
  ;; => cryptographically signing new block
  ;; => publishing x new transactions
  ;; => db saved: fluree:ipfs:lkjsdflkjdf/mydb
  ;; => updated ledger record
  ;; ===> Consensus multisig achieved
  ;; ===> ipns/ens/cardano root updated
  ;; ===> ledger saved: fluree:ipns:xyzlkjsdflkjsdf/mydb
  ;; ===> Fluree Hub notified


  (def conn (ipfs/connect "bafybeiakxvdwbawhfrus6io233dfqr2tkpzuuawi3yxbxbskqapn7zdt3m"))

  (def ledger (ipfs/new-ledger "ipns:<key>/myledger"))


  (def conn (ipfs/connect {}))
  (def mydb (-> conn
                (jld-db/blank-db "blah" "hi" (atom {}) (fn [] (throw (Exception. "NO CURRENT DB FN YET"))))
                (assoc :t 0)))

  (def tx-res
    (jld-tx/transact
      mydb {"@context"                  "https://schema.org",
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
  (-> tx-res :flakes)


  (def mydb2 (:db-after tx-res))
  @(fdb/query (async/go mydb2)
              {:context "https://schema.org/"
               :select  ["*" {"isBasedOn" ["*"]}]
               :from    "https://www.wikidata.org/wiki/Q836821"})

  (def tx-res2
    (jld-tx/transact
      mydb2 {"@context" "https://schema.org",
             "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                          "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                          "commentCount" 42}]}))

  (:flakes tx-res2)

  (def mydb3 (:db-after tx-res2))
  @(fdb/query (async/go mydb3)
              {:context "https://schema.org/"
               :select  ["*" {"isBasedOn" ["*"]}]
               :from    "https://www.wikidata.org/wiki/Q836821"})



  (jld-tx/wrap-block tx-res)

  (-> tx-res :db-after :schema :pred (get "https://schema.org/isBasedOn"))
  (-> tx-res :db-after :novelty :spot)

  (get-in schema [:pred (.-p flake) :ref?])

  (def mydb2 (:db-after tx-res))

  (-> mydb2 :novelty :spot)
  (flake/match-spot (-> mydb2 :novelty :spot)
                    193514046488576 nil)

  @(fdb/query (async/go mydb2)
              {:context "https://schema.org/"
               :select  ["*" {"isBasedOn" ["*"]}]
               :from    "https://www.wikidata.org/wiki/Q836821"})

  )

(comment
  ;; basic micro-ledger db
  (def tx1 {"@context"                  "https://schema.org",
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
                                                   "name"  "Douglas Adams"}}})

  (def movie-db (ipfs/db "fluree:ipfs:QmWofgUFbvLyqwdmVVKE7K6SQNPrMcigy49cQXYBJm1f2H"))

  (def tx2 {"@context" "https://schema.org",
            "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                         "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                         "commentCount" 42}]})

  (jld-flakes/json-ld-graph->flakes tx2 {})

  (jld-db/with moviedb 2 (jld-flakes))

  "owl:minQualifiedCardinality"

  "owl:qualifiedCardinality"

  )

(comment

  ;; sample movie
  (def movie-db (ipfs/db "fluree:ipfs:QmWofgUFbvLyqwdmVVKE7K6SQNPrMcigy49cQXYBJm1f2H"))
  @(fdb/query movie-db
              {:context "https://schema.org/"
               :select  {"?s" ["*" {"isBasedOn" ["*"]}]}
               :where   [["?s" "a" "Movie"]]})

  )

(comment
  ;; BLOCK FORMATS

  ;; subject only
  {"id"      :TODO
   "tx"      "<>"
   "prev"    "<>"
   "t"       1
   "assert"  [{}]
   "retract" [{}]}

  {"@context"          ["https://www.w3.org/2018/credentials/v1"
                        "https://flur.ee/ns/block/v1"]
   "id"                "http://example.edu/credentials/3732"
   "type"              ["VerifiableCredential", "FlureeBlock"]
   "issuer"            {"id"   "did:example:76e12ec712ebc6f1c221ebfeb1f"
                        "name" "Example Organization"}
   "issuanceDate"      "2021-01-01T19:23:24Z"
   "credentialSubject" {"id"      :TODO
                        "tx"      "<>"
                        "prev"    "<>"
                        "t"       1
                        "assert"  [{}]
                        "retract" [{}]}
   "proof"             {}}

  )