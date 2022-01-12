(ns demo
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.flakes :as jld-flakes]
            [fluree.db.json-ld.transact :as jld-tx]
            [fluree.db.json-ld.commit :as jld-commit]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]))

(comment
  (def config {:context {"schema" "http://schema.org/"
                         "wiki"   "https://www.wikidata.org/wiki/"}
               :did     {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                         :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                         :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}
               :name    "example"
               :push    (ipfs/default-push-fn nil)
               :publish (ipfs/default-publish-fn nil)
               :read    (ipfs/default-read-fn nil)})

  (def loaded-db (jld-db/load-db "fluree:ipns:k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" config))

  loaded-db


  ;; blank db
  (def demodb (jld-db/blank-db config))
  (-> demodb :novelty :spot)

  ;; transaction
  (def demodb2 (jld-tx/transact demodb {"@context"                  "https://schema.org",
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

  (-> demodb2 :novelty :spot)

  ;; commit transaction
  (def mycommit (jld-commit/db demodb2 {:message "Initial commit"}))
  (-> mycommit :db-after :commit)

  (def demodb2 (:db-after mycommit))
  (-> demodb2
      :commit)

  ;; another transaction
  (def demodb3 (jld-tx/transact demodb2
                                {"@context" "https://schema.org",
                                 "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                              "name"         "BRIAN WAS HERE"
                                              "commentCount" 42}]}))

  (-> demodb3
      :commit)

  ;; another commit
  (def mycommit2 (jld-commit/db demodb3 {:message "Another commit"}))
  (-> mycommit2
      :commit)

  )


(comment

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


  (jld-tx/wrap-block tx-res)

  @(fdb/query (async/go (:db-after tx-res))
              {:context "https://schema.org/"
               :select  ["*" {"isBasedOn" ["*"]}]
               :from    "https://www.wikidata.org/wiki/Q836821"})

  (def tx-res2
    (jld-tx/transact (:db-after tx-res)
                     {"@context" "https://schema.org",
                      "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                   "name"         "The Hitchhiker's Guide to the Galaxy (original)"
                                   "commentCount" 42}]}))

  (jld-tx/wrap-block tx-res2)

  )







(comment
  ;; load up any JSON-LD content into queryable DB
  (def mydb (ipfs/db "fluree:ipfs:QmXVzbXNAe7QZDCmiJXqKLjMq6Tu1rRrYgzGezvJ3qyEbH"))

  (-> (async/<!! mydb) :schema :pred)




  @(fdb/query mydb {:context "https://schema.org/"
                    :select  [{"?movies" ["*"]}],
                    :where   [["?movies", "a", "http://schema.org/Movie"]]})





  (async/<!! mydb)

  @(fdb/query mydb {:select [{"?forecast" ["*"]}],
                    :where  [
                             ["?forecast", "https://api.weather.gov/ontology#name", "Tonight"]
                             ]})

  )

(comment
  ;; can use IPNS to update most recent block
  (def myledger (ipfs/db "fluree:s3:bplatz/mydata"))

  ;; block wrapped as a credential
  {"@context"          ["https://www.w3.org/2018/credentials/v1"
                        "https://flur.ee/ns/block"]

   "type"              ["VerifiableCredential", "Block"]
   "issuer"            {"id"   "did:fluree:ipfs:76e12ec712ebc6f1c221ebfeb1f"
                        "name" "Fluree PBC"}
   "issuanceDate"      "2021-01-01T19:23:24Z"
   "credentialSubject" {"id"       "fluree:ipns:data.flur.ee/mydb#2"
                        "tx"       "ipfs/QmP41zGX26H3Sxpohh8xFbuyA1EFSfrMLpzvQHbj7Ss1sS"
                        "service"  ["https://hub.flur.ee/nasa/stars"]
                        "snapshot" "fluree:ipfs:QmP41zGX26H3Sxpohh8xFbuyA1EFSfrMLpzvQHbj7Ss1sS"
                        "block"    2
                        "prev"     "fluree:ipfs:QmP41zGX26H3Sxpohh8xFbuyA1EFSfrMLpzvQHbj7Ss1sS"
                        "assert"   [{"http://schema.org/name"         ["The Hitchhiker's Guide to the Galaxy (original)"],
                                     "http://schema.org/commentCount" [42],
                                     "@id"                            "https://www.wikidata.org/wiki/Q836821"}],
                        "retract"  [{"http://schema.org/name" ["The Hitchhiker's Guide to the Galaxy"],
                                     "@id"                    "https://www.wikidata.org/wiki/Q836821"}]}
   "proof"             {}}

  )

