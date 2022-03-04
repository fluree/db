(ns gist
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.transact :as jld-tx]
            [fluree.db.json-ld.commit :as jld-commit]
            [fluree.crypto :as crypto]
            [clojure.core.async :as async]))

(def config {:context {"schema" "http://schema.org/"
                       "wiki"   "https://www.wikidata.org/wiki/"}
             :did     {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                       :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                       :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}
             :name    "example"
             :push    (ipfs/default-commit-fn nil)
             ;:publish (ipfs/default-publish-fn nil)
             :read    (ipfs/default-read-fn nil)})

#_(crypto/account-id-from-public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca")

(comment

  (jld-db/blank-db config)

  (-> (jld-db/blank-db config)
      (jld-tx/stage {"@context"                     "https://schema.org",
                        "@id"                       "https://www.wikidata.org/wiki/Q836821",
                        "@type"                     "Movie",
                        "name"                      "HELLO The Hitchhiker's Guide to the Galaxy",
                        "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                        "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                        "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                                     "@type"  "Book",
                                                     "name"   "The Hitchhiker's Guide to the Galaxy",
                                                     "isbn"   "0-330-25864-8",
                                                     "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                               "@type" "Person"
                                                               "name"  "Douglas Adams"}}})
      (jld-commit/db {:message "Initial Commit"}))


  (-> (jld-db/blank-db config)
      (jld-tx/stage {"@context"                     "https://schema.org",
                        "@id"                       "https://www.wikidata.org/wiki/Q836821",
                        "@type"                     "Movie",
                        "name"                      "HELLO The Hitchhiker's Guide to the Galaxy",
                        "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                        "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                        "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                                     "@type"  "Book",
                                                     "name"   "The Hitchhiker's Guide to the Galaxy",
                                                     "isbn"   "0-330-25864-8",
                                                     "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                               "@type" "Person"
                                                               "name"  "Douglas Adams"}}})
      :schema

      )

  (jld-db/load-db)

  (-> (jld-db/blank-db config)
      :context)


  @(fdb/query (async/go (jld-db/load-db "fluree:ipfs:Qmbj6J85V9vGpkTH3uVMLGiZPWDbEgK8N4EZmtWTfMVeWR" config))
              {:select ["*"]
               :from   "https://www.wikidata.org/wiki/Q836821"
               :opts   {:meta true}}
              )

  (def db (jld-db/load-db "fluree:ipfs:Qmbj6J85V9vGpkTH3uVMLGiZPWDbEgK8N4EZmtWTfMVeWR" config))


  @(fdb/query (async/go db)
              {:context {:id "@id"
                         :type "@type"
                         :schema "http://schema.org/"
                         :wiki "https://www.wikidata.org/wiki/"}
               :select  {"?s" ["*", {:schema/isBasedOn ["*"]}]}
               :where   [["?s" "rdf:type" "http://schema.org/Movie"]]}
              )


  (def mashup (merge ledger-a, ledger-b, ledger-c))

  )


