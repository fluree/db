(ns json-ld-combine
  (:require [fluree.db :as fluree]
            [fluree.db.did :as did]))

;; dev namespace for combining ledgers/dbs using :include option

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
                                :context {:id     "@id"
                                          :type   "@type"
                                          :schema "http://schema.org/"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                          :wiki   "https://www.wikidata.org/wiki/"
                                          :skos   "http://www.w3.org/2008/05/skos#"
                                          :f      "https://ns.flur.ee/ledger#"}
                                :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))

  (def books-ledger
    (let [ledger @(fluree/create ipfs-conn "cmb/books")
          newdb  @(fluree/stage
                    ledger
                    {"@context" "https://schema.org",
                     "id"       "https://www.wikidata.org/wiki/Q3107329",
                     "type"     "Book",
                     "name"     "The Hitchhiker's Guide to the Galaxy",
                     "isbn"     "0-330-25864-8",
                     "author"   {"@id"   "https://www.wikidata.org/wiki/Q42"
                                 "@type" "Person"
                                 "name"  "Douglas Adams"}})]
      (-> @(fluree/commit! newdb {:message "First commit!"
                                  :push?   true}))
      ledger))

  (def movies-ledger
    (let [ledger @(fluree/create ipfs-conn "cmb/movies")
          newdb  @(fluree/stage
                    ledger
                    {"@context"                  "https://schema.org",
                     "id"                        "https://www.wikidata.org/wiki/Q836821",
                     "type"                      ["Movie"],
                     "name"                      "The Hitchhiker's Guide to the Galaxy",
                     "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                     "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N"
                     "isBasedOn"                 "https://www.wikidata.org/wiki/Q3107329"})]
      (-> @(fluree/commit! newdb {:message "First commit!"
                                  :push?   true}))
      ledger))

  @(fluree/query (fluree/db movies-ledger)
                 {:select [:*]
                  :from   :wiki/Q836821})

  ;; you cannot crawl additional data with isBasedOn in the movies-ledger - as we created the DB with just an IRI and no book data
  @(fluree/query (fluree/db movies-ledger)
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})

  ;; create a new ledger that :include both the books and movies ledger (use fluree/status to get your ledger identities for inclusion)
  (fluree/status books-ledger)
  (fluree/status movies-ledger)

  (def ledger-cmb
    @(fluree/create ipfs-conn "cmb/all"
                    {:ipns    {:key "Fluree1"}
                     :include ["fluree:ipns://data.fluree.com/cmb/books"
                               "fluree:ipns://data.fluree.com/cmb/movies"]}))

  ;; now we can crawl from the movie, into the book, and into the author
  @(fluree/query (fluree/db ledger-cmb)
                 {:select [:* {:schema/isBasedOn [:* {:schema/author [:*]}]}]
                  :from   :wiki/Q836821})

  )
