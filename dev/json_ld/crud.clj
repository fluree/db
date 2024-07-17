(ns json-ld.crud
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]))


(comment

  (def ipfs-conn
    @(fluree/connect-ipfs
       {:server   nil                                       ;; use default
        ;; ledger defaults used for newly created ledgers
        :defaults {:ipns    {:key "self"}                   ;; publish to ipns by default using the provided key/profile
                   :indexer {:reindex-min-bytes 9000
                             :reindex-max-bytes 10000000}
                   :context {:id     "@id"
                             :type   "@type"
                             :xsd    "http://www.w3.org/2001/XMLSchema#"
                             :schema "http://schema.org/"
                             :sh     "http://www.w3.org/ns/shacl#"
                             :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                             :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                             :wiki   "https://www.wikidata.org/wiki/"
                             :skos   "http://www.w3.org/2008/05/skos#"
                             :f      "https://ns.flur.ee/ledger#"}
                   :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))

  (def ledger @(fluree/create ipfs-conn "crud/a" {:context {:ex "http://example.org/ns/"}}))

  ;; should work OK
  (def db
    @(fluree/stage
       ledger
       [{:where        "HI"
         :id           :ex/alice,
         :type         :ex/User,
         :schema/name  "Alice"
         :schema/email "alice@flur.ee"
         :schema/age   42}
        {:id          :ex/bob,
         :type        :ex/User,
         :schema/name "Bob"
         :schema/age  22}
        {:id           :ex/jane,
         :type         :ex/User,
         :schema/name  "Jane"
         :schema/email "jane@flur.ee"
         :schema/age   30}]))


  @(fluree/query db {:select [:*]
                     :from   :ex/alice})

  @(fluree/query db {:select {'?s [:*]}
                     :where  [['?s :type :ex/User]]})

  @(fluree/query db {:select ['?p '?o]
                     :where  [[:ex/alice '?p '?o]]})

  ;;;;;;;;;;;;;;;;
  ;; delete everything for :ex/alice
  (def db-subj-delete
    @(fluree/stage db
                   {:delete [:ex/alice '?p '?o]
                    :where  [[:ex/alice '?p '?o]]}))

  @(fluree/query
     db-subj-delete
     {:select '?name
      :where  [['?s :schema/name '?name]]})

  ;;;;;;;;;;;;;;;;
  ;; delete any :schema/age values for :ex/bob
  (def db-subj-pred-del
    @(fluree/stage db
                   {:delete [:ex/bob :schema/age '?o]
                    :where  [[:ex/bob :schema/age '?o]]}))

  ;; Bob should no longer have an age property.
  @(fluree/query
     db-subj-pred-del
     {:selectOne [:*]
      :from      :ex/bob})

  @(fluree/query db-subj-pred-del
                 {:select {'?s [:*]}
                  :where  [['?s :type :ex/User]]})

  ;;;;;;;;;;;;;;;;
  ;; delete all subjects with a :schema/email predicate
  (def db-all-preds
    @(fluree/stage db
                   {:delete ['?s '?p '?o]
                    :where  [['?s :schema/email '?x]
                             ['?s '?p '?o]]}))

  @(fluree/query db
                 {:select ['?s '?p '?o]
                  :where  [['?s :schema/email '?x]
                           ['?s '?p '?o]]})

  ;; Only Bob should be left, as he is the only one without an email
  @(fluree/query
     db-all-preds
     {:select '?name
      :where  [['?s :schema/name '?name]]})

  ;;;;;;;;;;;;;;;;
  ;; delete all subjects where :schema/age = 30
  (def db-age-delete
    @(fluree/stage db
                   {:delete ['?s :schema/age 17]
                    :where  [['?s :schema/age 17]]}))

  ;;Only Bob and Alice should be left in the db.
  @(fluree/query
     db-age-delete
     {:select '?name
      :where  [['?s :schema/name '?name]]})

  )
