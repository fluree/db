(ns ipfs-demo
  (:require [fluree.db :as fluree]
            [fluree.db.did :as did]))

;; dev namespace for combining ledgers/dbs using :include option

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server  nil ;; use default
                     :ipns    "Fluree1"
                     :context {:id     "@id"
                               :type   "@type"
                               :schema "http://schema.org/"
                               :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                               :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                               :wiki   "https://www.wikidata.org/wiki/"
                               :skos   "http://www.w3.org/2008/05/skos#"
                               :f      "https://ns.flur.ee/ledger#"}
                     :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}))

  (def ledger @(fluree/create ipfs-conn "my/test-ledger"))
  (def db (fluree/db ledger))

  (def default-context
    {:id     "@id"
     :type   "@type"
     :graph  "@graph"
     :xsd    "http://www.w3.org/2001/XMLSchema#"
     :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
     :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
     :sh     "http://www.w3.org/ns/shacl#"
     :schema "http://schema.org/"
     :skos   "http://www.w3.org/2008/05/skos#"
     :wiki   "https://www.wikidata.org/wiki/"
     :f      "https://ns.flur.ee/ledger#"
     :ex     "http://example.org/"})


  (def tx-1 [{:id           :ex/brian,
              :type         :ex/User,
              :schema/name  "Brian"
              :ex/last      "Smith"
              :schema/email "brian@example.org"
              :schema/age   50
              :ex/favNums   7
              :ex/scores    [76 80 15]}
             {:id           :ex/alice,
              :type         :ex/User,
              :schema/name  "Alice"
              :ex/last      "Smith"
              :schema/email "alice@example.org"
              :ex/favColor  "Green"
              :schema/age   42
              :ex/favNums   [42, 76, 9]
              :ex/scores    [102 92.5 90]}
             {:id          :ex/cam,
              :type        :ex/User,
              :schema/name "Cam"
              :ex/last     "Jones"
              :ex/email    "cam@example.org"
              :schema/age  34
              :ex/favNums  [5, 10]
              :ex/scores   [97.2 100 80]
              :ex/friend   [:ex/brian :ex/alice]}])

  (def db1 @(fluree/stage db {"@context" default-context
                              "insert"   tx-1}))

  @(fluree/query db1 {:context default-context
                      :select  {:ex/cam [:*]}})

  (def c1 @(fluree/commit! ledger db1))

  (fluree/status ledger)

  )
