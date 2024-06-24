(ns json-ld.policy
  (:require [fluree.db :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.did :as did]))

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     ;; ledger defaults used for newly created ledgers
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
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

  (def ledger @(fluree/create ipfs-conn "sf/a" {:context {:ex "http://example.org/ns/"}}))

  (def db
    @(fluree/stage
       (fluree/db ledger)
       [{:id               :ex/alice,
         :type             :ex/User,
         :schema/name      "Alice"
         :schema/email     "alice@flur.ee"
         :schema/birthDate "2022-08-17"
         :schema/ssn       "111-11-1111"
         :ex/location      {:ex/state   "NC"
                            :ex/country "USA"}}
        {:id               :ex/john,
         :type             :ex/User,
         :schema/name      "John"
         :schema/email     "john@flur.ee"
         :schema/birthDate "2021-08-17"
         :schema/ssn       "888-88-8888"}
        {:id                   :ex/widget,
         :type                 :ex/Product,
         :schema/name          "Widget"
         :schema/price         99.99
         :schema/priceCurrency "USD"}
        ;; assign root-did to :ex/rootRole
        {:id      (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
         :f/roles :ex/rootRole}
        ;; assign alice-did to :ex/userRole and also link the did to :ex/alice subject
        {:id      (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
         :ex/user :ex/alice
         :f/roles :ex/userRole}]))


  ;; attach a did record to a 'root' role - you can call the role anything you want
  (def db2
    @(fluree/stage
       db
       {:id     (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
        :f/role :ex/rootRole}))

  ;; attach a different did record to a different role
  (def db3
    @(fluree/stage
       db2
       {:id      (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
        :ex/user :ex/alice
        :f/role  :ex/externalRole}))

  ;; Note that already, one can query for everything unpermissioned - but either role hasn't been given any permissions so nothing will return
  @(fluree/query db3 {:select {'?s [:* {:ex/location [:*]}]}
                      :where  [['?s :type :ex/User]]})

  ;; try to get a permissioned DB - but not rules exist yet!
  @(fluree/wrap-policy db3 {:f/$identity (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
                            :f/role      :ex/rootRole})


  ;; establish a root role that can do anything
  (def db4
    @(fluree/stage
       db3
       {:id           :ex/rootPolicy,
        :type         [:f/Policy],
        :f/targetNode :f/allNodes
        :f/allow      [{:id           :ex/rootAccessAllow
                        :f/targetRole :ex/rootRole          ;; keyword for a global role
                        :f/action     [:f/view :f/modify]}]}))


  ;; try some queries... default uses no permissions
  @(fluree/query db4 {:select {'?s [:* {:ex/location [:*]}]}
                      :where  [['?s :type :ex/User]]})

  ;; try with the non-root role
  (def perm-db @(fluree/wrap-policy db4 {:f/$identity (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
                                         :f/role      :ex/externalRole}))

  ;; no permissions to :ex/User data
  @(fluree/query perm-db {:select {'?s [:* {:ex/location [:*]}]}
                          :where  [['?s :type :ex/User]]})

  ;; try with root role
  (def perm-db @(fluree/wrap-policy db4 {:f/$identity (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
                                         :f/role      :ex/rootRole}))

  ;; root can see all
  @(fluree/query perm-db {:select {'?s [:* {:ex/location [:*]}]}
                          :where  [['?s :type :ex/User]]})

  (-> perm-db :permissions)

  (def db5
    @(fluree/stage
       db4
       {:id            :ex/UserPolicy,
        :type          [:f/Policy],
        :f/targetClass :ex/User
        :f/allow       [{:id           :ex/globalViewAllow
                         :f/targetRole :ex/externalRole     ;; keyword for a global role
                         :f/action     [:f/view]}]
        :f/property    [{:f/path  :schema/ssn
                         :f/allow [{:id           :ex/ssnViewRule
                                    :f/targetRole :ex/externalRole
                                    :f/action     [:f/view]
                                    :f/equals     {:list [:f/$identity :ex/user]}}]}]}))


  ;; try with the non-root role
  (def perm-db @(fluree/wrap-policy db5 {:f/$identity (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
                                         :f/role      :ex/externalRole}))
  (-> perm-db :permissions)

  ;; should see users, but only own SSN - and not location in crawl
  @(fluree/query perm-db {:select {'?s [:* {:ex/location [:*]}]}
                          :where  [['?s :type :ex/User]]})

  ;; no product permissions
  @(fluree/query perm-db {:select {'?s [:* {:ex/location [:*]}]}
                          :where  [['?s :type :ex/Product]]})

  )


