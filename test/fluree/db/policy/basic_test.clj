(ns fluree.db.policy.basic-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.did :as did]))


(deftest ^:integration policy-enforcement
  (testing "Testing basic policy enforcement."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "policy/a" {:context {:ex "http://example.org/ns/"}})
          root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db        @(fluree/stage
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
                        {:id     root-did
                         :f/role :ex/rootRole}
                        ;; assign alice-did to :ex/userRole and also link the did to :ex/alice via :ex/user
                        {:id      alice-did
                         :ex/user :ex/alice
                         :f/role  :ex/userRole}])

          db+policy @(fluree/stage
                       db
                       ;; add policy targeting :ex/rootRole that can view and modify everything
                       [{:id           :ex/rootPolicy,
                         :type         [:f/Policy],
                         :f/targetNode :f/allNodes
                         :f/allow      [{:id           :ex/rootAccessAllow
                                         :f/targetRole :ex/rootRole ;; keyword for a global role
                                         :f/action     [:f/view :f/modify]}]}
                        ;; add a policy targeting :ex/userRole that can see all users, but only SSN if belonging to themselves
                        {:id            :ex/UserPolicy,
                         :type          [:f/Policy],
                         :f/targetClass :ex/User
                         :f/allow       [{:id           :ex/globalViewAllow
                                          :f/targetRole :ex/userRole ;; keyword for a global role
                                          :f/action     [:f/view]}]
                         :f/property    [{:f/path  :schema/ssn
                                          :f/allow [{:id           :ex/ssnViewRule
                                                     :f/targetRole :ex/userRole
                                                     :f/action     [:f/view]
                                                     :f/equals     {:list [:f/$identity :ex/user]}}]}]}])
          root-db   @(fluree/wrap-policy db+policy {:f/$identity root-did
                                                    :f/role      :ex/rootRole})
          alice-db  @(fluree/wrap-policy db+policy {:f/$identity alice-did
                                                    :f/role      :ex/userRole})]

      ;; root can see all user data
      (is (= @(fluree/query root-db {:select {'?s [:* {:ex/location [:*]}]}
                                     :where  [['?s :rdf/type :ex/User]]})
             [{:id               :ex/john,
               :rdf/type         [:ex/User],
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17",
               :schema/ssn       "888-88-8888"}
              {:id               :ex/alice,
               :rdf/type         [:ex/User],
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111",
               :ex/location      {:id         "_:f211106232532993",
                                  :ex/state   "NC",
                                  :ex/country "USA"}}])
          "Both user records + all attributes should show")

      ;; root can see all product data
      (is (= @(fluree/query root-db {:select {'?s [:* {:ex/location [:*]}]}
                                     :where  [['?s :rdf/type :ex/Product]]})
             [{:id                   :ex/widget,
               :rdf/type             [:ex/Product],
               :schema/name          "Widget",
               :schema/price         99.99,
               :schema/priceCurrency "USD"}])
          "The product record should show with all attributes")

      ;; Alice cannot see product data as it was not explicitly allowed
      (is (= @(fluree/query alice-db {:select {'?s [:*]}
                                      :where  [['?s :rdf/type :ex/Product]]})
             []))

      ;; Alice can see all users, but can only see SSN for herself, and can't see the nested location
      (is (= @(fluree/query alice-db {:select {'?s [:* {:ex/location [:*]}]}
                                      :where  [['?s :rdf/type :ex/User]]})
             [{:id               :ex/john,
               :rdf/type         [:ex/User],
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}
              {:id               :ex/alice,
               :rdf/type         [:ex/User],
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111"}])
          "Both users should show, but only SSN for Alice"))))