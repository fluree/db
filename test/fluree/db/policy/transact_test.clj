(ns fluree.db.policy.transact-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.did :as did]
    [fluree.db.util.core :as util]))

(deftest ^:integration policy-enforcement
  (testing "Testing basic policy enforcement."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "policy/tx-a" {:context {:ex "http://example.org/ns/"}})
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
                         :type         [:f/Policy], ;; must be of type :f/Policy, else it won't be treated as a policy
                         :f/targetNode :f/allNodes ;; :f/allNodes special keyword meaning every node (everything)
                         :f/allow      [{:id           :ex/rootAccessAllow
                                         :f/targetRole :ex/rootRole ;; our name for global / root role
                                         :f/action     [:f/view :f/modify]}]}
                        ;; add a policy targeting :ex/userRole that can see all users, but only SSN if belonging to themselves
                        {:id            :ex/UserPolicy,
                         :type          [:f/Policy],
                         :f/targetClass :ex/User
                         :f/allow       [{:id           :ex/globalViewAllow
                                          :f/targetRole :ex/userRole ;; our assigned name for standard user's role (given to Alice above)
                                          :f/action     [:f/view]}]
                         :f/property    [{:f/path  :schema/ssn
                                          :f/allow [{:id           :ex/ssnViewRule
                                                     :f/targetRole :ex/userRole
                                                     :f/action     [:f/view]
                                                     :f/equals     {:list [:f/$identity :ex/user]}}]}]}
                        ;; add a :ex/Product policy allows view & modify for only :schema/name
                        {:id            :ex/ProductPolicy,
                         :type          [:f/Policy],
                         :f/targetClass :ex/Product
                         :f/property    [{:f/path  :rdf/type
                                          :f/allow [{:f/targetRole :ex/userRole
                                                     :f/action     [:f/view]}]}
                                         {:f/path  :schema/name
                                          :f/allow [{:f/targetRole :ex/userRole
                                                     :f/action     [:f/view :f/modify]}]}]}])]

      (testing "Policy allowed modification"
        (let [alice-db    @(fluree/wrap-policy db+policy {:f/$identity alice-did
                                                          :f/role      :ex/userRole})
              update-name @(fluree/stage alice-db {:id          :ex/widget
                                                   :schema/name "Widget2"})]

          (is (= [{:rdf/type    [:ex/Product]
                   :schema/name "Widget2"}]
                 @(fluree/query update-name
                                {:select {'?s [:*]}
                                 :where  [['?s :rdf/type :ex/Product]]}))
              "Updated :schema/name should have been allowed and have updated value.")))

      (testing "Policy doesn't allow a modification"
        (let [alice-db     @(fluree/wrap-policy db+policy {:f/$identity alice-did
                                                           :f/role      :ex/userRole})
              update-price @(fluree/stage alice-db {:id           :ex/widget
                                                    :schema/price 42.99})]
          (is (util/exception? update-price)
              "Attempted update should have thrown an exception")

          (is (= :db/policy-exception
                 (:error (ex-data update-price)))
              "Exception should be of type :db/policy-exception"))))))