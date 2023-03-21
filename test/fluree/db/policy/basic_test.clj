(ns fluree.db.policy.basic-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.did :as did]))


(deftest ^:integration query-policy-enforcement
  (testing "Testing basic policy enforcement on queries."
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
                         :type         [:f/Policy],         ;; must be of type :f/Policy, else it won't be treated as a policy
                         :f/targetNode :f/allNodes          ;; :f/allNodes special keyword meaning every node (everything)
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
                                                     :f/equals     {:list [:f/$identity :ex/user]}}]}]}])
          root-identity {:f/$identity root-did
                         :f/role      :ex/rootRole}
          alice-identity {:f/$identity alice-did
                          :f/role      :ex/userRole}]

      ;; root can see all user data
      (is (= [{:id               :ex/john,
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
                                  :ex/country "USA"}}]
             @(fluree/query db+policy {:select {'?s [:* {:ex/location [:*]}]}
                                       :where  [['?s :rdf/type :ex/User]]
                                       :opts root-identity}))
          "Both user records + all attributes should show")

      ;; root can see all product data
      (is (= [{:id                   :ex/widget,
               :rdf/type             [:ex/Product],
               :schema/name          "Widget",
               :schema/price         99.99,
               :schema/priceCurrency "USD"}]
             @(fluree/query db+policy {:select {'?s [:* {:ex/location [:*]}]}
                                       :where  [['?s :rdf/type :ex/Product]]
                                       :opts root-identity}))
          "The product record should show with all attributes")

      ;; Alice cannot see product data as it was not explicitly allowed
      (is (= []
             @(fluree/query db+policy {:select {'?s [:*]}
                                       :where  [['?s :rdf/type :ex/Product]]
                                       :opts alice-identity})))

      ;; Alice can see all users, but can only see SSN for herself, and can't see the nested location
      (is (= [{:id               :ex/john,
               :rdf/type         [:ex/User],
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}
              {:id               :ex/alice,
               :rdf/type         [:ex/User],
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111"}]
             @(fluree/query db+policy {:select {'?s [:* {:ex/location [:*]}]}
                                       :where  [['?s :rdf/type :ex/User]]
                                       :opts alice-identity}))
          "Both users should show, but only SSN for Alice")

      ;; Alice can only see her allowed data in a non-graph-crawl query too
      (is (= [["John" nil] ["Alice" "111-11-1111"]]
             @(fluree/query db+policy {:select '[?name ?ssn]
                                       :where  '[[?p :schema/name ?name]
                                                 {:optional [?p :schema/ssn ?ssn]}]
                                       :opts alice-identity}))
          "Both user names should show, but only SSN for Alice")

      (testing "multi-query"
        (is (= {"john" [["john@flur.ee" nil]]
                "alice" [["alice@flur.ee" "111-11-1111"]]}
               @(fluree/multi-query db+policy {"john" '{:select [?email ?ssn]
                                                        :where  [[?p :schema/name "John"]
                                                                 [?p :schema/email ?email]
                                                                 {:optional [?p :schema/ssn ?ssn]}]}
                                               "alice" '{:select [?email ?ssn]
                                                         :where  [[?p :schema/name "Alice"]
                                                                  [?p :schema/email ?email]
                                                                  {:optional [?p :schema/ssn ?ssn]}]}
                                               :opts alice-identity}))
            "Both emails should show, but only SSN for Alice"))

      (testing "history query"
        (let [_ @(fluree/commit! ledger db+policy)]
          (is (= []
                 @(fluree/history ledger {:history [:ex/john :schema/ssn] :t {:from 1}
                                          :commit-details true
                                          :opts alice-identity}))
              "Alice should not be able to see any history for John's ssn")
          (is (= [{:f/t 1,
                   :f/assert [{:schema/ssn "111-11-1111", :id :ex/alice}],
                   :f/retract []}]
                 @(fluree/history ledger {:history [:ex/alice :schema/ssn] :t {:from 1}
                                          :opts    alice-identity}))
              "Alice should be able to see history for her own ssn.")
          (let [[history-result]  @(fluree/history ledger {:history [:ex/alice :schema/ssn] :t {:from 1}
                                                           :commit-details true
                                                           :opts    alice-identity})
                commit-details-asserts (get-in history-result [:f/commit :f/data :f/assert])]
            (is (= [{:rdf/type [:ex/User],
                     :schema/name "John",
                     :schema/email "john@flur.ee",
                     :schema/birthDate "2021-08-17",
                     :id :ex/john}
                    {:rdf/type [:ex/User],
                     :schema/name "Alice",
                     :schema/email "alice@flur.ee",
                     :schema/birthDate "2022-08-17",
                     :schema/ssn "111-11-1111",
                     :ex/location {:id nil},
                     :id :ex/alice}]
                   commit-details-asserts)
                "Alice should be able to see her own ssn in commit asserts, but not John's."))
          (let [[history-result] @(fluree/history ledger {:history [:ex/alice :schema/ssn] :t {:from 1}
                                                          :commit-details true
                                                          :opts    root-identity})
                commit-details-asserts (get-in history-result [:f/commit :f/data :f/assert])]
            (is (contains? (into #{} commit-details-asserts)
                           {:rdf/type [:ex/User],
                            :schema/name "John",
                            :schema/email "john@flur.ee",
                            :schema/birthDate "2021-08-17",
                            :schema/ssn "888-88-8888",
                            :id :ex/john})
                "Root can see John's ssn in commit details."))
          (let [_ @(test-utils/transact ledger {:id :ex/john
                                                :schema/name "Jack"})]
            (is (= [{:f/t 1,
                     :f/assert [{:schema/name "John", :id :ex/john}],
                     :f/retract []}
                    {:f/t 2,
                     :f/assert [{:schema/name "Jack", :id :ex/john}],
                     :f/retract [{:schema/name "John", :id :ex/john}]}]
                   @(fluree/history ledger {:history [:ex/john :schema/name] :t {:from 1}
                                            :opts alice-identity}))
                "Alice should be able to see all history for John's name")))))))
