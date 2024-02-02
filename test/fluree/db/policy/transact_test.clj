(ns fluree.db.policy.transact-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.did :as did]
            [fluree.db.util.core :as util]))

(deftest ^:integration policy-enforcement
  (testing "Testing basic policy enforcement."
    (let [conn      @(fluree/connect {:method :memory})
          context   [test-utils/default-context {:ex "http://example.org/ns/"}]
          ledger    @(fluree/create conn "policy/tx-a")
          root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db        @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
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
                          :f/role  [:ex/userRole :ex/otherRole]}]})

          db+policy @(fluree/stage
                       db
                       {"@context" context
                        "insert"
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
                                                      :f/equals     {:list [:f/$identity :ex/user]}}]}
                                          {:f/path  :schema/email
                                           :f/allow [{:id           :ex/emailChangeRule
                                                      :f/targetRole :ex/userRole
                                                      :f/action     [:f/view :f/modify]
                                                      :f/equals     {:list [:f/$identity :ex/user]}}]}]}
                         ;; add a :ex/Product policy allows view & modify for only :schema/name
                         {:id            :ex/ProductPolicy,
                          :type          [:f/Policy],
                          :f/targetClass :ex/Product
                          :f/property    [{:f/path  :type
                                           :f/allow [{:f/targetRole :ex/userRole
                                                      :f/action     [:f/view]}]}
                                          {:f/path  :schema/name
                                           :f/allow [{:f/targetRole :ex/userRole
                                                      :f/action     [:f/view :f/modify]}]}]}]})]
      (testing "Policy allowed modification"
        (testing "using role + id"
          (let [update-name @(fluree/stage db+policy
                                            {"@context" context
                                             "delete"
                                             {:id          :ex/alice
                                              :schema/email "alice@flur.ee"}
                                             "insert"
                                             {:id          :ex/alice
                                              :schema/email "alice@foo.bar"}}
                                            {:did alice-did
                                             :role      :ex/userRole})]
            (is (= [{:id :ex/alice,
                     :type :ex/User,
                     :schema/name "Alice",
                     :schema/email "alice@foo.bar",
                     :schema/birthDate "2022-08-17",
                     :schema/ssn "111-11-1111"}]
                   @(fluree/query update-name
                                  {:context context
                                   :select {'?s [:*]}
                                   :where  {:id '?s, :schema/name "Alice"}
                                   :opts {:did alice-did}}))
                "Alice should be allowed to update her own name.")))
        (testing "using role only"
            (let [update-price @(fluree/stage db+policy
                                               {"@context" context
                                                "delete"
                                                {:id          :ex/widget
                                                 :schema/price 99.99}
                                                "insert"
                                                {:id          :ex/widget
                                                 :schema/price 105.99}}
                                               {:role :ex/rootRole})]

              (is (= [{:id :ex/widget,
                       :type :ex/Product,
                       :schema/name "Widget",
                       :schema/price 105.99M,
                       :schema/priceCurrency "USD"}]
                     @(fluree/query update-price
                                    {:context context
                                     :select {'?s [:*]}
                                     :where  {:id '?s, :type :ex/Product}}))
                  "Updated :schema/price should have been allowed, and entire product is visible in query."))
            (let [update-name @(fluree/stage db+policy
                                              {"@context" context
                                               "delete"
                                               {:id          :ex/widget
                                                :schema/name "Widget"}
                                               "insert"
                                               {:id          :ex/widget
                                                :schema/name "Widget2"}}
                                              {:role :ex/userRole})]

              (is (= [{:type    :ex/Product
                       :schema/name "Widget2"}]
                     @(fluree/query update-name
                                    {:context context
                                     :select {'?s [:*]}
                                     :where  {:id '?s, :type :ex/Product}
                                     :opts {:role :ex/userRole}}))
                  "Updated :schema/name should have been allowed, and only name is visible in query."))))
      (testing "Policy doesn't allow a modification"
        (let [update-price @(fluree/stage db+policy {"@context" context
                                                      "insert" {:id           :ex/widget
                                                                :schema/price 42.99}}
                                          {:did root-did
                                           :role      :ex/userRole})]
          (is (util/exception? update-price)
              "Attempted update should have thrown an exception, `:ex/userRole` cannot modify product prices regardless of identity")

          (is (= :db/policy-exception
                 (:error (ex-data update-price)))
              "Exception should be of type :db/policy-exception"))
        (let [update-email @(fluree/stage db+policy {"@context" context
                                                      "insert"   {:id           :ex/john
                                                                  :schema/email "john@foo.bar"}}
                                          {:role :ex/user})]

          (is (util/exception? update-email)
              "attempted update should have thrown an exception, no identity was provided")

          (is (= :db/policy-exception
                 (:error (ex-data update-email)))
              "exception should be of type :db/policy-exception"))
        (let [update-name-other-role @(fluree/stage db+policy {"@context" context
                                                                "insert" {:id          :ex/widget
                                                                          :schema/name "Widget2"}}
                                                    {:did alice-did
                                                     :role      :ex/otherRole})]
          (is (util/exception? update-name-other-role)
              "Attempted update should have thrown an exception, this role cannot modify product names")

          (is (= :db/policy-exception
                 (:error (ex-data update-name-other-role)))
              "Exception should be of type :db/policy-exception"))))))


(deftest ^:integration root-read-only-policy
  (let [conn          @(fluree/connect {:method :memory})
        context       {"ex"     "http://example.org/"
                       "schema" "http://schema.org/"
                       "f"      "https://ns.flur.ee/ledger#"}
        ledger        @(fluree/create conn "test/root-read")
        root-read-did (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
        db            @(fluree/stage (fluree/db ledger )
                                     {"@context" context
                                      "insert"   [{"@id"         "ex:betty"
                                                   "@type"       "ex:Yeti"
                                                   "schema:name" "Betty"
                                                   "schema:age"  55}
                                                  {"@id"         "ex:freddy"
                                                   "@type"       "ex:Yeti"
                                                   "schema:name" "Freddy"
                                                   "schema:age"  1002}
                                                  {"@id"         "ex:letty"
                                                   "@type"       "ex:Yeti"
                                                   "schema:name" "Leticia"
                                                   "schema:age"  38}
                                                  {"@id"    root-read-did
                                                   "f:role" {"@id" "ex:rootRole"}}]})
        db+policy     @(fluree/stage db {"@context" context
                                         "insert"
                                         {"@id"          "ex:rootPolicy"
                                          "@type"        ["f:Policy"]
                                          "f:targetNode" {"@id" "f:allNodes"}
                                          "f:allow"      [{"@id"          "ex:rootAccessAllow"
                                                           "f:targetRole" {"@id" "ex:rootRole"}
                                                           "f:action"     [{"@id" "f:view"}]}]}})
        update-yeti   @(fluree/stage db+policy {"@context" context
                                                "insert"
                                                {"@id"        "ex:betty",
                                                 "schema:age" 56}}
                                     {:did root-read-did})]
    (is (util/exception? update-yeti)
        "Should throw an exception, role is read-only")
    (is (= :db/policy-exception
           (:error (ex-data update-yeti)))
        "Exception should be of type :db/policy-exception")))
