(ns fluree.db.policy.basic-test
  (:require
   [clojure.test :refer :all]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.did :as did]
   [fluree.db.util.core :as util]
   [clojure.string :as str]))


(deftest ^:integration query-policy-enforcement
  (testing "Testing basic policy enforcement on queries."
    (let [conn      @(fluree/connect {:method :memory
                                      :defaults {:context-type :keyword}})
          context   [test-utils/default-context {:ex "http://example.org/ns/"}]
          ledger    @(fluree/create conn "policy/a")
          root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db        @(fluree/stage
                       (fluree/db ledger)
                       {"@context"          context
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
                          :f/role  :ex/userRole}]})

          db+policy @(fluree/stage
                       db
                       ;; add policy targeting :ex/rootRole that can view and modify everything
                       {"@context" context
                        "insert"
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
                                                      :f/equals     {:list [:f/$identity :ex/user]}}]}]}]})]
      (let [root-wrapped-db            @(fluree/wrap-policy
                                         db+policy {:did  root-did
                                                    :role :ex/rootRole
                                                    :context context})
            double-policy-query-result @(fluree/query
                                         root-wrapped-db
                                         {:context context
                                          :select {'?s [:* {:ex/location [:*]}]}
                                          :where  {:id   '?s
                                                   :type :ex/User}
                                          :opts   {:did  root-did
                                                   :role :ex/rootRole}})]
        (is (util/exception? double-policy-query-result)
            "Should be an error to try to apply policy twice on one db.")
        (is (str/includes? (ex-message double-policy-query-result)
                           "Policy already in place")))

      ;; root can see all user data
      (is (= [{:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17",
               :schema/ssn       "888-88-8888"}
              {:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111",
               :ex/location      {:id         "_:f211106232532993",
                                  :ex/state   "NC",
                                  :ex/country "USA"}}]
             @(fluree/query db+policy {:context context
                                       :select {'?s [:* {:ex/location [:*]}]}
                                       :where  {:id   '?s
                                                :type :ex/User}
                                       :opts   {:did  root-did
                                                :role :ex/rootRole}}))
          "Both user records + all attributes should show")

      (is (= [{:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17",
               :schema/ssn       "888-88-8888"}
              {:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111",
               :ex/location      {:id         "_:f211106232532993",
                                  :ex/state   "NC",
                                  :ex/country "USA"}}]
             @(fluree/query db+policy {:context context
                                       :select {'?s [:* {:ex/location [:*]}]}
                                       :where  {:id   '?s
                                                :type :ex/User}
                                       :opts   {:did  root-did
                                                :role :ex/rootRole}}))
          "Both user records + all attributes should show")

      ;; root role can see all product data, without identity
      (is (= [{:id                   :ex/widget,
               :type                 :ex/Product,
               :schema/name          "Widget",
               :schema/price         99.99M,
               :schema/priceCurrency "USD"}]
             @(fluree/query db+policy {:context context
                                       :select {'?s [:* {:ex/location [:*]}]}
                                       :where  {:id   '?s
                                                :type :ex/Product}
                                       :opts   {:role :ex/rootRole}}))
          "The product record should show with all attributes")
      (is (= [{:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}
              {:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17"}]
             @(fluree/query db+policy {:context context
                                       :select {'?s [:* {:ex/location [:*]}]}
                                       :where  {:id   '?s
                                                :type :ex/User}
                                       :opts   {:role :ex/userRole}}))
          "Both users should show, but no SSNs because no identity was provided")

      ;; Alice cannot see product data as it was not explicitly allowed
      (is (= []
             @(fluree/query db+policy {:context context
                                       :select {'?s [:*]}
                                       :where  {:id   '?s
                                                :type :ex/Product}
                                       :opts   {:did  alice-did
                                                :role :ex/userRole}})))

      ;; Alice can see all users, but can only see SSN for herself, and can't see the nested location
      (is (= [{:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}
              {:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111"}]
             @(fluree/query db+policy {:context context
                                       :select {'?s [:* {:ex/location [:*]}]}
                                       :where  {:id   '?s
                                                :type :ex/User}
                                       :opts   {:did  alice-did
                                                :role :ex/userRole}}))
          "Both users should show, but only SSN for Alice")

      ;; Alice can only see her allowed data in a non-graph-crawl query too
      (is (= [["Alice" "111-11-1111"] ["John" nil]]
             @(fluree/query db+policy {:context context
                                       :select '[?name ?ssn]
                                       :where  '[{:id          ?p
                                                  :schema/name ?name}
                                                 [:optional {:id         ?p
                                                             :schema/ssn ?ssn}]]
                                       :opts   {:did  alice-did
                                                :role :ex/userRole}}))
          "Both user names should show, but only SSN for Alice")
        (let [_ @(fluree/commit! ledger db+policy)]
          (testing "query-connection"
            (is (= [["Alice" "111-11-1111"] ["John" nil]]
                   @(fluree/query-connection conn
                                             {:context context
                                              :from    "policy/a"
                                              :select  '[?name ?ssn]
                                              :where   '[{:id          ?p
                                                          :schema/name ?name}
                                                         [:optional {:id         ?p
                                                                     :schema/ssn ?ssn}]]
                                              :opts    {:did  alice-did
                                                        :role :ex/userRole}}))
                "Both user names should show, but only SSN for Alice"))
          (testing "history query"
            (is (= []
                   @(fluree/history ledger {:context        context
                                            :history        [:ex/john :schema/ssn] :t {:from 1}
                                            :commit-details true
                                            :opts           {:did  alice-did
                                                             :role :ex/userRole}}))
                "Alice should not be able to see any history for John's ssn"))
          (is (= [{:f/t       1,
                   :f/assert  [{:schema/ssn "111-11-1111", :id :ex/alice}],
                   :f/retract []}]
                 @(fluree/history ledger {:context context
                                          :history [:ex/alice :schema/ssn] :t {:from 1}
                                          :opts    {:did  alice-did
                                                    :role :ex/userRole}}))
              "Alice should be able to see history for her own ssn.")
          (let [[history-result]       @(fluree/history ledger {:context context
                                                                :history        [:ex/alice :schema/ssn] :t {:from 1}
                                                                :commit-details true
                                                                :opts           {:did  alice-did
                                                                                 :role :ex/userRole}})
                commit-details-asserts (get-in history-result [:f/commit :f/data :f/assert])]
            (is (= [{:type             :ex/User,
                     :schema/name      "John",
                     :schema/email     "john@flur.ee",
                     :schema/birthDate "2021-08-17",
                     :id               :ex/john}
                    {:type             :ex/User,
                     :schema/name      "Alice",
                     :schema/email     "alice@flur.ee",
                     :schema/birthDate "2022-08-17",
                     :schema/ssn       "111-11-1111",
                     :ex/location      {:id nil},
                     :id               :ex/alice}]
                   commit-details-asserts)
                "Alice should be able to see her own ssn in commit details, but not John's."))
          (let [[history-result]       @(fluree/history ledger {:context context
                                                                :history        [:ex/alice :schema/ssn] :t {:from 1}
                                                                :commit-details true
                                                                :opts           {:did  root-did
                                                                                 :role :ex/rootRole}})
                commit-details-asserts (get-in history-result [:f/commit :f/data :f/assert])]
            (is (contains? (into #{} commit-details-asserts)
                           {:type             :ex/User,
                            :schema/name      "John",
                            :schema/email     "john@flur.ee",
                            :schema/birthDate "2021-08-17",
                            :schema/ssn       "888-88-8888",
                            :id               :ex/john})
                "Root can see John's ssn in commit details."))
          (let [_ @(test-utils/transact ledger {"@context" context
                                                "delete"   {:id          :ex/john
                                                            :schema/name "John"}
                                                "insert"   {:id          :ex/john
                                                            :schema/name "Jack"}})]
            (is (= [{:f/t       1,
                     :f/assert  [{:schema/name "John", :id :ex/john}],
                     :f/retract []}
                    {:f/t       2,
                     :f/assert  [{:schema/name "Jack", :id :ex/john}],
                     :f/retract [{:schema/name "John", :id :ex/john}]}]
                   @(fluree/history ledger {:context context
                                            :history [:ex/john :schema/name] :t {:from 1}
                                            :opts    {:did  alice-did
                                                      :role :ex/userRole}}))
                "Alice should be able to see all history for John's name"))))))

(deftest policy-without-f-context-term
  (testing "policies should work w/o an explicit f -> https://ns.flur.ee/ledger# context term"
    (let [conn        (test-utils/create-conn)
          ledger-name (str "policy-without-f-context-term-" (random-uuid))
          ledger      @(fluree/create conn ledger-name
                                      {:defaultContext
                                       {:id     "@id"
                                        :type   "@type"
                                        :list   "@list"
                                        :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                        :schema "http://schema.org/"
                                        :ex     "http://example.org/ns/"}})
          alice-did   "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"
          db          @(fluree/stage
                         (fluree/db ledger)
                         {"@context" "https://ns.flur.ee"
                          "insert"
                          [{:id          :ex/alice,
                            :type        :ex/User,
                            :schema/name "Alice"
                            :ex/secret   "alice's secret"}
                           {:id          :ex/bob,
                            :type        :ex/User,
                            :schema/name "Bob"
                            :ex/secret   "bob's secret"}
                           {:id                   :ex/widget,
                            :type                 :ex/Product,
                            :schema/name          "Widget"
                            :schema/price         99.99
                            :schema/priceCurrency "USD"
                            :ex/secret            "this is overpriced"}]})
          db          @(fluree/stage
                         db
                         {"@context" "https://ns.flur.ee"
                          "insert"
                          [{:id                              alice-did
                            :ex/user                         :ex/alice
                            "https://ns.flur.ee/ledger#role" :ex/userRole}
                           {:id                                     :ex/UserPolicy
                            :type
                            ["https://ns.flur.ee/ledger#Policy"]
                            "https://ns.flur.ee/ledger#targetClass" :ex/User
                            "https://ns.flur.ee/ledger#allow"
                            [{:id                                    :ex/globalViewAllow
                              "https://ns.flur.ee/ledger#targetRole" :ex/userRole
                              "https://ns.flur.ee/ledger#action"
                              [{:id "https://ns.flur.ee/ledger#view"}]}]
                            "https://ns.flur.ee/ledger#property"
                            [{"https://ns.flur.ee/ledger#path" :ex/secret
                              "https://ns.flur.ee/ledger#allow"
                              [{:id                                    :ex/ssnViewRule
                                "https://ns.flur.ee/ledger#targetRole" :ex/userRole
                                "https://ns.flur.ee/ledger#action"
                                [{:id "https://ns.flur.ee/ledger#view"}]
                                "https://ns.flur.ee/ledger#equals"
                                {:list [{:id "https://ns.flur.ee/ledger#$identity"} :ex/user]}}]}]}]})]
      (is (= #{{:id :ex/bob, :type :ex/User, :schema/name "Bob"}
               {:id          :ex/alice, :type :ex/User, :ex/secret "alice's secret"
                :schema/name "Alice"}}
             (set @(fluree/query db {:where  '{:id   ?s
                                               :type :ex/User}
                                     :select '{?s [:*]}
                                     :opts   {:role :ex/userRole
                                              :did  alice-did}})))))))

(deftest ^:integration missing-type
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "policy" {:defaultContext [test-utils/default-str-context
                                                               {"ex" "http://example.com/"}]})
        db0 (fluree/db ledger)

        alice-did    "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"

        db1 @(fluree/stage db0 {"@context" "https://ns.flur.ee"
                                 "insert" [{"id" "ex:alice"
                                            "type" "ex:User"
                                            "ex:secret" "alice's secret"}
                                           {"id" "ex:bob"
                                            "type" "ex:User"
                                            "ex:secret" "bob's secret"}
                                           {"id" "ex:UserPolicy"
                                            "type" ["f:Policy"]
                                            "f:targetClass" {"id" "ex:User"}
                                            "f:allow"
                                            [{"id" "ex:globalViewAllow"
                                              "f:targetRole" {"id" "ex:userRole"}
                                              "f:action" [{"id" "f:view"}]}]
                                            "f:property"
                                            [{"f:path" {"id" "ex:secret"}
                                              "f:allow"
                                              [{"id" "ex:secretsRule"
                                                "f:targetRole" {"id" "ex:userRole"}
                                                "f:action" [{"id" "f:view"}
                                                            {"id" "f:modify"}]
                                                "f:equals" {"@list"
                                                            [{"id" "f:$identity"}
                                                             {"id" "ex:User"}]}}]}]}
                                           {"id" alice-did
                                            "ex:User" {"id" "ex:alice"}
                                            "f:role" {"id" "ex:userRole"}}]})]
    (is (= #{{"id" "ex:alice", "type" "ex:User", "ex:secret" "alice's secret"}
             {"id" "ex:bob", "type" "ex:User"}}
           (set @(fluree/query db1
                               {"select" {"?s" ["*"]}
                                "where" {"@id" "?s" "type" "ex:User"}
                                :opts {:role "ex:userRole"
                                       :did alice-did}}))))))

(deftest ^:pending ^:integration identity-equals-test
  (let [conn         @(fluree/connect {:method :memory})
        context      {"ex"     "http://example.org/"
                      "schema" "http://schema.org/"
                      "f"      "https://ns.flur.ee/ledger#"}
        ledger-alias "test/identity"
        ledger       @(fluree/create conn ledger-alias)
        did          "did:fluree:Tf5M4L7SNkziB4Q5gC8Hjuqu9WQKCwKpU1Y"
        user-txn     {"@context" context
                      "insert"   [{"@id"         "http://example.org/betty",
                                   "@type"       "http://example.org/Yeti",
                                   "schema:name" "Betty"
                                   "schema:age"  55},
                                  {"@id"         "ex:freddy",
                                   "@type"       "ex:Yeti",
                                   "schema:name" "Freddy",
                                   "schema:age"  1002},
                                  {"@id"         "ex:letty",
                                   "@type"       "ex:Yeti",
                                   "schema:name" "Leticia",
                                   "schema:age"  38}
                                  {"@id"     did
                                   "ex:user" {"@id" "ex:freddy"}
                                   "f:role"  {"@id" "ex:yetiRole"}}]}
        policy-txn   {"@context" context
                      "insert"
                      {"@id"           "ex:yetiPolicy",
                       "@type"         ["f:Policy"],
                       "f:targetClass" {"@id" "ex:Yeti"},
                       "f:allow"       [{"@id"          "ex:yetiViewAllow",
                                         "f:targetRole" {"@id" "ex:yetiRole"},
                                         "f:action"     [{"@id" "f:view"}]}],
                       "f:property"    [{"@id"     "ex:yetisViewOnlyOwnAge",
                                         "f:path"  {"@id" "schema:age"},
                                         "f:allow" [{"@id"          "ex:ageViewRule",
                                                     "f:targetRole" {"@id" "ex:yetiRole"},
                                                     "f:action"     [{"@id" "f:view"}],
                                                     "f:equals"     { "@list" [{"@id" "f:$identity"}, {"@id" "ex:user"}] }}]}]}}
        db           @(fluree/stage (fluree/db ledger)
                                    policy-txn)
        db2          @(fluree/stage db
                                    user-txn)
        _            @(fluree/commit! ledger db2)]
    (is (= [{"@id"         "http://example.org/betty",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Betty",}
            {"@id"         "http://example.org/freddy",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Freddy",
             "schema:age"  1002}
            {"@id"         "http://example.org/letty",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Leticia",}]
           @(fluree/query db2 {"@context" {"schema" "http://schema.org/"}
                               :where     '{"@id"         ?s
                                            "schema:name" "?name"}
                               :select    '{?s ["*"]}
                               :opts      {:did did}}) )
        "Should return Freddy's age, but no one else's")
    (is (= [{"@id"         "http://example.org/betty",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Betty",}
            {"@id"         "http://example.org/freddy",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Freddy",
             "schema:age"  1002}
            {"@id"         "http://example.org/letty",
             "@type"       "http://example.org/Yeti",
             "schema:name" "Leticia",}]
           @(fluree/query-connection conn {"@context" {"schema" "http://schema.org/"}
                                           :from      ledger-alias
                                           :where     '{"@id"         ?s
                                                        "schema:name" "?name"}
                                           :select    '{?s ["*"]}
                                           :opts      {:did did}}) )
        "Should return Freddy's age, but no one else's")))
