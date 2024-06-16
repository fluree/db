(ns fluree.db.policy.basic-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.crypto :as crypto]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))


(deftest ^:integration query-policy-enforcement
  (testing "Testing basic policy enforcement on queries."
    (let [conn      @(fluree/connect {:method :memory})
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
      (let [root-wrapped-db            @(fluree/wrap-policy db+policy
                                                            {:did  root-did
                                                             :role :ex/rootRole}
                                                            context)
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
      (is (pred-match? [{:id               :ex/alice,
                         :type             :ex/User,
                         :schema/name      "Alice",
                         :schema/email     "alice@flur.ee",
                         :schema/birthDate "2022-08-17",
                         :schema/ssn       "111-11-1111",
                         :ex/location      {:id         iri/blank-node-id?,
                                            :ex/state   "NC",
                                            :ex/country "USA"}}
                        {:id               :ex/john,
                         :type             :ex/User,
                         :schema/name      "John",
                         :schema/email     "john@flur.ee",
                         :schema/birthDate "2021-08-17",
                         :schema/ssn       "888-88-8888"}]
                       @(fluree/query db+policy {:context context
                                                 :select {'?s [:* {:ex/location [:*]}]}
                                                 :where  {:id   '?s
                                                          :type :ex/User}
                                                 :opts   {:did  root-did
                                                          :role :ex/rootRole}}))
          "Both user records + all attributes should show")

      (is (pred-match? [{:id               :ex/alice,
                         :type             :ex/User,
                         :schema/name      "Alice",
                         :schema/email     "alice@flur.ee",
                         :schema/birthDate "2022-08-17",
                         :schema/ssn       "111-11-1111",
                         :ex/location      {:id         iri/blank-node-id?,
                                            :ex/state   "NC",
                                            :ex/country "USA"}}
                        {:id               :ex/john,
                         :type             :ex/User,
                         :schema/name      "John",
                         :schema/email     "john@flur.ee",
                         :schema/birthDate "2021-08-17",
                         :schema/ssn       "888-88-8888"}]
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
      (is (= [{:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17"}
              {:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}]
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
      (is (= [{:id               :ex/alice,
               :type             :ex/User,
               :schema/name      "Alice",
               :schema/email     "alice@flur.ee",
               :schema/birthDate "2022-08-17",
               :schema/ssn       "111-11-1111"}
              {:id               :ex/john,
               :type             :ex/User,
               :schema/name      "John",
               :schema/email     "john@flur.ee",
               :schema/birthDate "2021-08-17"}]
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
                     :schema/name      "Alice",
                     :schema/email     "alice@flur.ee",
                     :schema/birthDate "2022-08-17",
                     :schema/ssn       "111-11-1111",
                     :id               :ex/alice}
                    {:type             :ex/User,
                     :schema/name      "John",
                     :schema/email     "john@flur.ee",
                     :schema/birthDate "2021-08-17",
                     :id               :ex/john}]
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
          ledger      @(fluree/create conn ledger-name)
          alice-did   "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"
          db          @(fluree/stage
                         (fluree/db ledger)
                         {"@context" ["https://ns.flur.ee"
                                      {:id     "@id"
                                       :type   "@type"
                                       :list   "@list"
                                       :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                       :schema "http://schema.org/"
                                       :ex     "http://example.org/ns/"}]
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
                         {"@context" ["https://ns.flur.ee"
                                      {:id     "@id"
                                       :type   "@type"
                                       :list   "@list"
                                       :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                       :schema "http://schema.org/"
                                       :ex     "http://example.org/ns/"}]
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
      (is (= [{:id          :ex/alice, :type :ex/User, :ex/secret "alice's secret"
               :schema/name "Alice"}
              {:id :ex/bob, :type :ex/User, :schema/name "Bob"}]
             @(fluree/query db {:context {:id     "@id"
                                          :type   "@type"
                                          :list   "@list"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :schema "http://schema.org/"
                                          :ex     "http://example.org/ns/"}
                                :where  '{:id   ?s
                                          :type :ex/User}
                                :select '{?s [:*]}
                                :opts   {:role :ex/userRole
                                         :did  alice-did}}))))))

(deftest ^:integration missing-type
  (let [conn      @(fluree/connect {:method :memory})
        ledger    @(fluree/create conn "policy")
        db0       (fluree/db ledger)
        context   [test-utils/default-str-context {"ex" "http://example.com/"}]
        alice-did "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"

        db1 @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                "insert"   [{"id"        "ex:alice"
                                             "type"      "ex:User"
                                             "ex:secret" "alice's secret"}
                                            {"id"        "ex:bob"
                                             "type"      "ex:User"
                                             "ex:secret" "bob's secret"}
                                            {"id"            "ex:UserPolicy"
                                             "type"          ["f:Policy"]
                                             "f:targetClass" {"id" "ex:User"}
                                             "f:allow"
                                             [{"id"           "ex:globalViewAllow"
                                               "f:targetRole" {"id" "ex:userRole"}
                                               "f:action"     [{"id" "f:view"}]}]
                                             "f:property"
                                             [{"f:path" {"id" "ex:secret"}
                                               "f:allow"
                                               [{"id"           "ex:secretsRule"
                                                 "f:targetRole" {"id" "ex:userRole"}
                                                 "f:action"     [{"id" "f:view"}
                                                                 {"id" "f:modify"}]
                                                 "f:equals"     {"@list"
                                                                 [{"id" "f:$identity"}
                                                                  {"id" "ex:User"}]}}]}]}
                                            {"id"      alice-did
                                             "ex:User" {"id" "ex:alice"}
                                             "f:role"  {"id" "ex:userRole"}}]})]
    (is (= [{"id" "ex:alice", "type" "ex:User", "ex:secret" "alice's secret"}
            {"id" "ex:bob", "type" "ex:User"}]
           @(fluree/query db1
                          {"@context" context
                           "select"   {"?s" ["*"]}
                           "where"    {"@id" "?s" "type" "ex:User"}
                           :opts      {:role "ex:userRole"
                                       :did  alice-did}})))))

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
                                                   "f:role" {"@id" "ex:rootRole"} }]})
        db+policy     @(fluree/stage db {"@context" context
                                         "insert"
                                         {"@id"          "ex:rootPolicy"
                                          "@type"        ["f:Policy"]
                                          "f:targetNode" {"@id" "f:allNodes"}
                                          "f:allow"      [{"@id"          "ex:rootAccessAllow"
                                                           "f:targetRole" {"@id" "ex:rootRole"}
                                                           "f:action"     [{"@id" "f:view"} ]}]}})]
    (is (= [{"@id"         "http://example.org/betty"
             "@type"       "http://example.org/Yeti"
             "schema:age"  55
             "schema:name" "Betty"}
            {"@id"         "http://example.org/freddy"
             "@type"       "http://example.org/Yeti"
             "schema:age"  1002
             "schema:name" "Freddy"}
            {"@id"         "http://example.org/letty"
             "@type"       "http://example.org/Yeti"
             "schema:age"  38
             "schema:name" "Leticia"}]
           @(fluree/query db+policy {"@context" {"schema" "http://schema.org/"}
                                     :where     {"schema:name" '?name
                                                 "@id"         '?s}
                                     :select    '{?s ["*"]}
                                     :opts      {:did root-read-did}})))
    (is (= []
           @(fluree/query db+policy {"@context" {"schema" "http://schema.org/"}
                                     :where     {"schema:name" '?name
                                                 "@id"         '?s}
                                     :select    '{?s ["*"]}
                                     :opts      {:did  "not-a-did"
                                                 :role "not-a-role"}}))
        "Should not be able to see any data")))

(deftest ^:integration jws
  (let [conn    @(fluree/connect {:method :memory})
        context {"ex"     "http://example.org/"
                 "schema" "http://schema.org/"
                 "f"      "https://ns.flur.ee/ledger#"}
        ledger  @(fluree/create conn "test/root-read")

        pleb-privkey "bb854f7ae267234235d57b2dff87359771cf75a76bc17a418102a48147d29ba7",
        pleb-did     (:id (did/private->did-map pleb-privkey))
        root-privkey "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
        root-did     (:id (did/private->did-map root-privkey))
        db           @(fluree/stage (fluree/db ledger )
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
                                                 {"@id"    root-did
                                                  "f:role" {"@id" "ex:rootRole"}}
                                                 {"@id"    pleb-did
                                                  "f:role" {"@id" "ex:plebRole"}}]})
        db+policy    @(fluree/stage db {"@context" context
                                        "insert"
                                        {"@id"          "ex:rootPolicy"
                                         "@type"        ["f:Policy"]
                                         "f:targetNode" {"@id" "f:allNodes"}
                                         "f:allow"      [{"@id"          "ex:rootAccessAllow"
                                                          "f:targetRole" {"@id" "ex:rootRole"}
                                                          "f:action"     [{"@id" "f:view"}
                                                                          {"@id" "f:modify"}]}
                                                         {"@id"          "ex:plebReadAllow"
                                                          "f:targetRole" {"@id" "ex:plebRole"}
                                                          "f:action"     [{"@id" "f:view"}]}]}})]


    (testing "root jws"
      (let [db1 @(fluree/stage db+policy (crypto/create-jws
                                           (json/stringify
                                             {"@context" context
                                              "insert"
                                              {"@id"         "ex:spaghetti"
                                               "@type"       "ex:Yeti"
                                               "schema:name" "Spaghetti"
                                               "schema:age"  150}})
                                           root-privkey))]
        (is (= [{"@id"         "ex:betty"
                 "@type"       "ex:Yeti"
                 "schema:age"  55
                 "schema:name" "Betty"}
                {"@id"         "ex:freddy"
                 "@type"       "ex:Yeti"
                 "schema:age"  1002
                 "schema:name" "Freddy"}
                {"@id"         "ex:letty"
                 "@type"       "ex:Yeti"
                 "schema:age"  38
                 "schema:name" "Leticia"}
                {"@id"         "ex:spaghetti"
                 "@type"       "ex:Yeti"
                 "schema:name" "Spaghetti"
                 "schema:age"  150}]
               @(fluree/query db1 (crypto/create-jws
                                    (json/stringify
                                      {"@context" context
                                       :where     {"schema:name" '?name
                                                   "@id"         '?s}
                                       :select    '{?s ["*"]}})
                                    root-privkey)))
            "transaction and query succeeded")))
    (testing "pleb jws"
      (let [db-err @(fluree/stage db+policy (crypto/create-jws
                                           (json/stringify
                                             {"@context" context
                                              "insert"
                                              {"@id"         "ex:confetti"
                                               "@type"       "ex:Yeti"
                                               "schema:name" "Confetti"
                                               "schema:age"  15}})
                                           pleb-privkey))]
        (is (= "Policy enforcement prevents modification."
               (ex-message db-err))
            "transaction failed")
        (is (= [{"@id"         "ex:betty"
                 "@type"       "ex:Yeti"
                 "schema:age"  55
                 "schema:name" "Betty"}
                {"@id"         "ex:freddy"
                 "@type"       "ex:Yeti"
                 "schema:age"  1002
                 "schema:name" "Freddy"}
                {"@id"         "ex:letty"
                 "@type"       "ex:Yeti"
                 "schema:age"  38
                 "schema:name" "Leticia"}]
               @(fluree/query db+policy (crypto/create-jws
                                          (json/stringify
                                            {"@context" context
                                             :where {"schema:name" '?name
                                                     "@id" '?s}
                                             :select '{?s ["*"]}})
                                          pleb-privkey)))
            "query succeeded")))))
