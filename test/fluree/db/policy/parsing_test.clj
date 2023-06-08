(ns fluree.db.policy.parsing-test
  (:require
   [clojure.test :refer :all]
   [fluree.db.constants :as const]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.did :as did]
   [fluree.db.json-ld.policy :as policy]))

;; tests to ensure policy enforcement parsing is accurate

(def non-policy-keys #{:ident :roles :root? :cache})

(defn- stub-policy-functions
  "Removes pre-compiled functions from policy map for testing results comparison"
  [policy-map]
  (reduce-kv (fn [acc k v]
               (cond
                 (map? v)
                 (assoc acc k (stub-policy-functions v))

                 (and (vector? v)
                      (fn? (second v)))
                 (assoc acc k [(first v) ::replaced-policy-function])

                 :else
                 (assoc acc k v)))
             {}
             policy-map))

(defn- replace-policy-fns
  "Policy functions are generated and cached. This replaces them in the return policy maps such that
  map equality will work for tests."
  [policy]
  ;; remove all non-policy keys, as the policy formats will all be the same.
  (let [policies (->> policy
                      (remove #(non-policy-keys (first %)))
                      (into {})
                      (reduce-kv (fn [acc policy-key policy-map]
                                   (assoc acc policy-key (stub-policy-functions policy-map)))
                                 {}))]
    (-> (merge policy policies)
        (dissoc :cache))))


(deftest ^:integration policy-enforcement
  (testing "Testing query policy returns correctly."
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "policy-parse/a" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          root-did     (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did    (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          customer-did (:id (did/private->did-map "854358f6cb3a78ff81febe0786010d6e22839ea6bd52e03365a728d7b693b5a0"))
          db           @(fluree/stage
                          (fluree/db ledger)
                          [;; assign root-did to :ex/rootRole
                           {:id     root-did
                            :f/role :ex/rootRole}
                           ;; assign alice-did to :ex/userRole and also link the did to :ex/alice via :ex/user
                           {:id      alice-did
                            :ex/user :ex/alice
                            :f/role  :ex/userRole}
                           {:id      customer-did
                            :ex/user :ex/bob
                            :f/role  :ex/customerRole}
                           {:id           :ex/rootPolicy,
                            :type         [:f/Policy],
                            :f/targetNode :f/allNodes       ;; :f/allNodes special keyword meaning every node (everything)
                            :f/allow      [{:id           :ex/rootAccessAllow
                                            :f/targetRole :ex/rootRole
                                            :f/action     [:f/view :f/modify]}]}
                           {:id            :ex/UserPolicy,
                            :type          [:f/Policy],
                            :f/targetClass :ex/User
                            :f/allow       [{:id           :ex/globalViewAllow
                                             :f/targetRole :ex/userRole
                                             :f/action     [:f/view]}
                                            {:f/targetRole :ex/userRole
                                             :f/action     [:f/modify]
                                             ;; by default, user can modify their own user profile (following relationship from identity/DID -> :ex/user to User object
                                             :f/equals     {:list [:f/$identity :ex/user]}}]
                            :f/property    [{:f/path  :schema/ssn
                                             :f/allow [{:id           :ex/ssnViewRule
                                                        :f/targetRole :ex/userRole
                                                        :f/action     [:f/view]
                                                        :f/equals     {:list [:f/$identity :ex/user]}}]}]}])]

      (testing "Policy map for classes and props within classes is properly formed"
        (let [sid-User      @(fluree/internal-id db :ex/User)
              sid-ssn       @(fluree/internal-id db :schema/ssn)
              sid-alice-did @(fluree/internal-id db alice-did)
              sid-userRole  @(fluree/internal-id db :ex/userRole)
              policy-alice  (-> @(fluree/promise-wrap (policy/policy-map db sid-alice-did #{sid-userRole} nil))
                                replace-policy-fns)]
          (is (= {const/iri-modify
                  {:class
                   {sid-User {:default {const/iri-equals      [{"@id" const/iri-$identity}
                                                               {"@id" "http://example.org/ns/user"}]
                                        const/iri-target-role {:_id sid-userRole}
                                        :function             [true
                                                               ::replaced-policy-function]
                                        "@id"                 "_:f211106232533007"}}}}
                  const/iri-view
                  {:class
                   {sid-User {sid-ssn  {const/iri-equals      [{"@id" const/iri-$identity}
                                                               {"@id" "http://example.org/ns/user"}]
                                        const/iri-target-role {:_id sid-userRole}
                                        :function             [true
                                                               ::replaced-policy-function]
                                        "@id"                 "http://example.org/ns/ssnViewRule"}
                              :default {const/iri-target-role {:_id sid-userRole}
                                        :function             [false
                                                               ::replaced-policy-function]
                                        "@id"                 "http://example.org/ns/globalViewAllow"}}}}
                  :ident sid-alice-did
                  :roles #{sid-userRole}}
                 policy-alice)
              "Policies for only :ex/userRole should return")))
      (testing "Root policy contains {:root? true} for each applicable :f/action"
        (let [sid-root-did @(fluree/internal-id db root-did)
              sid-rootRole @(fluree/internal-id db :ex/rootRole)
              policy-root  (-> @(fluree/promise-wrap (policy/policy-map db sid-root-did #{sid-rootRole} nil))
                               replace-policy-fns)]
          (is (= {"https://ns.flur.ee/ledger#modify" {:root? true}
                  "https://ns.flur.ee/ledger#view"   {:root? true}
                  :ident                             sid-root-did
                  :roles                             #{sid-rootRole}}
                 policy-root))))))
  (testing "Testing query policy with strings"
    (let [conn     (test-utils/create-conn)
          ledger   @(fluree/create conn "policy-parse/a" {:defaultContext ["" {"ex" "http://example.org/ns/"}]
                                                          :context-type   :string})
          root-did (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          db       @(fluree/stage
                     (fluree/db ledger)
                     [{"id"     root-did
                       "f:role" {"id" "ex:rootRole"}}
                      {"id"           "ex:rootPolicy",
                       "type"         ["f:Policy"],
                       "f:targetNode" {"id" "f:allNodes"}
                       "f:allow"      [{"id"           "ex:rootAccessAllow"
                                        "f:targetRole" {"id" "ex:rootRole"}
                                        "f:action"     [{"id" "f:view"} {"id" "f:modify"}]}]}])]
      (testing "Root policy contains {:root? true} for each applicable :f/action"
        (let [sid-root-did @(fluree/internal-id db root-did)
              sid-rootRole @(fluree/internal-id db :ex/rootRole)
              policy-root  (-> @(fluree/promise-wrap (policy/policy-map db sid-root-did #{sid-rootRole} nil))
                               replace-policy-fns)]
          (is (= {"https://ns.flur.ee/ledger#modify" {:root? true}
                  "https://ns.flur.ee/ledger#view"   {:root? true}
                  :ident                             sid-root-did
                  :roles                             #{sid-rootRole}}
                 policy-root)))))))
