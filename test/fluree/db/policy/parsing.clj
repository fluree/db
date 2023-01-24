(ns fluree.db.policy.parsing
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.did :as did]
    [fluree.db.json-ld.policy :as policy]
    [fluree.db.util.async :refer [<? <?? go-try]]
    [fluree.db.util.log :as log]
    [fluree.db.dbproto :as dbproto]))

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
          ledger       @(fluree/create conn "policy-parse/a" {:context {:ex "http://example.org/ns/"}})
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
                                                        :f/equals     {:list [:f/$identity :ex/user]}}]}]}])
          sid-userRole @(fluree/promise-wrap (dbproto/-subid db :ex/userRole))
          sid-alice    @(fluree/promise-wrap (dbproto/-subid db alice-did))
          sid-User     @(fluree/promise-wrap (dbproto/-subid db :ex/User))
          sid-ssn      @(fluree/promise-wrap (dbproto/-subid db :schema/ssn))
          ;; create optimized policy map for ex:userRole
          policy-alice @(fluree/promise-wrap (policy/policy-map db alice-did :ex/userRole nil))
          policy-root  @(fluree/promise-wrap (policy/policy-map db root-did :ex/rootRole nil))]

      ;; look at  policy for user
      (is (= (replace-policy-fns policy-alice)              ;; replace compiled functions for data comparisons below
             {:f/modify {:class {sid-User {:default {:f/equals     [{:id :f/$identity}
                                                                    {:id :ex/user}]
                                                     :f/targetRole {:_id sid-userRole}
                                                     :function     [true
                                                                    :fluree.db.policy.parsing/replaced-policy-function]
                                                     :id           "_:f211106232533008"}}}}
              :f/view   {:class {sid-User {sid-ssn  {:f/equals     [{:id :f/$identity}
                                                                    {:id :ex/user}]
                                                     :f/targetRole {:_id sid-userRole}
                                                     :function     [true
                                                                    :fluree.db.policy.parsing/replaced-policy-function]
                                                     :id           :ex/ssnViewRule}
                                           :default {:f/targetRole {:_id sid-userRole}
                                                     :function     [false
                                                                    :fluree.db.policy.parsing/replaced-policy-function]
                                                     :id           :ex/globalViewAllow}}}}
              :ident    sid-alice
              :roles    #{sid-userRole}})
          "Policies for only :ex/userRole should return"))))
