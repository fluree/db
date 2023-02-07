(ns fluree.db.policy.subj-flakes-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.did :as did]
    [fluree.db.permissions-validate :as policy-enforce]
    [clojure.core.async :as async]))

;; tests for the optimized policy filtering for groups of flakes of the same subject
;; (used for simple subject crawl)

(deftest ^:integration subject-flakes-policy
  (testing "Policy enforcement for groups of flakes by subject."
    (let [conn            (test-utils/create-conn)
          ledger          @(fluree/create conn "policy/a" {:context {:ex "http://example.org/ns/"}})
          root-did        (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
          alice-did       (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          db              @(fluree/stage
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

          db+policy       @(fluree/stage
                             db
                             ;; add policy targeting :ex/rootRole that can view and modify everything
                             [{:id           :ex/rootPolicy,
                               :type         [:f/Policy],   ;; must be of type :f/Policy, else it won't be treated as a policy
                               :f/targetNode :f/allNodes    ;; :f/allNodes special keyword meaning every node (everything)
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
          ;; get a group of flakes that we know will have different permissions for different users.
          john-flakes     @(fluree/range db+policy :spot = [:ex/john])
          alice-flakes    @(fluree/range db+policy :spot = [(fluree/expand-iri db+policy :ex/alice)])
          widget-flakes   @(fluree/range db+policy :spot = [(fluree/expand-iri db+policy :ex/widget)])

          alice-db        @(fluree/wrap-policy db+policy {:f/$identity alice-did
                                                          :f/role      :ex/userRole})

          ;; john's flakes filtered using alice's policy-enforced db
          alice-db-john   (->> john-flakes
                               (policy-enforce/filter-subject-flakes alice-db)
                               async/<!!)
          ;; alice's flakes filtered using alice's policy-enforced db
          alice-db-alice  (->> alice-flakes
                               (policy-enforce/filter-subject-flakes alice-db)
                               async/<!!)
          ;; widget flakes filtered using alice's policy-enforced db
          alice-db-widget (->> widget-flakes
                               (policy-enforce/filter-subject-flakes alice-db)
                               async/<!!)]

      (is (= [#Flake [211106232532994 0 "http://example.org/ns/john" 1 -1 true nil]
              #Flake [211106232532994 200 1001 0 -1 true nil]
              #Flake [211106232532994 1002 "John" 1 -1 true nil]
              #Flake [211106232532994 1003 "john@flur.ee" 1 -1 true nil]
              #Flake [211106232532994 1004 "2021-08-17" 1 -1 true nil]]
             alice-db-john)
          "Alice cannot see John's ssn, but can see everything else.")

      (is (= [#Flake [211106232532992 0 "http://example.org/ns/alice" 1 -1 true nil]
              #Flake [211106232532992 200 1001 0 -1 true nil]
              #Flake [211106232532992 1002 "Alice" 1 -1 true nil]
              #Flake [211106232532992 1003 "alice@flur.ee" 1 -1 true nil]
              #Flake [211106232532992 1004 "2022-08-17" 1 -1 true nil]
              #Flake [211106232532992 1005 "111-11-1111" 1 -1 true nil]
              #Flake [211106232532992 1006 211106232532993 0 -1 true nil]]
             alice-db-alice)
          "Alice can see all flakes for herself, including her ssn.")

      (is (= []
             alice-db-widget)
          "Alice wasn't given any permissions for class :ex/Product, so should see nothing for widget."))))