(ns fluree.db.query.optional-query-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.log :as log]))

(deftest ^:integration optional-queries
  (testing "Testing various 'optional' query clauses."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/optional" {:default-context ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage
                    (fluree/db ledger)
                    [{:id          :ex/brian,
                      :type        :ex/User,
                      :schema/name "Brian"
                      :ex/friend   [:ex/alice]}
                     {:id          :ex/alice,
                      :type        :ex/User,
                      :ex/favColor "Green"
                      :schema/email "alice@flur.ee"
                      :schema/name "Alice"}
                     {:id          :ex/cam,
                      :type        :ex/User,
                      :schema/name "Cam"
                      :schema/email "cam@flur.ee"
                      :ex/friend   [:ex/brian :ex/alice]}])]

      ;; basic single optional statement
      (is (= @(fluree/query db '{:select [?name ?favColor]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          {:optional [?s :ex/favColor ?favColor]}]})
             [["Cam" nil]
              ["Alice" "Green"]
              ["Brian" nil]])
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      ;; including another pass-through variable - note Brian doesn't have an email
      (is (= @(fluree/query db '{:select [?name ?favColor ?email]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          [?s :schema/email ?email]
                                          {:optional [?s :ex/favColor ?favColor]}]})
             [["Cam" nil "cam@flur.ee"]
              ["Alice" "Green" "alice@flur.ee"]]))

      ;; including another pass-through variable, but with 'optional' sandwiched
      (is (= @(fluree/query db '{:select [?name ?favColor ?email]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          {:optional [?s :ex/favColor ?favColor]}
                                          [?s :schema/email ?email]]})
             [["Cam" nil "cam@flur.ee"]
              ["Alice" "Green" "alice@flur.ee"]]))

      ;; query with two optionals!
      (is (= @(fluree/query db '{:select [?name ?favColor ?email]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          {:optional [?s :ex/favColor ?favColor]}
                                          {:optional [?s :schema/email ?email]}]})
             [["Cam" nil "cam@flur.ee"]
              ["Alice" "Green" "alice@flur.ee"]
              ["Brian" nil nil]]))

      ;; optional with unnecessary embedded vector statement
      (is (= @(fluree/query db '{:select [?name ?favColor]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          {:optional [[?s :ex/favColor ?favColor]]}]})
             [["Cam" nil]
              ["Alice" "Green"]
              ["Brian" nil]])
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      ;; Multiple optional clauses should work as a left outer join between them
      (is (= [["Cam" nil nil]
              ["Alice" "Green" "alice@flur.ee"]
              ["Brian" nil nil]]
             @(fluree/query db '{:select [?name ?favColor ?email]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/name ?name]
                                          {:optional [[?s :ex/favColor ?favColor]
                                                      [?s :schema/email ?email]]}]}))
          "Multiple optional clauses should work as a left outer join between them"))))
