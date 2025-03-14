(ns fluree.db.query.optional-query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]))

(deftest ^:integration optional-queries
  (testing "Testing various 'optional' query clauses."
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "query/optional")
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db      @(fluree/stage
                     (fluree/db ledger)
                     {"@context" context
                      "insert"
                      [{:id          :ex/brian,
                        :type        :ex/User,
                        :schema/name "Brian"
                        :ex/friend   [:ex/alice]}
                       {:id           :ex/alice,
                        :type         :ex/User,
                        :ex/favColor  "Green"
                        :schema/email "alice@flur.ee"
                        :schema/name  "Alice"}
                       {:id           :ex/cam,
                        :type         :ex/User,
                        :schema/name  "Cam"
                        :schema/email "cam@flur.ee"
                        :ex/friend    [:ex/brian :ex/alice]}]})]

      ;; basic single optional statement
      (is (= [["Alice" "Green"]
              ["Brian" nil]
              ["Cam" nil]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id ?s, :ex/favColor ?favColor}]]}))
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      (is (= [["Alice" "Green"]
              ["Brian" nil]
              ["Cam" nil]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           ["optional" {:id ?s, :ex/favColor ?favColor}]]}))
          "Cam, Alice and Brian should all return, but only Alice has a favColor, even with string 'optional' key")

      ;; including another pass-through variable - note Brian doesn't have an email
      (is (= [["Alice" "Green" "alice@flur.ee"]
              ["Cam" nil "cam@flur.ee"]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor ?email]
                                :where   '[{:id           ?s
                                            :type         :ex/User
                                            :schema/name  ?name
                                            :schema/email ?email}
                                           [:optional {:id ?s, :ex/favColor ?favColor}]]})))

      ;; including another pass-through variable, but with 'optional' sandwiched
      (is (= [["Alice" "Green" "alice@flur.ee"]
              ["Cam" nil "cam@flur.ee"]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor ?email]
                                :where   '[{:id          ?s,
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id ?s, :ex/favColor ?favColor}]
                                           {:id           ?s
                                            :schema/email ?email}]})))

      ;; query with two optionals!
      (is (= [["Alice" "Green" "alice@flur.ee"]
              ["Brian" nil nil]
              ["Cam" nil "cam@flur.ee"]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor ?email]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id ?s, :ex/favColor ?favColor}]
                                           [:optional {:id ?s, :schema/email ?email}]]})))

      ;; query with two optionals in the same vector
      (is (= [["Alice" "Green" "alice@flur.ee"]
              ["Brian" nil nil]
              ["Cam" nil "cam@flur.ee"]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor ?email]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional
                                            {:id ?s, :ex/favColor ?favColor}
                                            {:id ?s, :schema/email ?email}]]})))

      ;; optional with unnecessary embedded vector statement
      (is (= [["Alice" "Green"]
              ["Brian" nil]
              ["Cam" nil]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id ?s, :ex/favColor ?favColor}]]}))
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      ;; Multiple optional clauses should work as a left outer join between them
      (is (= [["Alice" "Green" "alice@flur.ee"]
              ["Brian" nil nil]
              ["Cam" nil nil]]
             @(fluree/query db {:context context
                                :select  '[?name ?favColor ?email]
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id           ?s,
                                                       :ex/favColor  ?favColor
                                                       :schema/email ?email}]]}))
          "Multiple optional clauses should work as a left outer join between them"))))
