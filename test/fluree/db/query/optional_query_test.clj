(ns fluree.db.query.optional-query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration optional-queries
  (testing "Testing various 'optional' query clauses."
    (let [conn    (test-utils/create-conn)
          db0 @(fluree/create conn "query/optional")
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db      @(fluree/update
                    db0
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

(deftest nested-optionals
  (let [conn @(fluree/connect-memory)
        db0  @(fluree/create conn "optional-vars")
        db1  @(fluree/insert db0 {"@context" {"ex" "http://example.com/"}
                                  "@graph"
                                  [{"@id"    "ex:1"
                                    "ex:lit" "literal1"
                                    "ex:ref" {"@id"    "ex:2"
                                              "ex:lit" "literal2"
                                              "ex:ref" {"@id"    "ex:3"
                                                        "ex:lit" "literal3"
                                                        "ex:ref" {"@id"    "ex:4"
                                                                  "ex:lit" "literal4"
                                                                  "ex:ref" {"@id" "ex:5"}}}}}]})]
    (is (= [["ex:1" "ex:lit" "literal1" nil nil nil nil nil nil]
            ["ex:1" "ex:ref" "ex:2" "ex:lit" "literal2" nil nil nil nil]
            ["ex:1" "ex:ref" "ex:2" "ex:ref" "ex:3" "ex:lit" "literal3" nil nil]
            ["ex:1" "ex:ref" "ex:2" "ex:ref" "ex:3" "ex:ref" "ex:4" "ex:lit" "literal4"]
            ["ex:1" "ex:ref" "ex:2" "ex:ref" "ex:3" "ex:ref" "ex:4" "ex:ref" "ex:5"]]
           @(fluree/query db1 {"@context" {"ex" "http://example.com/"}
                               "where"    [{"@id" "?s1" "ex:lit" "literal1"}
                                           {"@id" "?s1" "?p1" "?o1"}
                                           ["optional"
                                            {"@id" "?o1" "?p2" "?o2"}
                                            ["optional"
                                             {"@id" "?o2" "?p3" "?o3"}
                                             ["optional"
                                              {"@id" "?o3" "?p4" "?o4"}]]]]
                               "select"   ["?s1" "?p1" "?o1" "?p2" "?o2" "?p3" "?o3" "?p4" "?o4"]})))))
