(ns fluree.db.query.reverse-query-test
  (:require
   [clojure.test :refer :all]
   [fluree.db.api :as fluree]
   [fluree.db.test-utils :as test-utils]))

(deftest ^:integration context-reverse-test
  (testing "Test that the @reverse context values pulls select values back correctly."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/reverse")
          context          [test-utils/default-context {:ex "http://example.org/ns/"}]
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" ["https://ns.flur.ee" context]
                    "insert"
                    [{:id           :ex/brian
                      :type         :ex/User
                      :schema/name  "Brian"
                      :ex/friend    [:ex/alice]}
                     {:id           :ex/alice
                      :type         :ex/User
                      :schema/name  "Alice"}
                     {:id           :ex/cam
                      :type         :ex/User
                      :schema/name  "Cam"
                      :ex/friend    [:ex/brian :ex/alice]}]})]

      (is (= {:schema/name "Brian"
              :friended    :ex/cam}
             @(fluree/query db {:context   [context {:friended {:reverse :ex/friend}}]
                                :selectOne {:ex/brian [:schema/name :friended]}})))

      (is (= {:schema/name "Brian"
              :friended    [:ex/cam]}
             @(fluree/query db {:context   [context {:friended {:container :set
                                                                :reverse :ex/friend}}]
                                :selectOne {:ex/brian [:schema/name :friended]}})))

      (is (= {:schema/name "Alice"
              :friended    [:ex/brian :ex/cam]}
             @(fluree/query db {:context   [context {:friended {:reverse :ex/friend}}]
                                :selectOne {:ex/alice [:schema/name :friended]}})))

      (is (= {:schema/name "Brian"
              :friended    {:id          :ex/cam
                            :type        :ex/User
                            :schema/name "Cam"
                            :ex/friend   #{{:id :ex/brian} {:id :ex/alice}}}}
             (-> db
                 (fluree/query {:context   [context {:friended {:reverse :ex/friend}}]
                                :selectOne {:ex/brian [:schema/name {:friended [:*]}]}})
                 deref
                 (update-in [:friended :ex/friend] set)))))))

(deftest ^:integration reverse-preds-in-where-and-select
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "reverse")
        db0    (fluree/db ledger)

        db1 @(fluree/stage db0 {"@context" {"ex" "http://example.org/ns/"}
                                "insert"   [{"@id"      "ex:dad"
                                             "@type"    "ex:Person"
                                             "ex:name"  "Dad"
                                             "ex:child" {"@id" "ex:kid"}}
                                            {"@id"      "ex:mom"
                                             "@type"    "ex:Person"
                                             "ex:name"  "Mom"
                                             "ex:child" {"@id" "ex:kid"}}
                                            {"@id"     "ex:kid"
                                             "@type"   "ex:Person"
                                             "ex:name" "Kiddo"}
                                            {"@id"        "ex:school"
                                             "@type"      "ex:Organization"
                                             "ex:student" "ex:kid"}]})]
    (testing "select clause"
      (is (= {"@id"     "ex:kid",
              "@type"   "ex:Person"
              "ex:name" "Kiddo",
              "parent"
              #{{"@id" "ex:mom", "ex:name" "Mom", "@type" "ex:Person" "ex:child" {"@id" "ex:kid"}}
                {"@id" "ex:dad", "ex:name" "Dad", "@type" "ex:Person" "ex:child" {"@id" "ex:kid"}}}}
             (-> @(fluree/query db1 {"@context" {"ex"     "http://example.org/ns/"
                                                 "parent" {"@reverse" "ex:child"}}
                                     "select"   {"ex:kid" ["*" {"parent" ["*"]}]}})
                 (first)
                 (update "parent" set)))))
    (testing "where clause"
      (is (= [{"@id"     "ex:kid"
               "@type"   "ex:Person"
               "ex:name" "Kiddo"}]
             @(fluree/query db1 {"@context"       {"ex"     "http://example.org/ns/"
                                                   "parent" {"@reverse" "ex:child"}}
                                 "where"          {"@id" "?s" "parent" "?x"}
                                 "selectDistinct" {"?s" ["*"]}}))))

    (testing "@type reverse"
      (is (= #{"ex:Person" "ex:Organization"}
             (set @(fluree/query db1 {"@context"       {"ex"           "http://example.org/ns/"
                                                        "isTypeObject" {"@reverse" "@type"}}
                                      "where"          {"@id" "?class" "isTypeObject" "?x"}
                                      "selectDistinct" "?class"})))))
    (testing "@type forward"
      (is (= #{"ex:Person" "ex:Organization"}
             (set @(fluree/query db1 {"@context"       {"ex" "http://example.org/ns/"}
                                      "where"          {"@id" "?x" "@type" "?class"}
                                      "selectDistinct" "?class"})))))))
