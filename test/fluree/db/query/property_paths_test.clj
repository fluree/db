(ns fluree.db.query.property-paths-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration recursive-+-queries
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/recur+" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
                  [{:id          :ex/brian,
                    :type        :ex/User,
                    :schema/name "Brian"
                    :ex/last     "Smith"
                    :ex/friend   [:ex/alice]}
                   {:id          :ex/alice,
                    :type        :ex/User,
                    :schema/name "Alice"
                    :ex/friend   [:ex/david :ex/jerry]}
                   {:id          :ex/cam,
                    :type        :ex/User,
                    :schema/name "Cam"
                    :ex/last     "Jones"
                    :ex/friend   [:ex/brian]}
                   {:id          :ex/david
                    :schema/name "David"
                    :type        :ex/User
                    :ex/friend   [:ex/cam]}
                   {:id          :ex/jerry
                    :schema/name "jerry"
                    :type        :ex/User}])]
    (testing "no depth supplied, contains a loop"
      (is (= [[:ex/brian "Brian"]
              [:ex/alice "Alice"]
              [:ex/david "David"]
              [:ex/jerry "jerry"]
              [:ex/cam "Cam"]
              [:ex/brian "Brian"]
              [:ex/alice "Alice"]
              [:ex/david "David"]
              [:ex/jerry "jerry"]
              [:ex/cam "Cam"]]
             (take 10 @(fluree/query db {:select ['?friend '?name]
                                         :where  [[:ex/cam :ex/friend+ '?friend]
                                                  ['?friend :schema/name '?name]]}))))
      (is (= [[:ex/brian "Brian"]
              [:ex/alice "Alice"]
              [:ex/david "David"]
              [:ex/jerry "jerry"]
              [:ex/cam "Cam"]
              [:ex/brian "Brian"]
              [:ex/alice "Alice"]
              [:ex/david "David"]
              [:ex/jerry "jerry"]
              [:ex/cam "Cam"]]
             (take 10 @(fluree/query db {:select ['?friend '?name]
                                         :where  [['?s :id :ex/cam]
                                                  ['?s :ex/friend+ '?friend]
                                                  ['?friend :schema/name '?name]]})))))
    (testing "with supplied depth"
      (is (= [[:ex/brian "Brian"]
              [:ex/alice "Alice"]]
             @(fluree/query db {:select ['?friend '?name]
                                :where  [[:ex/cam :ex/friend+2 '?friend]
                                         ['?friend :schema/name '?name]]}))))))
