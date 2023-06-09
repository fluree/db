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
                  [{:id           :ex/brian,
                    :type         :ex/User,
                    :schema/name  "Brian"
                    :ex/last      "Smith"
                    :ex/friend    [:ex/alice]}
                   {:id           :ex/alice,
                    :type         :ex/User,
                    :schema/name  "Alice"
                    :ex/last      "Walker"
                    :ex/friend [:ex/david]}
                   {:id          :ex/cam,
                    :type        :ex/User,
                    :schema/name "Cam"
                    :ex/last     "Jones"
                    :ex/friend   [:ex/brian]}
                   {:id :ex/david
                    :schema/name "David"
                    :type :ex/User}])]
    (is (= [{:id :ex/brian,
             :rdf/type [:ex/User],
             :schema/name "Brian",
             :ex/last "Smith",
             :ex/friend {:id :ex/alice}}
            {:id :ex/alice,
             :rdf/type [:ex/User],
             :schema/name "Alice",
             :ex/last "Walker",
             :ex/friend {:id :ex/david}}
            {:id :ex/david,
             :schema/name "David"
             :rdf/type [:ex/User]}]
           @(fluree/query db {:select {'?friend [:*]}
                              :where  [[:ex/cam :ex/friend+ '?friend]]})))
    (is (= [{:id :ex/brian,
             :rdf/type [:ex/User],
             :schema/name "Brian",
             :ex/last "Smith",
             :ex/friend {:id :ex/alice}}
            {:id :ex/alice,
             :rdf/type [:ex/User],
             :schema/name "Alice",
             :ex/last "Walker",
             :ex/friend {:id :ex/david}}]
           @(fluree/query db {:select {'?friend [:*]}
                              :where  [[:ex/cam :ex/friend+1 '?friend]]})))))
