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
    (is (= [{:id          :ex/brian,
             :rdf/type    [:ex/User],
             :schema/name "Brian",
             :ex/last     "Smith",
             :ex/friend   {:id :ex/alice}}
            {:id          :ex/alice,
             :rdf/type    [:ex/User],
             :schema/name "Alice",
             :ex/friend   [{:id :ex/david}
                           {:id :ex/jerry}]}
            {:id          :ex/david,
             :rdf/type    [:ex/User],
             :schema/name "David",
             :ex/friend   {:id :ex/cam}}
            {:id          :ex/jerry,
             :rdf/type    [:ex/User],
             :schema/name "jerry"}
            {:id          :ex/cam,
             :rdf/type    [:ex/User],
             :schema/name "Cam",
             :ex/last     "Jones",
             :ex/friend   {:id :ex/brian}}]
           @(fluree/query db {:select {'?friend [:*]}
                              :where  [[:ex/cam :ex/friend+ '?friend]]})))
    (is (= [{:id          :ex/brian,
             :rdf/type    [:ex/User]
             :schema/name "Brian",
             :ex/last     "Smith",
             :ex/friend   {:id :ex/alice}}
            {:id          :ex/alice,
             :rdf/type    [:ex/User],
             :schema/name "Alice",
             :ex/friend   [{:id :ex/david}
                           {:id :ex/jerry}]}]
           @(fluree/query db {:select {'?friend [:*]}
                              :where  [[:ex/cam :ex/friend+2 '?friend]]})))))
