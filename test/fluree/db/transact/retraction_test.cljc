(ns fluree.db.transact.retraction-test
  (:require [clojure.core.async :refer [go <!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils #?(:clj :refer :cljs :refer-macros )[test-async]]
            [fluree.db.util.async :refer [p->c <?]]))

(deftest ^:integration retracting-data
  (test-async
   (testing "Retractions of individual properties and entire subjects."
     (go
      (let [conn           (<? (p->c (test-utils/create-conn')))
            ledger         (<? (p->c (fluree/create conn "tx/retract")))
            db             (<? (p->c (fluree/stage
                                      (fluree/db ledger)
                                      {:context ["" {:ex "http://example.org/ns/"}]
                                       :graph   [{:id          :ex/alice,
                                                  :type        :ex/User,
                                                  :schema/name "Alice"
                                                  :schema/age  42}
                                                 {:id          :ex/bob,
                                                  :type        :ex/User,
                                                  :schema/name "Bob"
                                                  :schema/age  22}
                                                 {:id          :ex/jane,
                                                  :type        :ex/User,
                                                  :schema/name "Jane"
                                                  :schema/age  30}]})))
            ; retract Alice's age attribute by using nil
            db-age-retract (<? (p->c (fluree/stage
                                      db
                                      {:context    ["" {:ex "http://example.org/ns/"}]
                                       :id         :ex/alice,
                                       :schema/age nil})))]
        (is (= (<! (p->c (fluree/query db-age-retract
                                       '{:context ["" {:ex "http://example.org/ns/"}],
                                         :select {?s [:*]},
                                         :where [[?s :id :ex/alice]]})))
               [{:id           :ex/alice,
                 :rdf/type     [:ex/User],
                 :schema/name  "Alice"}])
            "Alice should no longer have an age property"))))))
