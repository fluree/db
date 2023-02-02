(ns fluree.db.query.range-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.dbproto :as dbproto]
    [clojure.core.async :as async]))

;; tests for index-range calls (just some basic tests for now)

(deftest ^:integration index-range-basic
  (testing "Index-range calls with various options."
    (let [conn                 (test-utils/create-conn)
          ledger               @(fluree/create conn "policy/a" {:context {:ex "http://example.org/ns/"}})
          ;; get some basic data transacted
          db                   @(fluree/stage
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
                                    :schema/priceCurrency "USD"}])
          ;; get a group of flakes that we know will have different permissions for different users.
          john-flakes-compact  @(fluree/range db :spot = [:ex/john])
          john-flakes-expanded @(fluree/range db :spot = [(fluree/expand-iri db :ex/john)])
          john-flakes-sid      @(fluree/range db :spot = [(async/<!! (dbproto/-subid db :ex/john))])

          alice-flakes         @(fluree/range db :spot = [:ex/alice])
          widget-flakes        @(fluree/range db :spot = [:ex/widget])]


      ;; root can see all user data
      (is (= john-flakes-compact
             john-flakes-expanded
             john-flakes-sid)
          "query-range should properly expand IRIs")

      (is (= [#Flake [211106232532992 0 "http://example.org/ns/alice" 1 -1 true nil]
              #Flake [211106232532992 200 1002 0 -1 true nil]
              #Flake [211106232532992 1003 "Alice" 1 -1 true nil]
              #Flake [211106232532992 1004 "alice@flur.ee" 1 -1 true nil]
              #Flake [211106232532992 1005 "2022-08-17" 1 -1 true nil]
              #Flake [211106232532992 1006 "111-11-1111" 1 -1 true nil]
              #Flake [211106232532992 1007 211106232532993 0 -1 true nil]]
             alice-flakes))


      (is (= [#Flake [211106232532995 0 "http://example.org/ns/widget" 1 -1 true nil]
              #Flake [211106232532995 200 1010 0 -1 true nil]
              #Flake [211106232532995 1003 "Widget" 1 -1 true nil]
              #Flake [211106232532995 1011 99.99 5 -1 true nil]
              #Flake [211106232532995 1012 "USD" 1 -1 true nil]]
             widget-flakes)))))

