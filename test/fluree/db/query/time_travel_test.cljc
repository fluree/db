(ns fluree.db.query.time-travel-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest query-with-numeric-t-value-test
  (testing "only gets results from that t"
    (let [conn   (test-utils/create-conn)
          ledger (test-utils/load-movies conn)
          db     (fluree/db ledger)
          movies @(fluree/query db {:select '{?s [:*]}
                                    :where  '[[?s :rdf/type :schema/Movie]]
                                    :t      2})]
      (is (= 3 (count movies)))
      (is (every? #{"The Hitchhiker's Guide to the Galaxy"
                    "Back to the Future" "Back to the Future Part II"}
                  (map :schema/name movies))))))

(deftest ^:slow query-with-iso8601-string-t-value-test
  (testing "only gets results from before that time"
    (let [;conn         (test-utils/create-conn) ; doesn't work see comment below
          conn         @(fluree/connect {:method :memory
                                         :defaults
                                         {:context
                                          test-utils/default-context}})
                                          ;; if the :did default below is present on the conn
                                          ;; (as it is w/ test-utils/create-conn)
                                          ;; then the tests below fail at the last check
                                          ;; b/c they can't see the last movie transacted
                                          ;; when queried by ISO-8601 string ONLY
                                          ;; (o/w it shows up just fine)
                                          ;:did (fluree.db.did/private->did-map test-utils/default-private-key)}})
          start        (System/currentTimeMillis)
          ledger       (test-utils/load-movies conn 500)
          one-loaded   (util/epoch-ms->iso-8601-str (+ start 400))
          three-loaded (util/epoch-ms->iso-8601-str (+ start 900))
          after        (util/epoch-ms->iso-8601-str (+ 60000 (System/currentTimeMillis)))
          db           (fluree/db ledger)
          base-query   {:select '{?s [:*]}
                        :where  '[[?s :rdf/type :schema/Movie]]}
          one-movie    @(fluree/query db (assoc base-query :t one-loaded))
          three-movies @(fluree/query db (assoc base-query :t three-loaded))
          all-movies   @(fluree/query db (assoc base-query :t after))]
      (is (= 1 (count one-movie)))
      (is (= 3 (count three-movies)))
      (is (= 4 (count all-movies))))))
