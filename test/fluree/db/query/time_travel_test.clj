(ns fluree.db.query.time-travel-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest query-with-numeric-t-value-test
  (testing "only gets results from that t"
    (let [conn   (test-utils/create-conn)
          ledger (test-utils/load-movies conn)
          db     (fluree/db ledger)
          movies @(fluree/query db '{:select {?s [:*]}
                                     :where  [[?s :type :schema/Movie]]
                                     :t      2})]
      (is (= 3 (count movies)))
      (is (every? #{"The Hitchhiker's Guide to the Galaxy"
                    "Back to the Future"
                    "Back to the Future Part II"}
                  (map :schema/name movies))))))

(deftest query-with-iso8601-string-t-value-test
  (testing "only gets results from before that time"
    (let [;conn         (test-utils/create-conn) ; doesn't work see comment below
          conn                   @(fluree/connect {:method :memory
                                                   :defaults
                                                   {:context      test-utils/default-context
                                                    :context-type :keyword}})
          ;; if the :did default below is present on the conn
          ;; (as it is w/ test-utils/create-conn)
          ;; then the tests below fail at the last check
          ;; b/c they can't see the last movie transacted
          ;; when queried by ISO-8601 string ONLY
          ;; (o/w it shows up just fine)
          ;:did (fluree.db.did/private->did-map test-utils/default-private-key)}})
          start-iso              "2022-10-05T00:00:00Z"
          start                  (util/str->epoch-ms start-iso)
          three-loaded-millis    (+ start 60000)
          all-loaded-millis      (+ three-loaded-millis 60000)
          three-loaded-iso       (util/epoch-ms->iso-8601-str three-loaded-millis)
          all-loaded-iso         (util/epoch-ms->iso-8601-str all-loaded-millis)
          after-one-loaded-iso   (util/epoch-ms->iso-8601-str (+ start 5000))
          after-three-loaded-iso (util/epoch-ms->iso-8601-str (+ three-loaded-millis
                                                                 5000))
          after-all-loaded-iso   (util/epoch-ms->iso-8601-str (+ all-loaded-millis
                                                                 5000))
          too-early-iso          (util/epoch-ms->iso-8601-str (- start
                                                                 (* 24 60 60 1000)))
          ledger                 @(fluree/create conn "iso8601/test")
          _                      (with-redefs-fn {#'util/current-time-millis
                                                  (fn [] start)
                                                  #'util/current-time-iso
                                                  (fn [] start-iso)}
                                   (fn []
                                     (let [db1 @(fluree/stage
                                                  (fluree/db ledger)
                                                  (first test-utils/movies))]
                                       @(fluree/commit! ledger db1))))
          _                      (with-redefs-fn {#'util/current-time-millis
                                                  (fn [] three-loaded-millis)
                                                  #'util/current-time-iso
                                                  (fn [] three-loaded-iso)}
                                   (fn []
                                     (let [db2 @(fluree/stage
                                                  (fluree/db ledger)
                                                  (second test-utils/movies))]
                                       @(fluree/commit! ledger db2))))
          _                      (with-redefs-fn {#'util/current-time-millis
                                                  (fn [] all-loaded-millis)
                                                  #'util/current-time-iso
                                                  (fn [] all-loaded-iso)}
                                   (fn []
                                     (let [db3 @(fluree/stage
                                                  (fluree/db ledger)
                                                  (nth test-utils/movies 2))]
                                       @(fluree/commit! ledger db3))))
          db                     (fluree/db ledger)
          base-query             {:select '{?s [:*]}
                                  :where  '[[?s :type :schema/Movie]]}
          one-movie              @(fluree/query db (assoc base-query
                                                     :t after-one-loaded-iso))
          three-movies           @(fluree/query db (assoc base-query
                                                     :t after-three-loaded-iso))
          all-movies             @(fluree/query db (assoc base-query
                                                     :t after-all-loaded-iso))
          too-early              @(fluree/query db (assoc base-query
                                                     :t too-early-iso))]
      (is (= 1 (count one-movie)))
      (is (= 3 (count three-movies)))
      (is (= 4 (count all-movies)))
      (is (util/exception? too-early))
      (is (re-matches #"There is no data as of .+" (ex-message too-early))))))
