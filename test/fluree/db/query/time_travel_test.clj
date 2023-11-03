(ns fluree.db.query.time-travel-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest query-with-numeric-t-value-test
  (testing "only gets results from that t"
    (let [conn   (test-utils/create-conn)
          ledger (test-utils/load-movies conn)
          db     (fluree/db ledger)
          movies @(fluree/query db '{:select {?s [:*]}
                                     :where  {:id ?s, :type :schema/Movie}
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
          base-query             '{:select {?s [:*]}
                                   :where  {:id ?s, :type :schema/Movie}}
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

(deftest ^:integration federated-time-travel-test
  (testing "Federated queries with time travel"
    (let [conn    (test-utils/create-conn {:defaults {:context-type :string
                                                      :context      {"id"     "@id",
                                                                     "type"   "@type",
                                                                     "ex"     "http://example.org/",
                                                                     "f"      "https://ns.flur.ee/ledger#",
                                                                     "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                                     "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                                                     "schema" "http://schema.org/",
                                                                     "xsd"    "http://www.w3.org/2001/XMLSchema#"}}})

          ledger1 @(fluree/create-with-txn conn
                                      {"f:ledger" "test/time1"
                                       "@graph"   [{"@id"   "ex:time-test"
                                                    "@type" "ex:foo"
                                                    "ex:time" 1}]}
                                      {:context-type :string})
          _ @(fluree/transact! conn {"f:ledger" "test/time1"
                                     "@graph"   [{"@id"   "ex:time-test"
                                                  "ex:time" 2}]}
                                {:context-type :string})]

      (testing "Single ledger"
        (let [q '{:from   "test/time1"
                  :select {"ex:time-test" ["*"]}
                  :t      1}]
          (is (= [{:id       "ex:time-test"
                   :type     "ex:foo"
                   "ex:time" 1}]
                 @(fluree/query-connection conn q))
              "should return only results for `t` of `1`"))
        (let [q            '{:from   "test/time1"
                             :select {"ex:time-test" ["*"]}
                             :t      "1988-05-30T12:40:44.823Z"}
              invalid-time (try @(fluree/query-connection conn q)
                                (catch Exception e e))]
          (is (str/includes?  (ex-message invalid-time)
                              "There is no data as of")
              "should return an error")))
      (testing "Across multiple ledgers"
        (let [ledger2 @(fluree/create-with-txn conn
                                               {"f:ledger" "test/time2"
                                                "@graph"   [{"@id"   "ex:time-test"
                                                             "ex:p1" "value1"}
                                                            {"@id" "ex:foo"
                                                             "ex:p2" "t1"}]}
                                               {:context-type :string})
              _ @(fluree/transact! conn
                                   {"f:ledger" "test/time2"
                                    "@graph"   [{"@id"   "ex:time-test"
                                                 "ex:p1" "value2"}
                                                {"@id"   "ex:foo"
                                                 "ex:p2" "t2"}]}
                                   {:context-type :string})]

          (let [q '{:from   ["test/time1" "test/time2"]
                    :select [?p1 ?time]
                    :where {"@id" "ex:time-test"
                            "ex:p1" ?p1
                            "ex:time" ?time}
                    :t      1}]
            (is (= [["value1" 1]]
                   @(fluree/query-connection conn q))
                "should return results for `t` of `1` across both ledgers")))
        (let [q '{:from-named ["test/time1" "test/time2"]
                  :select     [?p2 ?time]
                  :where      [[:graph "test/time1" {"@id"     "ex:time-test"
                                                     "ex:time" ?time}]
                               [:graph "test/time2" {"@id"   "ex:foo"
                                                     "ex:p2" ?p2}]]
                  :t 1}]
          (is (= [["t1" 1]]
                 @(fluree/query-connection conn q))
              "should be results as of `t` = 1 for both ledgers"))
        (testing "Some ledgers do not have data for given t"
          (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
            (let [ledger-valid @(fluree/create-with-txn conn
                                                         {"f:ledger" "test/time-before"
                                                          "@graph" [{"@id" "ex:time-test"
                                                                     "ex:p1" "value"}]}
                                                         {:context-type :string})]
              (let [q '{:from   ["test/time1" "test/time2" "test/time-before"]
                        :select [?p1 ?time]
                        :where {"@id" "ex:time-test"
                                "ex:p1" ?p1
                                "ex:time" ?time}
                        ;;`t` is valid for `ledger-valid`,
                        ;;but not the others
                        :t      "1988-05-30T12:40:44.823Z"}
                    invalid-time (try @(fluree/query-connection conn q)
                                      (catch Exception e e))]
                (is (str/includes? (ex-message invalid-time)
                                   "There is no data as of")
                    "should return an error")))))))))
