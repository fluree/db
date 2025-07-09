(ns fluree.db.query.time-travel-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest query-with-numeric-t-value-test
  (testing "only gets results from that t"
    (let [conn   (test-utils/create-conn)
          ledger (test-utils/load-movies conn)
          db     (fluree/db ledger)
          movies @(fluree/query db {:context [test-utils/default-context
                                              {:ex "http://example.org/ns/"}]
                                    :select  '{?s [:*]}
                                    :where   '{:id ?s, :type :schema/Movie}
                                    :t       2})]
      (is (= 3 (count movies)))
      (is (every? #{"The Hitchhiker's Guide to the Galaxy"
                    "Back to the Future"
                    "Back to the Future Part II"}
                  (map :schema/name movies))))))

(deftest query-with-iso8601-string-t-value-test
  (testing "only gets results from before that time"
    (let [;conn         (test-utils/create-conn) ; doesn't work see comment below
          conn                   @(fluree/connect-memory)
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
                                     (let [db1 @(fluree/update
                                                 (fluree/db ledger)
                                                 {"@context" [test-utils/default-context
                                                              {:ex "http://example.org/ns/"}]
                                                  "insert"   (first test-utils/movies)})]
                                       @(fluree/commit! ledger db1))))
          _                      (with-redefs-fn {#'util/current-time-millis
                                                  (fn [] three-loaded-millis)
                                                  #'util/current-time-iso
                                                  (fn [] three-loaded-iso)}
                                   (fn []
                                     (let [db2 @(fluree/update
                                                 (fluree/db ledger)
                                                 {"@context" [test-utils/default-context
                                                              {:ex "http://example.org/ns/"}]
                                                  "insert"   (second test-utils/movies)})]
                                       @(fluree/commit! ledger db2))))
          _                      (with-redefs-fn {#'util/current-time-millis
                                                  (fn [] all-loaded-millis)
                                                  #'util/current-time-iso
                                                  (fn [] all-loaded-iso)}
                                   (fn []
                                     (let [db3 @(fluree/update
                                                 (fluree/db ledger)
                                                 {"@context" [test-utils/default-context
                                                              {:ex "http://example.org/ns/"}]
                                                  "insert"   (nth test-utils/movies 2)})]
                                       @(fluree/commit! ledger db3))))
          db                     (fluree/db ledger)
          base-query             {:context test-utils/default-context
                                  :select  '{?s [:*]}
                                  :where   '{:id ?s, :type :schema/Movie}}
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

(deftest ^:integration query-connection-time-travel
  (testing "query-connection queries with time travel"
    (let [t1         "2023-11-04T00:00:00Z"
          query-time "2023-11-05T00:00:00Z"
          t2         "2023-11-06T00:00:00Z"
          conn       @(fluree/connect-memory)
          context    {"id"     "@id",
                      "type"   "@type",
                      "ex"     "http://example.org/",
                      "f"      "https://ns.flur.ee/ledger#",
                      "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                      "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                      "schema" "http://schema.org/",
                      "xsd"    "http://www.w3.org/2001/XMLSchema#"}

          _ledger1 (with-redefs [util/current-time-iso (fn [] t1)]
                     @(fluree/create-with-txn conn
                                              {"@context" context
                                               "ledger"   "test/time1"
                                               "insert"   [{"@id"     "ex:time-test"
                                                            "@type"   "ex:foo"
                                                            "ex:time" 1}]}))
          _ledger2 (with-redefs [util/current-time-iso (fn [] t1)]
                     @(fluree/create-with-txn conn
                                              {"@context" context
                                               "ledger"   "test/time2"
                                               "insert"   [{"@id"   "ex:time-test"
                                                            "ex:p1" "value1"}
                                                           {"@id"   "ex:foo"
                                                            "ex:p2" "t1"}]}))
          _        (with-redefs [util/current-time-iso (fn [] t2)]
                     @(fluree/update! conn {"@context" context
                                            "ledger"   "test/time1"
                                            "insert"   [{"@id"     "ex:time-test"
                                                         "ex:time" 2}]}))
          _        (with-redefs [util/current-time-iso (fn [] t2)]
                     @(fluree/update! conn
                                      {"@context" context
                                       "ledger"   "test/time2"
                                       "insert"   [{"@id"   "ex:time-test"
                                                    "ex:p1" "value2"}
                                                   {"@id"   "ex:foo"
                                                    "ex:p2" "t2"}]}))]
      (testing "Single ledger"
        (let [q {:context context
                 :from    "test/time1"
                 :select  {"ex:time-test" ["*"]}
                 :t       1}]
          (is (= [{"id"      "ex:time-test"
                   "type"    "ex:foo"
                   "ex:time" 1}]
                 @(fluree/query-connection conn q))
              "should return only results for `t` of `1`"))
        (let [q {:context context
                 :from    "test/time1"
                 :select  {"ex:time-test" ["*"]}
                 :t       query-time}]
          (is (= [{"id"      "ex:time-test"
                   "type"    "ex:foo"
                   "ex:time" 1}]
                 @(fluree/query-connection conn q))
              "should return only results for `t` of `1`"))
        (let [q            {:context context
                            :from    "test/time1"
                            :select  {"ex:time-test" ["*"]}
                            :t       "1988-05-30T12:40:44.823Z"}
              invalid-time (try @(fluree/query-connection conn q)
                                (catch Exception e e))]

          (is (util/exception? invalid-time))
          (is (= 400 (-> invalid-time ex-data :status)))
          (is (str/includes?  (ex-message invalid-time)
                              "There is no data as of"))
          (is (str/includes? (ex-message invalid-time)
                             "test/time1")
              "message should report which ledger has an error")))
      (testing "Across multiple ledgers"
        (let [q {:context context
                 :from    ["test/time1" "test/time2"]
                 :select  '[?p1 ?time]
                 :where   '{"@id"     "ex:time-test"
                            "ex:p1"   ?p1
                            "ex:time" ?time}
                 :t       query-time}]
          (is (= [["value1" 1]]
                 @(fluree/query-connection conn q))
              "should return results for first commit from both ledgers"))
        (testing "from-named"
          (let [q {:context    context
                   :from-named ["test/time1" "test/time2"]
                   :select     '[?p2 ?time]
                   :where      '[[:graph "test/time1" {"@id"     "ex:time-test"
                                                       "ex:time" ?time}]
                                 [:graph "test/time2" {"@id"   "ex:foo"
                                                       "ex:p2" ?p2}]]
                   :t          query-time}]
            (is (= [["t1" 1]]
                   @(fluree/query-connection conn q))
                "should be results as of `t` = 1 for both ledgers")))
        (testing "Not all ledgers have data for given `t`"
          (with-redefs [util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
            (let [_ledger-valid @(fluree/create-with-txn conn
                                                         {"@context" context
                                                          "ledger"   "test/time-before"
                                                          "insert"   [{"@id"   "ex:time-test"
                                                                       "ex:p1" "value"}]})
                  q             {:context context
                                 :from    ["test/time1" "test/time-before"]
                                 :select  '[?p1 ?time]
                                 :where   '{"@id"     "ex:time-test"
                                            "ex:p1"   ?p1
                                            "ex:time" ?time}
                                 ;;`t` is valid for "ledger-valid",
                                 ;;but not "test/time1"
                                 :t       "1988-05-30T12:40:44.823Z"}
                  invalid-time  (try @(fluree/query-connection conn q)
                                     (catch Exception e e))]

              (is (util/exception? invalid-time))
              (is (= 400 (-> invalid-time ex-data :status)))
              (is (str/includes? (ex-message invalid-time)
                                 "There is no data as of"))
              (is (str/includes? (ex-message invalid-time)
                                 "test/time1")
                  "message should report which ledger has an error"))))
        (testing "Federated queries must use wall-clock time as global `t` value"
          (with-redefs [util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
            (let [q            {:context context
                                :from    ["test/time1" "test/time-before"]
                                :select  '[?p1 ?time]
                                :where   '{"@id"     "ex:time-test"
                                           "ex:p1"   ?p1
                                           "ex:time" ?time}
                                :t       1}
                  invalid-time (try @(fluree/query-connection conn q)
                                    (catch Exception e e))]
              (is (util/exception? invalid-time))
              (is (= 400 (-> invalid-time ex-data :status)))
              (is (str/includes? (ex-message invalid-time)
                                 "Error in federated query: top-level `t` value")
                  "error message should indicate invalid t value type"))))))))
