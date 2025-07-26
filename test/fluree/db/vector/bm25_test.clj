(ns fluree.db.vector.bm25-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration basic-connection-test
  (testing "Basic connection and ledger operations work"
    (let [conn   (test-utils/create-conn)]
      (testing "connection creation succeeds"
        (is (some? conn)))

      (testing "ledger creation succeeds"
        (let [ledger @(fluree/create conn "test-basic")]
          (is (some? ledger))))

      (testing "data insertion succeeds"
        (let [db @(fluree/insert! conn "test-basic"
                                  {"@context" {"ex" "http://example.org/"}
                                   "@graph" [{"@id" "ex:article1"
                                              "@type" "ex:Article"
                                              "ex:title" "Test Article"}]})]
          (is (some? db)))))))

(deftest ^:integration bm25-creation-test
  (testing "Basic virtual graph creation test"
    (let [conn   (test-utils/create-conn)
          _ledger @(fluree/create conn "bm25-creation")
          _db     @(fluree/insert! conn "bm25-creation"
                                   {"@context" {"ex" "http://example.org/"}
                                    "@graph" [{"@id" "ex:article1"
                                               "@type" "ex:Article"
                                               "ex:title" "Introduction to Fluree"
                                               "ex:content" "Fluree is a semantic graph database"}]})
          ;; Create VG using new API
          vg-result @(fluree/create-virtual-graph
                      conn
                      {:name "creation-test-index"
                       :type :bm25
                       :config {:ledgers ["bm25-creation"]
                                :query {"@context" {"ex" "http://example.org/"}
                                        "where" [{"@id" "?x"
                                                  "@type" "ex:Article"}]
                                        "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})]

      (println "RESULT: " vg-result)
      (testing "virtual graph creation succeeds"
        (is (some? vg-result))
        (is (= "creation-test-index" vg-result))))))

(defn full-text-search
  "Performs a full text search and returns a couple attributes joined from the db
  for use of tests below"
  [db search-term]
  @(fluree/query db {"@context" {"ex"   "http://example.org/ns/"
                                 "fidx" "https://ns.flur.ee/index#"}
                     "select"   ["?x", "?score", "?title"]
                     "where"    [["graph" "##articleSearch" {"fidx:target" search-term
                                                             "fidx:limit"  10,
                                                             "fidx:sync"   true,
                                                             "fidx:result" {"@id"        "?x"
                                                                            "fidx:score" "?score"}}]
                                 {"@id"      "?x"
                                  "ex:title" "?title"}]}))

(defn has-index?
  [db]
  (-> db :stats :indexed pos-int?))

(defn async-db->flake-db
  [db]
  (if-let [c (:db-chan db)]
    (async/<!! c)
    db))

(defn db-with-index
  "Reapeatedly creates a new conn to force a new db load
  until a db is loaded which contains a valid index."
  ([conn-settings ledger-name] (db-with-index conn-settings ledger-name 0))
  ([conn-settings ledger-name retry-count]
   (let [db (-> @(fluree/connect-file conn-settings)
                (fluree/load ledger-name)
                deref
                fluree/db
                async-db->flake-db)]
     (if (has-index? db)
       db
       (if (> retry-count 20)
         (throw (ex-info (str "No index present after waiting to max threshold for db: " db)
                         {:status 500}))

         (do
           (Thread/sleep 100)
           (recur conn-settings ledger-name (inc retry-count))))))))
