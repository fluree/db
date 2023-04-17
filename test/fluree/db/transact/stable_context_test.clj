(ns fluree.db.transact.stable-context-test
  (:require [clojure.test :refer :all]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))


;; ensures default context remains stable across many commits.

(deftest ^:integration default-context-stability-memory-from-load
  (let [conn     (test-utils/create-conn)
        ledger   @(fluree/create conn "ctx/stability-mem-ld"
                                 {"defaults"
                                  {"@context"
                                   {"id"   "@id"
                                    "type" "@type"
                                    "ex"   "http://example.org/ns/"
                                    "blah" "http://blah.me/wow/ns/"}}})
        db1      @(test-utils/transact ledger [{"id"      "blah:one"
                                                "ex:name" "One"}])
        db1-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem-ld"
                                                   100))

        db2      (->> @(fluree/stage db1-load [{"id"      "blah:two"
                                                "ex:name" "Two"}])
                      (fluree/commit! ledger)
                      deref)
        db2-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem-ld"
                                                   100))

        db3      (->> @(fluree/stage db2-load [{"id"      "blah:three"
                                                "ex:name" "Three"}])
                      (fluree/commit! ledger)
                      deref)
        db3-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem-ld"
                                                   100))]

    (testing "Loaded default context is same as initial db's"
      (is (= (dbproto/-default-context db1-load)
             (dbproto/-default-context db1))))

    (testing "Second transaction default context is same as initial db's"
      (is (= (dbproto/-default-context db2-load)
             (dbproto/-default-context db1))))

    (testing "Third transaction default context is same as initial db's"
      (is (= (dbproto/-default-context db3-load)
             (dbproto/-default-context db1))))

    (testing "Query after the 3rd load is using original default context"
      (is (= [{"id"      "blah:three"
               "ex:name" "Three"}
              {"id"      "blah:two"
               "ex:name" "Two"}
              {"id"      "blah:one"
               "ex:name" "One"}]
             @(fluree/query db3-load '{"select" {?s ["*"]}
                                       "where"  [[?s "ex:name" nil]]}))))))


(deftest ^:integration default-context-stability-memory
  (let [conn     (test-utils/create-conn)
        ledger   @(fluree/create conn "ctx/stability-mem"
                                 {"defaults"
                                  {"@context"
                                   {"id"   "@id"
                                    "type" "@type"
                                    "ex"   "http://example.org/ns/"
                                    "blah" "http://blah.me/wow/ns/"}}})
        db1      @(test-utils/transact ledger [{"id"      "blah:one"
                                                "ex:name" "One"}])
        db1-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem"
                                                   100))
        db2      @(test-utils/transact ledger [{"id"      "blah:two"
                                                "ex:name" "Two"}])
        db2-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem"
                                                   100))
        db3      @(test-utils/transact ledger [{"id"      "blah:three"
                                                "ex:name" "Three"}])
        db3-load (fluree/db (test-utils/retry-load conn "ctx/stability-mem"
                                                   100))]

    (testing "Loaded default context is same as initial db's"
      (is (= (dbproto/-default-context db1-load)
             (dbproto/-default-context db1))))

    (testing "Second transaction default context is same as initial db's"
      (is (= (dbproto/-default-context db2-load)
             (dbproto/-default-context db1))))

    (testing "Third transaction default context is same as initial db's"
      (is (= (dbproto/-default-context db3-load)
             (dbproto/-default-context db1))))

    (testing "Query after the 3rd load is using original default context"
      (is (= [{"id"      "blah:three"
               "ex:name" "Three"}
              {"id"      "blah:two"
               "ex:name" "Two"}
              {"id"      "blah:one"
               "ex:name" "One"}]
             @(fluree/query db3-load '{"select" {?s ["*"]}
                                       "where"  [[?s "ex:name" nil]]}))))))


(deftest ^:integration default-context-stability-file
  (with-tmp-dir storage-path
    (let [conn     @(fluree/connect
                     {"method"       "file"
                      "storage-path" storage-path
                      "defaults"
                      {"@context" test-utils/default-context}})
          ledger   @(fluree/create conn "ctx/stability"
                                   {"defaults"
                                    {"@context"
                                     {"id"   "@id"
                                      "type" "@type"
                                      "ex"   "http://example.org/ns/"
                                      "blah" "http://blah.me/wow/ns/"}}})
          db1      @(test-utils/transact ledger [{"id"      "blah:one"
                                                  "ex:name" "One"}])
          db1-load (fluree/db (test-utils/retry-load conn "ctx/stability" 100))
          db2      @(test-utils/transact ledger [{"id"      "blah:two"
                                                  "ex:name" "Two"}])
          db2-load (fluree/db (test-utils/retry-load conn "ctx/stability" 100))
          db3      @(test-utils/transact ledger [{"id"      "blah:three"
                                                  "ex:name" "Three"}])
          db3-load (fluree/db (test-utils/retry-load conn "ctx/stability" 100))]

      (testing "Loaded default context is same as initial db's"
        (is (= (dbproto/-default-context db1-load)
               (dbproto/-default-context db1))))

      (testing "Second transaction default context is same as initial db's"
        (is (= (dbproto/-default-context db2-load)
               (dbproto/-default-context db1))))

      (testing "Third transaction default context is same as initial db's"
        (is (= (dbproto/-default-context db3-load)
               (dbproto/-default-context db1))))

      (testing "Query after the 3rd load is using original default context"
        (is (= [{"id"      "blah:three"
                 "ex:name" "Three"}
                {"id"      "blah:two"
                 "ex:name" "Two"}
                {"id"      "blah:one"
                 "ex:name" "One"}]
               @(fluree/query db3-load '{"select" {?s ["*"]}
                                         "where"  [[?s "ex:name" nil]]})))))))
