(ns fluree.db.flake.index.novelty-test
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration index-datetimes-test
  (testing "Serialize and reread flakes with time types"
    (with-temp-dir [storage-path {}]
      (let [conn    @(fluree/connect-file {:storage-path (str storage-path)
                                           :defaults
                                           {:indexing {:reindex-min-bytes 12
                                                       :reindex-max-bytes 10000000}}})
            context (merge test-utils/default-str-context {"ex" "http://example.org/ns/"})
            ledger  @(fluree/create conn "index/datetimes")
            db      @(fluree/update
                      (fluree/db ledger)
                      {"@context" context
                       "insert"
                       [{"@id"   "ex:Foo",
                         "@type" "ex:Bar",

                         "ex:offsetDateTime"  {"@type"  "xsd:dateTime"
                                               "@value" "2023-04-01T00:00:00.000Z"}
                         "ex:localDateTime"   {"@type"  "xsd:dateTime"
                                               "@value" "2021-09-24T11:14:32.833"}
                         "ex:offsetDateTime2" {"@type"  "xsd:date"
                                               "@value" "2022-01-05Z"}
                         "ex:localDate"       {"@type"  "xsd:date"
                                               "@value" "2024-02-02"}
                         "ex:offsetTime"      {"@type"  "xsd:time"
                                               "@value" "12:42:00Z"}
                         "ex:localTime"       {"@type"  "xsd:time"
                                               "@value" "12:42:00"}}]})
            _db-commit @(fluree/commit! ledger db)
            loaded     (test-utils/retry-load conn (:alias ledger) 100)
            q          {"@context" context
                        "select"   {"?s" ["*"]}
                        "where"    {"@id" "?s", "type" "ex:Bar"}}]
        (is (= @(fluree/query (fluree/db loaded) q)
               @(fluree/query db q)))))))
