(ns fluree.db.indexer.default-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            [jsonista.core :as json]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration index-datetimes-test
  (testing "Serialize and reread flakes with time types"
    (with-tmp-dir storage-path
      (let [conn @(fluree/connect {:method :file
                                   :storage-path storage-path
                                   :defaults
                                   { :indexer {:reindex-min-bytes 12
                                               :reindex-max-bytes 10000000}
                                    :context (merge test-utils/default-str-context {"ex" "http://example.org/ns/"})}})
            ledger @(fluree/create conn "index/datetimes")
            db @(fluree/stage
                  (fluree/db ledger)
                  [{"@id" "ex:Foo",
                    "@type" "ex:Bar",
                    "ex:createdDate" {"@type" "xsd:dateTime"
                                      "@value" "2023-04-01T00:00:00.000Z"}}])
            db-commit @(fluree/commit! ledger db)
            loaded (test-utils/retry-load conn (:alias ledger) 100)
            q {"select" {"?s" ["*"]}
               "where" [["?s" "type" "ex:Bar"]]}]
        (is (= @(fluree/query (fluree/db loaded) q)
               @(fluree/query db q)))))))
