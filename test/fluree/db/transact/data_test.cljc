(ns fluree.db.transact.data-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration insert-data
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "insert-data" {:defaultContext [test-utils/default-str-context
                                                                    {"ex" "http://example.com/"}]})
        db0 (fluree/db ledger)

        invalid-tx
        {"@context" "https://flur.ee"
         "where" [["?s" "ex:name" "Vader"]]
         "insertData" {"@context" {"ex" "http://example.com/"}
                       "@id" "ex:anakin"
                       "ex:name" "Vader"
                       "ex:droid" {"@list" ["C3PO" "R2D2"]}
                       "ex:father" {"ex:name" "The Force"
                                    "ex:description" "a blank node"}
                       "ex:kid" [{"@id" "ex:luke"
                                  "ex:name" "Luke"}
                                 {"@id" "ex:leia"
                                  "ex:name" "ex:Leia"
                                  "ex:kid" {"@id" "ex:ben"
                                            "ex:name" "Ben"}}]}}

        tx
        {"@context" "https://flur.ee"
         "insertData" {"@context" {"ex" "http://not-default.com/"}
                       "@id" "ex:anakin"
                       "ex:name" "Vader"
                       "ex:droid" {"@list" ["C3PO" "R2D2"]}
                       "ex:father" {"ex:name" "The Force"
                                    "ex:description" "a blank node"}
                       "ex:kid" [{"@id" "ex:luke"
                                  "ex:name" "ex:Luke"}
                                 {"@id" "ex:leia"
                                  "ex:name" "ex:Leia"
                                  "ex:kid" {"@id" "ex:ben"
                                            "ex:name" "Ben"}}]}}

        default-context-tx
        {"@context" "https://flur.ee"
         "insertData" {"@id" "ex:anakin"
                       "ex:name" "Vader"
                       "ex:droid" {"@list" ["C3PO" "R2D2"]}
                       "ex:father" {"ex:name" "The Force"
                                    "ex:description" "a blank node"}
                       "ex:kid" [{"@id" "ex:luke"
                                  "ex:name" "ex:Luke"}
                                 {"@id" "ex:leia"
                                  "ex:name" "ex:Leia"
                                  "ex:kid" {"@id" "ex:ben"
                                            "ex:name" "Ben"}}]}}

        db1 @(fluree/stage2 db0 tx)]
    #_(is (= "Transaction must contain only insertData, deleteData, or upsertData."
             (-> @(fluree/stage2 db0 invalid-tx)
                 (Throwable->map)
                 :cause)))
    (is (= []
           @(fluree/query db1 {"where" [["?s" "ex:name" "Vader"]]
                               "select" {"?s" ["*"]}})))
    #_(is (= []
             @(fluree/stage2 db0 default-context-tx)))))
