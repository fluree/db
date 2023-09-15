(ns fluree.db.transact.data-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            #?(:clj  [test-with-files.tools :refer [with-tmp-dir]
                      :as twf]
               :cljs [test-with-files.tools :as-alias twf])))

#?(:clj
   (deftest ^:integration insert-data
     (with-tmp-dir storage-path {::twf/delete-dir false}
       (def storage-path storage-path)
       (let [conn @(fluree/connect {:method :file :storage-path storage-path
                                    :defaults {:indexer {:reindex-min-bytes 100}}})
             ledger @(fluree/create conn "insert-data" {:defaultContext [test-utils/default-str-context
                                                                         {"ex" "http://example.com/"}]})
             db0 (fluree/db ledger)

             tx {"@context" "https://flur.ee"
                 "insertData" [{"@id" "ex:anakin"
                                "ex:name" "Vader"
                                "ex:droid" {"@list" ["C3PO" "R2D2"]}
                                "ex:father" {"ex:name" "The Force"
                                             "ex:description" "a blank node"}
                                "ex:kid" [{"@id" "ex:luke"
                                           "ex:name" "Luke"}
                                          {"@id" "ex:leia"
                                           "ex:name" "Leia"
                                           "ex:kid" {"@id" "ex:ben"
                                                     "ex:name" "Ben"}}]}
                               {"@id" "ex:green"
                                "ex:name" "T-Rex"
                                "ex:friend" [{"@id" "ex:yellow" "ex:name" "Dromiceiomimus"}
                                             {"@id" "ex:orange" "ex:name" "Utahrapter"}]}]}

             db1s @(fluree/stage2 db0 tx)
             db1 @(fluree/commit! ledger db1s)
             ]
         (def db db1s)

         (-> db :schema :pred )

         (testing "basic insert can be queried"
           (is (= [{"id" "ex:anakin"
                    "ex:name" "Vader"
                    "ex:droid" ["C3PO" "R2D2"]
                    "ex:father" {"id" "_:211106232532993"
                                 "ex:name" "The Force"
                                 "ex:description" "a blank node"}
                    "ex:kid" [{"id" "ex:luke"
                               "ex:name" "Luke"}
                              {"id" "ex:leia"
                               "ex:name" "Leia"
                               "ex:kid" {"id" "ex:ben"
                                         "ex:name" "Ben"}}]}]
                  @(fluree/query db1s {"where" [["?s" "ex:name" "Vader"]]
                                       "select" {"?s" ["*"]}
                                       "depth" 3}))))

         (testing "inserting additional values increases cardinality"
           (let [db2 @(fluree/stage db1 {"@context" "https://flur.ee"
                                         "insertData" {"@id" "ex:anakin"
                                                       "ex:name" "Skywalker"}})]
             (is (= [["Skywalker"] ["Vader"]]
                    @(fluree/query db2 {"where" [["ex:anakin" "ex:name" "?name"]]
                                        "select" ["?name"]})))))

         (testing "non-default context is processed correctly"
           (let [db2 @(fluree/stage2 db1 {"@context" "https://flur.ee"
                                          "insertData" {"@context" {"foo" "http://not-default.com/"}
                                                        "@id" "foo:owen"
                                                        "foo:name" "Lars"}})]
             (is (= [{"foo:name" "Lars" "@id" "foo:owen"}]
                    @(fluree/query db2 {"@context" {"foo" "http://not-default.com/"}
                                        "where" [["?s" "foo:name" "Lars"]]
                                        "select" {"?s" ["*"]}})))))
         (testing "loading works correctly"
           (let [db2s @(fluree/stage2 db1 {"@context" "https://flur.ee"
                                           "insertData" {"@id" "ex:anakin"
                                                         "ex:name" "Skywalker"}})

                 db2 @(fluree/commit! ledger db2s)

                 ;; wait for index to be written
                 _ (Thread/sleep 100)

                 loaded @(fluree/load conn "insert-data")
                 dbl (fluree/db loaded)]
             (is (= [{"id" "ex:anakin"
                      "ex:name" ["Skywalker" "Vader"]
                      "ex:droid" ["C3PO" "R2D2"]
                      "ex:father" {"id" "_:211106232532993"
                                   "ex:name" "The Force"
                                   "ex:description" "a blank node"}
                      "ex:kid" [{"id" "ex:luke"
                                 "ex:name" "Luke"}
                                {"id" "ex:leia"
                                 "ex:name" "Leia"
                                 "ex:kid" {"id" "ex:ben"
                                           "ex:name" "Ben"}}]}]
                    @(fluree/query db2 {"where" [["?s" "ex:name" "Vader"]]
                                        "select" {"?s" ["*"]}
                                        "depth" 3})))
             (is (= [{"id" "ex:anakin"
                      "ex:name" ["Skywalker" "Vader"]
                      "ex:droid" ["C3PO" "R2D2"]
                      "ex:father" {"id" "_:211106232532993"
                                   "ex:name" "The Force"
                                   "ex:description" "a blank node"}
                      "ex:kid" [{"id" "ex:luke"
                                 "ex:name" "Luke"}
                                {"id" "ex:leia"
                                 "ex:name" "Leia"
                                 "ex:kid" {"id" "ex:ben"
                                           "ex:name" "Ben"}}]}]
                    @(fluree/query dbl {"where" [["?s" "ex:name" "Vader"]]
                                        "select" {"?s" ["*"]}
                                        "depth" 3})))))
         (testing "delete-data"
           (let [db2 @(fluree/stage2 db1 {"@context" "https://flur.ee"
                                          "deleteData" {"@id" "ex:anakin"
                                                        "ex:father" {"id" "_:211106232532993"
                                                                     "ex:name" "The Force"
                                                                     "ex:description" "a blank node"}}})]
             (is (= [{"id" "ex:anakin"
                      "ex:name" "Vader"
                      "ex:droid" ["C3PO" "R2D2"]
                      "ex:kid"
                      [{"id" "ex:luke" "ex:name" "Luke"}
                       {"id" "ex:leia"
                        "ex:name" "Leia"
                        "ex:kid" {"id" "ex:ben" "ex:name" "Ben"}}]}]
                    @(fluree/query db2 {"where" [["?s" "ex:name" "Vader"]]
                                        "select" {"?s" ["*"]}
                                        "depth" 2})))))
         (testing "upsert-data"
           (let [db2 @(fluree/stage2 db1 {"@context" "https://flur.ee"
                                          "upsertData" {"@id" "ex:anakin"
                                                        "ex:name" "Skywalker"}})]
             (is (= [{"id" "ex:anakin",
                      "ex:name" "Skywalker",
                      "ex:droid" ["C3PO" "R2D2"],
                      "ex:father" {"id" "_:211106232532993"},
                      "ex:kid" [{"id" "ex:luke"} {"id" "ex:leia"}]}]
                    @(fluree/query db2 {"where" [["?s" "ex:name" "Skywalker"]]
                                        "select" {"?s" ["*"]}})))))))))
