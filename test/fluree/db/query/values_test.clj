(ns fluree.db.query.values-test
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.json-ld.api :as fluree]
             [fluree.db.test-utils :as test-utils]))

(deftest values
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "values-test")
        context ["https://flur.ee"
                 test-utils/default-str-context
                 {"ex" "http://example.com/"}]
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert" (into test-utils/people-strings
                                                   [{"@id" "ex:nikola"
                                                     "ex:cool" true}])})]
    (testing "clause"
      (testing "no where clause"
        (testing "multiple vars"
          (is (= [["foo1" "bar1"] ["foo2" "bar2"] ["foo3" "bar3"]]
                 @(fluree/query db0 {"select" ["?foo" "?bar"]
                                     "values" [["?foo" "?bar"]
                                               [["foo1" "bar1"]
                                                ["foo2" "bar2"]
                                                ["foo3" "bar3"]]]}))))
        (testing "single var"
          (is (= [["foo1"] ["foo2"] ["foo3"]]
                 @(fluree/query db0 {"select" ["?foo"]
                                     "values" ["?foo" ["foo1" "foo2" "foo3" ]]}))))))
    (testing "pattern"
      (testing "multiple vars"
        (is (= [["Cam" "cam@example.org"]]
               @(fluree/query db1 {"@context" context
                                   "select" ["?name" "?email"]
                                   "where" [{"@id" "?s" "schema:name" "?name"}
                                            {"@id" "?s" "schema:email" "?email"}
                                            ["values"
                                             ["?s" [{"@type" "xsd:anyURI" "@value" "ex:cam"}]]]]}))))
      #_(testing "multiple vars"
        (is (= [["foo1" "bar1"] ["foo2" "bar2"] ["foo3" "bar3"]]
               @(fluree/query db0 {"@context" context
                                   "select" ["?foo" "?bar"]
                                   "where" [["optional"
                                             [["values" [["?foo" "?bar"]
                                                         [["foo1" "bar1"] ["foo2" "bar2"] ["foo3" "bar3"]]]]]]]})))))))
