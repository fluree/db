(ns fluree.db.query.sparql-parser-test
  (:require
    #?@(:clj  [[clojure.test :refer :all]]
        :cljs [[cljs.test :refer-macros [deftest is testing]]])
    [fluree.db.query.sparql-parser :refer [sparql-to-ad-hoc]]))


(deftest sparql-parser-test
  (testing "simple WHERE"
    (let [query "SELECT ?person \nWHERE {\n    ?person     fd:person/handle    \"jdoe\".\n}"]
      (is (= {:prefixes {}	  
              :select ["?person"]
              :where [["$fdb" "?person" "person/handle" "jdoe"]]}
             (sparql-to-ad-hoc query)))
      (testing "with prefix"
        (let [prefix-query (str "PREFIX pr: <http://www.wikidata.org/prop/reference/>\n" query)]
          (is (= {:prefixes {:pr "http://www.wikidata.org/prop/reference/"}
                  :select ["?person"]
                  :where [["$fdb" "?person" "person/handle" "jdoe"]]}
                 (sparql-to-ad-hoc prefix-query)))))))
  (testing "two-triple WHERE"
    (let [query "SELECT ?person ?nums\nWHERE {\n    ?person     fd:person/handle    \"jdoe\";\n                fd:person/favNums    ?nums.\n}"]
      (is (= {:prefixes {}	  
              :select ["?person" "?nums"]
              :where [["$fdb" "?person" "person/handle" "jdoe"]
                      ["$fdb" "?person" "person/favNums" "?nums"]]}
             (sparql-to-ad-hoc query)))))
  (testing "MAX"
    (let [query "SELECT ?fullName (MAX(?favNums) AS ?max)\nWHERE {\n  ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"]
      (is (= {:prefixes {}
              :select ["?fullName" "(as (max ?favNums) ?max)"]
              :where [ ["$fdb" "?person" "person/favNums" "?favNums"]
                      ["$fdb" "?person" "person/fullName" "?fullName"]]}
             (sparql-to-ad-hoc query)))))
  (testing "GROUP BY, HAVING"
    (let [query "SELECT (SUM(?favNums) AS ?sumNums)\n WHERE {\n ?e fd:person/favNums ?favNums. \n } \n GROUP BY ?e \n HAVING(SUM(?favNums) > 1000)"]
      (is (= {:prefixes {}
              :select ["(as (sum ?favNums) ?sumNums)"]
              :where [["$fdb" "?e" "person/favNums" "?favNums"]]
              :groupBy "?e"
              :having "(> (sum ?favNums) 1000)"}
             (sparql-to-ad-hoc query)))))
  (testing "multiple objects separated with commas"
    (let [query "SELECT ?person\n  WHERE {\n  ?person fd:person/handle \"jdoe\", \"zsmith\", \"dsanchez\".\n}"]
      (is (= {:prefixes {}
              :select ["?person"]
              :where [["$fdb" "?person" "person/handle" "jdoe"]
                      ["$fdb" "?person" "person/handle" "zsmith"]
                      ["$fdb" "?person" "person/handle" "dsanchez"]]}
             (sparql-to-ad-hoc query)))))
  (testing "multi-clause with semicolon"
    (let [query "SELECT ?person ?fullName ?favNums \n WHERE {\n ?person fd:person/handle \"jdoe\";\n fd:person/fullName ?fullName;\n fd:person/favNums ?favNums\n}"]
      (is (= {:prefixes {}
              :select ["?person" "?fullName" "?favNums"]
              :where [["$fdb" "?person" "person/handle" "jdoe"]
                      ["$fdb" "?person" "person/fullName" "?fullName"]
                      ["$fdb" "?person" "person/favNums" "?favNums"]]} 
             (sparql-to-ad-hoc query)))))
  (testing "invalid query throws expected error"
    (let [query "SELECT ?person\n WHERE  ?person fd:person/fullName \"jdoe\" "]
      (is (= {:status 400
              :error :db/invalid-query}
             (try
               (sparql-to-ad-hoc query)
               "should throw 400, :db/invalid-query"
               (catch #?(:clj clojure.lang.ExceptionInfo
                         :cljs :default) e (ex-data e))))))))
