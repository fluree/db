(ns fluree.db.query.sparql-parser-test
  (:require
    #?@(:clj  [[clojure.test :refer :all]]
        :cljs [[cljs.test :refer-macros [deftest is testing]]])
    [fluree.db.query.sparql-parser :refer [sparql-to-ad-hoc]]))


(deftest parse-select
  (testing "basic SELECT"
    (let [query "SELECT ?person \n WHERE {\n ?person fd:person/handle \"jdoe\".\n}"
          {:keys [select]} (sparql-to-ad-hoc query)]
      (is (= ["?person"]
             select)))
    (let [query "SELECT ?person ?nums\n WHERE {\n ?person :fd:person/favNums ?nums.\n}"
          {:keys [select]} (sparql-to-ad-hoc query)]
      (is (= ["?person" "?nums"]
             select))))

  (testing "aggregates"
    (testing "AVG"
      (let [query "SELECT (AVG(?favNums) AS ?nums)\n WHERE {\n ?person fd:person/favNums ?favNums.\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["(as (avg ?favNums) ?nums)"]
               select))))
    (testing "COUNT"
      (let [query "SELECT (COUNT(?friends) AS ?friends)\n WHERE {\n ?friends fd:person/friendsWith \"jdoe\".\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["(as (count ?friends) ?friends)"]
               select))))
    (testing "COUNT DISTINCT"
      (let [query "SELECT (COUNT(DISTINCT ?handle) AS ?handles)\n WHERE {\n ?person fd:person/handle ?handle.\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["(as (count-distinct ?handle) ?handles)"]
               select))))
    (testing "MAX"
      (let [query "SELECT ?fullName (MAX(?favNums) AS ?max)\n WHERE {\n ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["?fullName" "(as (max ?favNums) ?max)"]
               select))))
    (testing "MIN"
      (let [query  "SELECT ?fullName (MIN(?favNums) AS ?min)\n WHERE {\n ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["?fullName" "(as (min ?favNums) ?min)"]
               select))))
    (testing "SAMPLE"
      (let [query  "SELECT ?fullName (SAMPLE(?favNums) AS ?sample)\n WHERE {\n ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["?fullName" "(as (sample ?favNums) ?sample)"]
               select))))
    (testing "SUM"
      (let [query  "SELECT ?fullName (SUM(?favNums) AS ?sum)\n WHERE {\n ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql-to-ad-hoc query)]
        (is (= ["?fullName" "(as (sum ?favNums) ?sum)"]
               select))))))

(deftest parse-where
  (testing "simple triple"
    (let [query "SELECT ?person \nWHERE {\n ?person fd:person/handle \"jdoe\".\n}"
          {:keys [where]} (sparql-to-ad-hoc query)]
      (is (= [["$fdb" "?person" "person/handle" "jdoe"]]
             where))))
  (testing "multi clause"
    (let [query "SELECT ?person ?nums \nWHERE {\n ?person fd:person/handle \"jdoe\".\n ?person fd:person/favNums ?nums.\n}"
          {:keys [where]} (sparql-to-ad-hoc query)]
      (is (= [["$fdb" "?person" "person/handle" "jdoe"]
              ["$fdb" "?person" "person/favNums" "?nums"]]
             where))))
  (testing "multi-clause, semicolon separator"
    (let [query "SELECT ?person ?nums\nWHERE {\n ?person fd:person/handle \"jdoe\";\n fd:person/favNums ?nums.\n}"
          {:keys [where]} (sparql-to-ad-hoc query)]
      (is (= [["$fdb" "?person" "person/handle" "jdoe"]
              ["$fdb" "?person" "person/favNums" "?nums"]]
             where))))
  (testing "multiple objectsx, comma separator"
    (let [query "SELECT ?person ?fullName ?favNums \n WHERE {\n ?person fd:person/handle \"jdoe\";\n fd:person/fullName ?fullName;\n fd:person/favNums ?favNums\n}"
          {:keys [where]} (sparql-to-ad-hoc query)]
      (is (= [["$fdb" "?person" "person/handle" "jdoe"]
              ["$fdb" "?person" "person/fullName" "?fullName"]
              ["$fdb" "?person" "person/favNums" "?favNums"]]
             where))))
  ;;TODO: not yet supported(?)
  #_(testing "language labels"))

;;TODO: not yet supported
#_(deftest parse-optional)

(deftest parse-sources
  (testing "wikidata, fluree"
    (let [query "SELECT ?movie ?title\nWHERE {\n  ?user  fdb:person/favMovies ?movie.\n ?movie fdb:movie/title ?title.\n ?wdMovie wd:?label ?title;\n wdt:P840 ?narrative_location;\n wdt:P31 wd:Q11424.\n ?user fdb:person/handle ?handle.\n \n}\n" {:keys [where]} (sparql-to-ad-hoc query)]
      (is (= ["$fdb" "$fdb" "$wd" "$wd" "$wd" "$fdb"]
             (mapv first where)))))
  (testing "external"
    (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT ?name ?mbox\n WHERE {\n ?x foaf:name ?name.\n?x foaf:mbox ?mbox\n}"
          {:keys [prefixes where]} (sparql-to-ad-hoc query)]
      (is (= {:foaf "http://xmlns.com/foaf/0.1/"}
             prefixes))
      (is (= ["foaf" "foaf"]
             (mapv first where))))))
(deftest modifiers
  (testing "LIMIT"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n LIMIT 1000"
          {:keys [limit]} (sparql-to-ad-hoc query)]
      (is (= 1000
             limit))))
  (testing "OFFSET"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n OFFSET 10"
          {:keys [offset]} (sparql-to-ad-hoc query)]
      (is (= 10
             offset))))
  (testing "ORDER BY"
    (let [query "SELECT ?favNums \n WHERE {\n ?person fd:person/favNums ?favNums\n} ORDER BY DESC(?favNums)"
          {:keys [orderBy]} (sparql-to-ad-hoc query)]
      (is (= ["DESC" "?favNums"]
             orderBy))))
  (testing "PRETTY PRINT"
    (testing "LIMIT"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n PRETTY-PRINT"
          {:keys [prettyPrint]} (sparql-to-ad-hoc query)]
      (is (= true
            prettyPrint)))))
  (testing "GROUP BY, HAVING"
    (let [query "SELECT (SUM(?favNums) AS ?sumNums)\n WHERE {\n ?e fd:person/favNums ?favNums. \n } \n GROUP BY ?e \n HAVING(SUM(?favNums) > 1000)"
          {:keys [groupBy having]} (sparql-to-ad-hoc query)]
      (is (= "?e"
             groupBy))
      (is (= "(> (sum ?favNums) 1000)"
             having))))
  ;;TODO: not yet supported
  #_(testing "DISTINCT")
  #_(testing "UNION")
  #_(testing "FILTER")
  #_(testing "BIND"))

;; TODO
#_(deftest supported-functions)

(deftest error
  (testing "invalid query throws expected error"
    (let [query "SELECT ?person\n WHERE  ?person fd:person/fullName \"jdoe\" "]
      (is (= {:status 400
              :error :db/invalid-query}
             (try
               (sparql-to-ad-hoc query)
               "should throw 400, :db/invalid-query"
               (catch #?(:clj clojure.lang.ExceptionInfo
                         :cljs :default) e (ex-data e))))))))
