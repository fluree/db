(ns fluree.db.query.sparql-test
  (:require
   #?@(:clj  [[clojure.test :refer :all]]
       :cljs [[cljs.test :refer-macros [deftest is testing]]])
   [fluree.db.query.sparql :as sparql]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]))


(deftest parse-select
  (testing "basic SELECT"
    (let [query "SELECT ?person \n WHERE {\n ?person person:handle \"jdoe\".\n}"
          {:keys [select]} (sparql/->fql query)]
      (is (= ["?person"]
             select)))
    (let [query "SELECT ?person ?nums\n WHERE {\n ?person person:favNums ?nums.\n}"
          {:keys [select]} (sparql/->fql query)]
      (is (= ["?person" "?nums"]
             select))))
  (testing "aggregates"
    (testing "AVG"
      (let [query "SELECT (AVG(?favNums) AS ?nums)\n WHERE {\n ?person person:favNums ?favNums.\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (avg ?favNums) ?nums)"]
               select))))
    (testing "COUNT"
      (let [query "SELECT (COUNT(?friends) AS ?friends)\n WHERE {\n ?friends person:friendsWith \"jdoe\".\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (count ?friends) ?friends)"]
               select))))
    (testing "COUNT DISTINCT"
      (let [query "SELECT (COUNT(DISTINCT ?handle) AS ?handles)\n WHERE {\n ?person person:handle ?handle.\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (count-distinct ?handle) ?handles)"]
               select))))
    (testing "MAX"
      (let [query "SELECT ?fullName (MAX(?favNums) AS ?max)\n WHERE {\n ?person person:favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (max ?favNums) ?max)"]
               select))))
    (testing "MIN"
      (let [query "SELECT ?fullName (MIN(?favNums) AS ?min)\n WHERE {\n ?person person:favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (min ?favNums) ?min)"]
               select))))
    (testing "SAMPLE"
      (let [query "SELECT ?fullName (SAMPLE(?favNums) AS ?sample)\n WHERE {\n ?person person:favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (sample ?favNums) ?sample)"]
               select))))
    (testing "SUM"
      (let [query "SELECT ?fullName (SUM(?favNums) AS ?sum)\n WHERE {\n ?person person:favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (sum ?favNums) ?sum)"]
               select))))
    ;;TODO: not yet supported
    #_(testing "GROUP_CONCAT")))

(deftest parse-where
  (testing "simple triple"
    (let [query "SELECT ?person \nWHERE {\n ?person person:handle \"jdoe\".\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person/handle" "jdoe"]]
             where))))
  (testing "multi clause"
    (let [query "SELECT ?person ?nums \nWHERE {\n ?person person:handle \"jdoe\".\n ?person fd:person/favNums ?nums.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person/handle" "jdoe"]
              ["?person" "person/favNums" "?nums"]]
             where))))
  (testing "multi-clause, semicolon separator"
    (let [query "SELECT ?person ?nums\nWHERE {\n ?person person:handle \"jdoe\";\n fd:person/favNums ?nums.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person/handle" "jdoe"]
              ["?person" "person/favNums" "?nums"]]
             where))))
  (testing "multiple objects, comma separator"
    (let [query "SELECT ?person ?fullName ?favNums \n WHERE {\n ?person person:handle \"jdoe\";\n fd:person/fullName ?fullName;\n fd:person/favNums ?favNums\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person/handle" "jdoe"]
              ["?person" "person/fullName" "?fullName"]
              ["?person" "person/favNums" "?favNums"]]
             where))))
  (testing "UNION"
    (let [query "SELECT ?person ?age\nWHERE {\n { ?person person:age 70.\n ?person person:handle \"dsanchez\". } \n  UNION \n { ?person person:handle \"anguyen\". } \n ?person person:age ?age.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{:union
               [[["?person" "person:age" 70]
                 ["?person" "person:handle" "dsanchez"]]
                [["?person" "person:handle" "anguyen"]]]}
              ["?person" "person:age" "?age"]]
             where))))
  (testing "FILTER"
    (let [query "SELECT ?handle ?num\nWHERE {\n ?person person:handle ?handle.\n ?person person:favNums ?num.\n  FILTER ( ?num > 10 ).\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person:handle" "?handle"]
              ["?person" "person:favNums" "?num"]
              {:filter ["(> ?num 10)"]}]
             where))))
  (testing "OPTIONAL"
    (let [query "SELECT ?handle ?num\nWHERE {\n ?person person:handle ?handle.\n OPTIONAL { ?person person:favNums ?num. }\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?person" "person:handle" "?handle"]
              {:optional [["?person" "person:favNums" "?num"]]}]
             where)))
    (testing "multi-clause"
      (let [query "SELECT ?person ?name ?handle ?favNums \nWHERE {\n  ?person person:fullName ?name. \n  OPTIONAL { ?person person:handle ?handle. \n ?person person:favNums ?favNums. }\n}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [["?person" "person:fullName" "?name"]
                {:optional
                 [["?person" "person:handle" "?handle"]
                  ["?person" "person:favNums" "?favNums"]]}]
               where))))
    (testing "OPTIONAL + FILTER"
      (let [query "SELECT ?handle ?num\nWHERE {\n  ?person person:handle ?handle.\n  OPTIONAL { ?person person:favNums ?num. \n FILTER( ?num > 10 )\n }\n}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [["?person" "person:handle" "?handle"]
                {:optional
                 [["?person" "person:favNums" "?num"]
                  {:filter ["(> ?num 10)"]}]}]
               where)))))
  (testing "VALUES"
    (let [query "SELECT ?handle\nWHERE {\n VALUES ?handle { \"dsanchez\" }\n ?person person:handle ?handle.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{:bind {"?handle" "dsanchez"}}
              ["?person" "person:handle" "?handle"]]
             where))))
  (testing "BIND"
    (let [query "SELECT ?person ?handle\nWHERE {\n BIND (\"dsanchez\" AS ?handle)\n  ?person person:handle ?handle.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{:bind {"?handle" "dsanchez"}}
              ["?person" "person/handle" "?handle"]]
             where)))
    (let [query "SELECT ?hash\nWHERE {\n  ?s fdb:_block/number ?bNum.\n  BIND (MAX(?bNum) AS ?maxBlock)\n  ?s fdb:_block/number ?maxBlock.\n  ?s fdb:_block/hash ?hash.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["?s" "_block/number" "?bNum"]
              {:bind {"?maxBlock" "#(max ?bNum)"}}
              ["?s" "_block/number" "?maxBlock"]
              ["?s" "_block/hash" "?hash"]]
             where))))

  ;;TODO: not yet supported
  #_(testing "language labels"))


;; TODO: Probably delete; likely no longer relevant in v3
#_(deftest parse-sources
    (testing "wikidata, current fluree"
      (let [query "SELECT ?movie ?title\nWHERE {\n  ?user  fdb:person/favMovies ?movie.\n ?movie fdb:movie/title ?title.\n ?wdMovie wd:?label ?title;\n wdt:P840 ?narrative_location;\n wdt:P31 wd:Q11424.\n ?user fdb:person/handle ?handle.\n \n}\n" {:keys [where]} (sparql/->fql query)]
        (is (= ["$fdb" "$fdb" "$wd" "$wd" "$wd" "$fdb"]
               (mapv first where)))))
    (testing "fullText"
      (let [query "SELECT ?person\nWHERE {\n  ?person fullText:person/handle \"jdoe\".\n}"
            {:keys [where]} (sparql/->fql query)]
        (is (= ["$fdb"]
               (mapv first where)))))
    (testing "fluree blocks"
      (let [query "SELECT ?nums\nWHERE {\n ?person fd4:person/handle \"zsmith\";\n fd4:person/favNums ?nums;\n fd5:person/favNums  ?nums.\n}"
            {:keys [where]} (sparql/->fql query)]
        (is (= ["$fdb4" "$fdb4" "$fdb5"]
               (mapv first where)))))
    (testing "external"
      (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT ?name ?mbox\n WHERE {\n ?x foaf:name ?name.\n?x foaf:mbox ?mbox\n}"
            {:keys [prefixes where]} (sparql/->fql query)]
        (is (= {:foaf "http://xmlns.com/foaf/0.1/"}
               prefixes))
        (is (= ["foaf" "foaf"]
               (mapv first where))))))

(deftest parse-modifiers
  (testing "LIMIT"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n LIMIT 1000"
          {:keys [limit]} (sparql/->fql query)]
      (is (= 1000
             limit))))
  (testing "OFFSET"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n OFFSET 10"
          {:keys [offset]} (sparql/->fql query)]
      (is (= 10
             offset))))
  (testing "ORDER BY"
    (testing "ASC"
      (let [query "SELECT ?favNums \n WHERE {\n ?person fd:person/favNums ?favNums\n} ORDER BY ASC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= ["ASC" "?favNums"]
               orderBy))))
    (testing "DESC"
      (let [query "SELECT ?favNums \n WHERE {\n ?person fd:person/favNums ?favNums\n} ORDER BY DESC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= ["DESC" "?favNums"]
               orderBy)))))
  (testing "PRETTY-PRINT"
    (let [query "SELECT ?person\n WHERE {\n ?person fd:person/fullName ?fullName\n}\n PRETTY-PRINT"
          {:keys [prettyPrint]} (sparql/->fql query)]
      (is (= true
             prettyPrint))))
  (testing "GROUP BY, HAVING"
    (let [query "SELECT (SUM(?favNums) AS ?sumNums)\n WHERE {\n ?e fd:person/favNums ?favNums. \n } \n GROUP BY ?e \n HAVING(SUM(?favNums) > 1000)"
          {:keys [groupBy having]} (sparql/->fql query)]
      (is (= "?e"
             groupBy))
      (is (= "(> (sum ?favNums) 1000)"
             having))))
  (testing "mutiple GROUP BY"
    (let [query "SELECT ?handle\nWHERE {\n ?person fdb:person/handle ?handle.\n}\nGROUP BY ?person ?handle"
          {:keys [groupBy]} (sparql/->fql query)]
      (is (= ["?person" "?handle"]
             groupBy))))
  (testing "DISTINCT"
    (let [query "SELECT DISTINCT ?person ?fullName \nWHERE {\n ?person fd:person/fullName ?fullName \n}"
          {:keys [selectDistinct]} (sparql/->fql query)]
      (is (= ["?person" "?fullName"]
             selectDistinct)))))

(deftest parse-recursive
  (let [query "SELECT ?followHandle\nWHERE {\n ?person fdb:person/handle \"anguyen\".\n ?person fdb:person/follows+ ?follows.\n ?follows fdb:person/handle ?followHandle.\n}"
        {:keys [where]} (sparql/->fql query)]
    (is (= [["$fdb" "?person" "person/handle" "anguyen"]
            ["$fdb" "?person" "person/follows+" "?follows"]
            ["$fdb" "?follows" "person/handle" "?followHandle"]]
           where)))
  (testing "depth"
    (let [query "SELECT ?followHandle\nWHERE {\n ?person fdb:person/handle \"anguyen\".\n ?person fdb:person/follows+3 ?follows.\n ?follows fdb:person/handle ?followHandle.\n}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [["$fdb" "?person" "person/handle" "anguyen"]
              ["$fdb" "?person" "person/follows+3" "?follows"]
              ["$fdb" "?follows" "person/handle" "?followHandle"]]
             where)))))

;; TODO
#_(deftest parse-functions)

(deftest parsing-error
  (testing "invalid query throws expected error"
    (let [query "SELECT ?person\n WHERE  ?person fd:person/fullName \"jdoe\" "]
      (is (= {:status 400
              :error  :db/invalid-query}
             (try
               (sparql/->fql query)
               "should throw 400, :db/invalid-query"
               (catch #?(:clj  clojure.lang.ExceptionInfo
                         :cljs :default) e (ex-data e))))))))

(deftest ^:integration query-test
  (let [conn @(fluree/connect {:method :memory
                               :defaults
                               {:context      test-utils/default-str-context
                                :context-type :string}})
        db   (-> conn
                 (fluree/create "people"
                                {:defaultContext
                                 ["" {"person" "http://example.org/Person#"}]})
                 deref
                 (fluree/transact!
                  [{"id"              "ex:jdoe"
                    "type"            "ex:Person"
                    "person:handle"   "jdoe"
                    "person:fullName" "Jane Doe"
                    "person:favNums"  [3 7 42 99]}
                   {"id"              "ex:bbob"
                    "type"            "ex:Person"
                    "person:handle"   "bbob"
                    "person:fullName" "Billy Bob"
                    "person:favNums"  [23]}]
                  nil)
                 deref)]
    (testing "basic query works"
      (let [query   "SELECT ?person ?fullName
                     WHERE {?person person:handle \"jdoe\".
                            ?person person:fullName ?fullName.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["ex:jdoe" "Jane Doe"]]
               results))))
    (testing "scalar fn query works"
      (let [query   "SELECT (SHA512(?handle) AS ?handleHash)
                     WHERE {?person person:handle ?handle.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d"]
                ["fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"]]
               results))))
    (testing "aggregate fn query works"
      (let [query   "SELECT (AVG(?favNums) AS ?nums)
                     WHERE {?person person:favNums ?favNums.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [[34.8]]
               results))))
    (testing "multi-arg fn query works"
      (let [query   "SELECT (CONCAT(?handle, '-', ?fullName) AS ?hfn)
                     WHERE {?person person:handle ?handle.
                            ?person person:fullName ?fullName.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["bbob-Billy Bob"]
                ["jdoe-Jane Doe"]]
               results))))))
