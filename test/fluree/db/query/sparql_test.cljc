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
              ["?person" "person:handle" "?handle"]]
             where))))

  ;;TODO: not yet supported
  #_(testing "language labels"))

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
                    "person:favNums"  [23]}
                   {"id"              "ex:jbob"
                    "type"            "ex:Person"
                    "person:handle"   "jbob"
                    "person:fullName" "Jenny Bob"
                    "person:favNums"  [8 6 7 5 3 0 9]}
                   {"id"              "ex:fbueller"
                    "type"            "ex:Person"
                    "person:handle"   "dankeshön"
                    "person:fullName" "Ferris Bueller"}]
                  nil)
                 deref)]
    (testing "basic query works"
      (let [query   "SELECT ?person ?fullName
                     WHERE {?person person:handle \"jdoe\".
                            ?person person:fullName ?fullName.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["ex:jdoe" "Jane Doe"]]
               results))))
    (testing "basic query w/ OPTIONAL works"
      (let [query   "SELECT ?person ?favNums
                     WHERE {?person person:handle ?handle.
                            OPTIONAL{?person person:favNums ?favNums.}}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["ex:bbob" 23]
                ["ex:fbueller" nil]
                ["ex:jbob" 0]
                ["ex:jbob" 3]
                ["ex:jbob" 5]
                ["ex:jbob" 6]
                ["ex:jbob" 7]
                ["ex:jbob" 8]
                ["ex:jbob" 9]
                ["ex:jdoe" 3]
                ["ex:jdoe" 7]
                ["ex:jdoe" 42]
                ["ex:jdoe" 99]]
               results))))
    (testing "basic query w/ GROUP BY & OPTIONAL works"
      (let [query   "SELECT ?person ?favNums
                     WHERE {?person person:handle ?handle.
                            OPTIONAL{?person person:favNums ?favNums.}}
                     GROUP BY ?person"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["ex:bbob" [23]]
                ["ex:fbueller" nil]
                ["ex:jbob" [0 3 5 6 7 8 9]]
                ["ex:jdoe" [3 7 42 99]]]
               results))))
    (testing "basic query w/ omitted subjects works"
      (let [query   "SELECT ?person ?fullName ?favNums
                     WHERE {?person person:handle \"jdoe\";
                                    person:fullName ?fullName;
                                    person:favNums ?favNums.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["ex:jdoe" "Jane Doe" 3]
                ["ex:jdoe" "Jane Doe" 7]
                ["ex:jdoe" "Jane Doe" 42]
                ["ex:jdoe" "Jane Doe" 99]]
               results))))
    (testing "scalar fn query works"
      (let [query   "SELECT (SHA512(?handle) AS ?handleHash)
                     WHERE {?person person:handle ?handle.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d"]
                ["eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90"]
                ["696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9"]
                ["fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"]]
               results))))
    (testing "aggregate fn query works"
      (let [query   "SELECT (AVG(?favNums) AS ?avgFav)
                     WHERE {?person person:favNums ?favNums.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [[17.66666666666667]]
               results))))
    (testing "aggregate fn w/ GROUP BY query works"
      (let [query   "SELECT (AVG(?favNums) AS ?avgFav)
                     WHERE {?person person:favNums ?favNums.}
                     GROUP BY ?person"
            results @(fluree/query db query {:format :sparql})]
        (is (= [[5.428571428571429] [37.75] [23]]
               results))))
    (testing "aggregate fn w/ GROUP BY ... HAVING query works"
      (let [query   "SELECT (AVG(?favNums) AS ?avgFav)
                     WHERE {?person person:favNums ?favNums.}
                     GROUP BY ?person HAVING(AVG(?favNums) > 10)"
            results @(fluree/query db query {:format :sparql})]
        (is (= [[37.75] [23]]
               results))))
    (testing "multi-arg fn query works"
      (let [query   "SELECT (CONCAT(?handle, '-', ?fullName) AS ?hfn)
                     WHERE {?person person:handle ?handle.
                            ?person person:fullName ?fullName.}"
            results @(fluree/query db query {:format :sparql})]
        (is (= [["bbob-Billy Bob"]
                ["dankeshön-Ferris Bueller"]
                ["jbob-Jenny Bob"]
                ["jdoe-Jane Doe"]]
               results))))

    ;; TODO: Make these tests pass

    ;; These queries don't parse; issues w/ the BNF?
    #_(testing "multiple AS selections query works"
        (let [query   "SELECT (AVG(?favNums) AS ?avgFav) (CEIL(?avgFav) AS ?caf)
                       WHERE {?person person:favNums ?favNums.}"
              results @(fluree/query db query {:format :sparql})]
          (is (= [[34.8]]
                 results))))
    #_(testing "mix of bindings and variables in SELECT query works"
        (let [query   "SELECT ?favNums (AVG(?favNums) AS ?avg) ?person ?handle (MAX(?favNums) as ?max
                     WHERE  {?person person:handle ?handle.
                             ?person person:favNums ?favNums.}"
              results @(fluree/query db query {:format :sparql})]
          (is (= :hoobajoob
                 results))))
    #_(testing "fn w/ langtag string arg query works"
        (let [query   "SELECT (CONCAT(?fullName, \"'s handle is \"@en, ?handle) AS ?hfn)
                     WHERE {?person person:handle ?handle.
                            ?person person:fullName ?fullName.}"
              results @(fluree/query db query {:format :sparql})]
          (is (= [["Billy Bob's handle is bbob"]
                  ["Jane Doe's handle is jdoe"]]
                 results))))

    ;; SELECT * queries will need some kind of special handling as there isn't
    ;; an exact corollary in FQL. Will need to find all in-scope vars and turn
    ;; it all into a :select map of {?var1 ["*"], ?var2 ["*"], ...}
    #_(testing "SELECT * query works"
        (let [query   "SELECT *
                       WHERE {?person person:handle \"jdoe\".
                              ?person person:fullName ?fullName.}"
              results @(fluree/query db query {:format :sparql})]
          (is (= [["ex:jdoe" "Jane Doe"]]
                 results))))))
