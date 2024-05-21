(ns fluree.db.query.sparql-test
  (:require
   #?@(:clj  [[clojure.test :refer :all]]
       :cljs [[cljs.test :refer-macros [deftest is testing async]]
              [clojure.core.async :refer [go <!]]
              [clojure.core.async.interop :refer [<p!]]])
   [fluree.db.query.sparql :as sparql]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree])
  #?(:clj (:import (clojure.lang ExceptionInfo))))

(deftest parse-select
  (testing "basic SELECT"
    (let [query "SELECT ?person
                 WHERE {?person person:handle \"jdoe\".}"
          {:keys [select]} (sparql/->fql query)]
      (is (= ["?person"]
             select)))
    (let [query "SELECT ?person ?nums
                 WHERE {?person person:favNums ?nums.}"
          {:keys [select]} (sparql/->fql query)]
      (is (= ["?person" "?nums"]
             select))))
  (testing "wildcard"
    (let [query "SELECT *
                 WHERE {?person person:handle ?handle;
                                person:favNums ?favNums.}"
          {:keys [select]} (sparql/->fql query)]
      (is (= ["*"] select))))
  (testing "aggregates"
    (testing "AVG"
      (let [query "SELECT (AVG(?favNums) AS ?nums)
                   WHERE {?person person:favNums ?favNums.}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (avg ?favNums) ?nums)"]
               select))))
    (testing "COUNT"
      (let [query "SELECT (COUNT(?friends) AS ?friends)
                   WHERE {?friends person:friendsWith \"jdoe\".}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (count ?friends) ?friends)"]
               select))))
    (testing "COUNT DISTINCT"
      (let [query "SELECT (COUNT(DISTINCT ?handle) AS ?handles)
                   WHERE {?person person:handle ?handle.}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["(as (count-distinct ?handle) ?handles)"]
               select))))
    (testing "MAX"
      (let [query "SELECT ?fullName (MAX(?favNums) AS ?max)
                   WHERE {?person person:favNums ?favNums.
                          ?person person:fullName ?fullName}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (max ?favNums) ?max)"]
               select))))
    (testing "MIN"
      (let [query "SELECT ?fullName (MIN(?favNums) AS ?min)
                   WHERE {?person person:favNums ?favNums.
                          ?person person:fullName ?fullName}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (min ?favNums) ?min)"]
               select))))
    (testing "SAMPLE"
      (let [query "SELECT ?fullName (SAMPLE(?favNums) AS ?sample)
                   WHERE {?person person:favNums ?favNums.
                          ?person person:fullName ?fullName}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (sample1 ?favNums) ?sample)"]
               select))))
    (testing "SUM"
      (let [query "SELECT ?fullName (SUM(?favNums) AS ?sum)
                   WHERE {?person person:favNums ?favNums.
                          ?person person:fullName ?fullName}"
            {:keys [select]} (sparql/->fql query)]
        (is (= ["?fullName" "(as (sum ?favNums) ?sum)"]
               select))))
    ;;TODO: not yet supported
    #_(testing "GROUP_CONCAT")))

(deftest parse-where
  (testing "simple triple"
    (let [query "SELECT ?person
                 WHERE {?person person:handle \"jdoe\".}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id"           "?person"
               "person:handle" "jdoe"}]
             where))))
  (testing "multi clause"
    (let [query "SELECT ?person ?nums
                 WHERE {?person person:handle \"jdoe\".
                        ?person person:favNums ?nums.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id"           "?person"
               "person:handle" "jdoe"}
              {"@id"            "?person"
               "person:favNums" "?nums"}]
             where))))
  (testing "multi-clause, semicolon separator"
    (let [query "SELECT ?person ?nums
                 WHERE {?person person:handle \"jdoe\";
                                person:favNums ?nums.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id"           "?person"
               "person:handle" "jdoe"}
              {"@id"            "?person"
               "person:favNums" "?nums"}]
             where))))
  (testing "multiple objects, semicolon separator"
    (let [query "SELECT ?person ?fullName ?favNums
                 WHERE {?person person:handle \"jdoe\";
                                person:fullName ?fullName;
                                person:favNums ?favNums}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id"           "?person"
               "person:handle" "jdoe"}
              {"@id"             "?person"
               "person:fullName" "?fullName"}
              {"@id"            "?person"
               "person:favNums" "?favNums"}]
             where))))
  (testing "UNION"
    (let [query "SELECT ?person ?age
                 WHERE {{?person person:age 70.
                         ?person person:handle \"dsanchez\".}
                        UNION {?person person:handle \"anguyen\".}
                        ?person person:age ?age.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:union
               {"@id" "?person", "person:age" 70}
               {"@id" "?person", "person:handle" "dsanchez"}
               {"@id" "?person", "person:handle" "anguyen"}]
              {"@id" "?person", "person:age" "?age"}]
             where))))
  (testing "FILTER"
    (let [query "SELECT ?handle ?num
                 WHERE {?person person:handle ?handle.
                        ?person person:favNums ?num.
                        FILTER ( ?num > 10 ).}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:handle" "?handle"}
              {"@id" "?person", "person:favNums" "?num"}
              [:filter ["(> ?num 10)"]]]
             where)))
    (let [query "PREFIX psm: <http://srv.ktbl.de/data/psm/>
                 PREFIX schema: <http://schema.org/>
                 SELECT ?s ?t ?name
                 FROM <cookbook/base>
                 WHERE {
                   ?s <@type> ?t.
                   ?s schema:name ?name.
                   FILTER REGEX(?name, \"^Jon\", \"i\")
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s", "@type" "?t"}
              {"@id" "?s", "schema:name" "?name"}
              [:filter ["(regex ?name \"^Jon\" \"i\")"]]]
             where))))
  (testing "OPTIONAL"
    (let [query "SELECT ?handle ?num
                 WHERE {?person person:handle ?handle.
                        OPTIONAL {?person person:favNums ?num.}}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:handle" "?handle"}
              [:optional [{"@id" "?person", "person:favNums" "?num"}]]]
             where)))
    (testing "multi-clause"
      (let [query "SELECT ?person ?name ?handle ?favNums
                   WHERE {?person person:fullName ?name.
                          OPTIONAL {?person person:handle ?handle.
                                    ?person person:favNums ?favNums.}}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [{"@id" "?person", "person:fullName" "?name"}
                [:optional
                 [{"@id" "?person", "person:handle" "?handle"}
                  {"@id" "?person", "person:favNums" "?favNums"}]]]
               where))))
    (testing "OPTIONAL + FILTER"
      (let [query "SELECT ?handle ?num
                   WHERE {?person person:handle ?handle.
                          OPTIONAL {?person person:favNums ?num.
                                    FILTER( ?num > 10 )}}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [{"@id" "?person", "person:handle" "?handle"}
                [:optional
                 [{"@id" "?person", "person:favNums" "?num"}
                  [:filter ["(> ?num 10)"]]]]]
               where)))))
  (testing "VALUES"
    (testing "pattern"
      (let [query "SELECT ?handle
                 WHERE {VALUES ?handle { \"dsanchez\" }
                        ?person person:handle ?handle.}"]
        (is (= [[:values ["?handle" ["dsanchez"]]]
                {"@id" "?person", "person:handle" "?handle"}]
               (:where (sparql/->fql query)))
            "where pattern: single var, single val"))
      (let [query "SELECT ?handle
                 WHERE {VALUES ?person { :personA :personB }
                        ?person person:handle ?handle.}"]
        (is (= [[:values
                 ["?person"
                  [{"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                    "@value" ":personA"}
                   {"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                    "@value" ":personB"}]]]
                {"@id" "?person", "person:handle" "?handle"}]
               (:where (sparql/->fql query)))
            "where pattern: single var, multiple values"))
      (let [query "SELECT * WHERE {
                     VALUES (?color ?direction) {
                     ( dm:red  \"north\" )
                     ( dm:blue  \"west\" )
                   }}"]
        (is (= [[:values
                 [["?color" "?direction"]]
                 [[{"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                    "@value" "dm:red"}
                   "north"]
                  [{"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                    "@value" "dm:blue"}
                   "west"]]]]
               (:where (sparql/->fql query)))
            "multiple vars, multiple values")))
    (testing "clause"
      (let [query "SELECT ?handle
                   WHERE { ?person person:handle ?handle.}
                   VALUES ?person { :personA :personB }"]
        (is (= {:where [{"@id" "?person", "person:handle" "?handle"}],
                :values
                ["?person"
                 [{"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                   "@value" ":personA"}
                  {"@type" "http://www.w3.org/2001/XMLSchema#anyURI",
                   "@value" ":personB"}]]}
               (select-keys (sparql/->fql query) [:where :values]))
            "where pattern: single var, multiple values"))))
  (testing "BIND"
    (let [query "SELECT ?person ?handle
                 WHERE {BIND (\"dsanchez\" AS ?handle)
                        ?person person:handle ?handle.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:bind "?handle" "dsanchez"]
              {"@id" "?person", "person:handle" "?handle"}]
             where)))
    (let [query "SELECT ?person ?prefix ?foofix ?num1
                 WHERE {BIND (SUBSTR(?handle, 4) AS ?prefix)
                        BIND (REPLACE(?prefix, \"abc\", \"FOO\") AS ?foofix)
                        BIND (?age*4*3/-2*(-4/2) AS ?num1)
                        ?person person:handle ?handle.
                        ?person person:age ?age}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:bind "?prefix" "(subStr ?handle 4)"]
              [:bind "?foofix" "(replace ?prefix \"abc\" \"FOO\")"]
              [:bind "?num1" "(* (/ (* (* ?age 4) 3) -2) (/ -4 2))"]
              {"@id" "?person", "person:handle" "?handle"}
              {"@id" "?person", "person:age" "?age"}]
             where)))
    (let [query "SELECT ?person ?abs ?bnode ?bound ?ceil ?coalesce ?concat ?contains ?datatype ?day ?encodeForUri ?floor ?hours ?if ?iri ?lang ?langMatches ?lcase ?md5 ?minutes ?month ?now ?rand ?round ?seconds ?sha1 ?sha256 ?sha512 ?str ?strAfter ?strBefore ?strDt ?strEnds
                 WHERE {BIND (ABS(1*4*3/-2*(-4/2)) AS ?abs)
                        BIND (BNODE(?foobar) AS ?bnode)
                        BIND (BOUND(?abs) AS ?bound)
                        BIND (CEIL(1.8) AS ?ceil)
                        BIND (COALESCE(?num1, 2) AS ?coalesce)
                        BIND (CONCAT(\"foo\", \"bar\") AS ?concat)
                        BIND (CONTAINS(\"foobar\", \"foo\") AS ?contains)
                        BIND (DATATYPE(\"foobar\") AS ?datatype)
                        BIND (DAY(\"2024-4-1T14:45:13.815-05:00\") AS ?day)
                        BIND (ENCODE_FOR_URI(\"Los Angeles\") AS ?encodeForUri)
                        BIND (FLOOR(1.8) AS ?floor)
                        BIND (HOURS(\"2024-4-1T14:45:13.815-05:00\") AS ?hours)
                        BIND (IF(\"true\", \"yes\", \"no\") AS ?if)
                        BIND (IRI(\"http://example.com\") AS ?iri)
                        BIND (LANG(\"Robert\"\"@en\") AS ?lang)
                        BIND (LANGMATCHES(?lang, \"FR\") AS ?langMatches)
                        BIND (LCASE(\"FOO\") AS ?lcase)
                        BIND (MD5(\"abc\") AS ?md5)
                        BIND (MINUTES(\"2024-4-1T14:45:13.815-05:00\") AS ?minutes)
                        BIND (MONTH(\"2024-4-1T14:45:13.815-05:00\") AS ?month)
                        BIND (NOW() AS ?now)
                        BIND (RAND() AS ?rand)
                        BIND (ROUND(1.8) AS ?round)
                        BIND (SECONDS(\"2024-4-1T14:45:13.815-05:00\") AS ?seconds)
                        BIND (SHA1(\"abc\") AS ?sha1)
                        BIND (SHA256(\"abc\") AS ?sha256)
                        BIND (SHA512(\"abc\") AS ?sha512)
                        BIND (STR(\"foobar\") AS ?str)
                        BIND (STRAFTER(\"abc\", \"b\") AS ?strAfter)
                        BIND (STRBEFORE(\"abc\", \"b\") AS ?strBefore)
                        BIND (STRDT(\"iiii\", \"http://example.com/romanNumeral\") AS ?strDt)
                        BIND (STRENDS(\"foobar\", \"bar\") AS ?strEnds)
                        ?person person:age ?age.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:bind "?abs" "(abs \"(* (/ (* (* 1 4) 3) -2) (/ -4 2))\")"]
              [:bind "?bnode" "(bnode ?foobar)"]
              [:bind "?bound" "(bound ?abs)"]
              [:bind "?ceil" "(ceil \"1.8\")"]
              [:bind "?coalesce" "(coalesce ?num1 \"2\")"]
              [:bind "?concat" "(concat \"foo\" \"bar\")"]
              [:bind "?contains" "(contains \"foobar\" \"foo\")"]
              [:bind "?datatype" "(datatype \"foobar\")"]
              [:bind "?day" "(day \"2024-4-1T14:45:13.815-05:00\")"]
              [:bind "?encodeForUri" "(encodeForUri \"Los Angeles\")"]
              [:bind "?floor" "(floor \"1.8\")"]
              [:bind "?hours" "(hours \"2024-4-1T14:45:13.815-05:00\")"]
              [:bind "?if" "(if \"true\" \"yes\" \"no\")"]
              [:bind "?iri" "(iri \"http://example.com\")"]
              [:bind "?lang" "(lang \"Robert\"\"@en\")"]
              [:bind "?langMatches" "(langMatches ?lang \"FR\")"]
              [:bind "?lcase" "(lcase \"FOO\")"]
              [:bind "?md5" "(md5 \"abc\")"]
              [:bind "?minutes" "(minutes \"2024-4-1T14:45:13.815-05:00\")"]
              [:bind "?month" "(month \"2024-4-1T14:45:13.815-05:00\")"]
              [:bind "?now" "(now)"]
              [:bind "?rand" "(rand)"]
              [:bind "?round" "(round \"1.8\")"]
              [:bind "?seconds" "(seconds \"2024-4-1T14:45:13.815-05:00\")"]
              [:bind "?sha1" "(sha1 \"abc\")"]
              [:bind "?sha256" "(sha256 \"abc\")"]
              [:bind "?sha512" "(sha512 \"abc\")"]
              [:bind "?str" "(str \"foobar\")"]
              [:bind "?strAfter" "(strAfter \"abc\" \"b\")"]
              [:bind "?strBefore" "(strBefore \"abc\" \"b\")"]
              [:bind "?strDt" "(strDt \"iiii\" \"http://example.com/romanNumeral\")"]
              [:bind "?strEnds" "(strEnds \"foobar\" \"bar\")"]
              {"@id" "?person", "person:age" "?age"}]
             where)))))

;;TODO: not yet supported
#_(testing "language labels")

(deftest parse-prefixes
  (testing "PREFIX"
    (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                 SELECT ?name ?mbox
                 WHERE {?x foaf:name ?name.
                        ?x foaf:mbox ?mbox}"
          {:keys [context where]} (sparql/->fql query)]
      (is (= {"foaf" "http://xmlns.com/foaf/0.1/"}
             context))
      (is (= [{"@id" "?x", "foaf:name" "?name"}
              {"@id" "?x", "foaf:mbox" "?mbox"}]
             where))))
  (testing "multiple PREFIXes"
    (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                 PREFIX ex: <http://example.org/ns/>
                 SELECT ?name ?mbox
                 WHERE {?x foaf:name ?name.
                        ?x foaf:mbox ?mbox}"
          {:keys [context]} (sparql/->fql query)]
      (is (= {"foaf" "http://xmlns.com/foaf/0.1/"
              "ex"   "http://example.org/ns/"}
             context)))))

(deftest parse-modifiers
  (testing "LIMIT"
    (let [query "SELECT ?person
                 WHERE {?person person:fullName ?fullName} LIMIT 1000"
          {:keys [limit]} (sparql/->fql query)]
      (is (= 1000
             limit))))
  (testing "OFFSET"
    (let [query "SELECT ?person
                 WHERE {?person person:fullName ?fullName} OFFSET 10"
          {:keys [offset]} (sparql/->fql query)]
      (is (= 10
             offset))))
  (testing "ORDER BY"
    (testing "ASC"
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY ASC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= [["asc" "?favNums"]]
               orderBy))))
    (testing "DESC"
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY DESC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= [["desc" "?favNums"]]
               orderBy)))))
  (testing "PRETTY-PRINT"
    (let [query "SELECT ?person
                 WHERE {?person person:fullName ?fullName}
                 PRETTY-PRINT"
          {:keys [prettyPrint]} (sparql/->fql query)]
      (is (= true
             prettyPrint))))
  (testing "GROUP BY, HAVING"
    (let [query "SELECT (SUM(?favNums) AS ?sumNums)
                 WHERE {?e person:favNums ?favNums.}
                 GROUP BY ?e HAVING(SUM(?favNums) > 1000)"
          {:keys [groupBy having]} (sparql/->fql query)]
      (is (= ["?e"]
             groupBy))
      (is (= "(> (sum ?favNums) 1000)"
             having))))
  (testing "mutiple GROUP BY"
    (let [query "SELECT ?handle
                 WHERE {?person person:handle ?handle.}
                 GROUP BY ?person ?handle"
          {:keys [groupBy]} (sparql/->fql query)]
      (is (= ["?person" "?handle"]
             groupBy))))
  (testing "DISTINCT"
    (let [query "SELECT DISTINCT ?person ?fullName
                 WHERE {?person person:fullName ?fullName}"
          {:keys [selectDistinct]} (sparql/->fql query)]
      (is (= ["?person" "?fullName"]
             selectDistinct)))))

;; TODO: these expectations do not work in FQL
#_(deftest parse-recursive
  (let [query "SELECT ?followHandle
               WHERE {?person person:handle \"anguyen\".
                      ?person person:follows+ ?follows.
                      ?follows person:handle ?followHandle.}"
        {:keys [where]} (sparql/->fql query)]
    (is (= [{"@id" "?person", "person:handle" "anguyen"}
            {"@id" "?person", "person:follows+" "?follows"}
            {"@id" "?follows", "person:handle" "?followHandle"}]
           where)))
  (testing "depth"
    (let [query "SELECT ?followHandle
                 WHERE {?person person:handle \"anguyen\".
                        ?person person:follows+3 ?follows.
                        ?follows person:handle ?followHandle.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:handle" "anguyen"}
              {"@id" "?person", "person:follows+3" "?follows"}
              {"@id" "?follows", "person:handle" "?followHandle"}]
             where)))))

;; TODO
#_(deftest parse-functions)

(deftest subject-iri
  (let [query
        "PREFIX psm: <http://srv.ktbl.de/data/psm/>
PREFIX ex: <http://example.org/>

SELECT ?p ?o
FROM <cookbook/base>
WHERE {
    ex:andrew ?p ?o. # Should provide just the triples related to andrew
}"
        fql (sparql/->fql query)]
    (is (= {:context {"psm" "http://srv.ktbl.de/data/psm/", "ex" "http://example.org/"},
            :select ["?p" "?o"],
            :from "cookbook/base",
            :where
            [{"@id" "ex:andrew" "?p" "?o"}]}
           fql))))

(deftest parsing-error
  (testing "invalid query throws expected error"
    (let [query "SELECT ?person
                 WHERE  ?person person:fullName \"jdoe\""]
      (is (= {:status 400
              :error  :db/invalid-query}
             (try
               (sparql/->fql query)
               "should throw 400, :db/invalid-query"
               (catch #?(:clj  clojure.lang.ExceptionInfo
                         :cljs :default) e (ex-data e))))))))

(deftest ^:integration query-test
  (let [people-data [{"id"              "ex:jdoe"
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
                      "person:fullName" "Ferris Bueller"}]]
    #?(#_#_:cljs
       (async done
         (go
           (let [conn   (<! (test-utils/create-conn))
                ledger (<p! (fluree/create conn "people"))
                db     (<p! (fluree/stage (fluree/db ledger) {"@context" "https://ns.flur.ee"
                                                               "insert" people-data}))]
            (testing "basic query works"
              (let [query   "SELECT ?person ?fullName
                             WHERE {?person person:handle \"jdoe\".
                                    ?person person:fullName ?fullName.}"
                    results (<p! (fluree/query db query {:format :sparql}))]
                (is (= [["ex:jdoe" "Jane Doe"]]
                       results))
                (done))))))

       :clj
       (let [conn @(fluree/connect {:method :memory})
             db   (-> conn
                      (fluree/create "people")
                      deref
                      fluree/db
                      (fluree/stage {"@context" ["https://ns.flur.ee"
                                                 test-utils/default-str-context
                                                 {"person" "http://example.org/Person#"}]
                                      "insert" people-data})
                      deref)]
         (testing "basic query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?fullName
                          WHERE {?person person:handle \"jdoe\".
                                 ?person person:fullName ?fullName.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [["ex:jdoe" "Jane Doe"]]
                    results))))
         (testing "basic wildcard query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT *
                          WHERE {?person person:handle ?handle;
                                         person:favNums ?favNums.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= '[[{?favNums 23, ?handle "bbob", ?person "ex:bbob"}]
                      [{?favNums 0, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 3, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 5, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 6, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 7, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 8, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 9, ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums 3, ?handle "jdoe", ?person "ex:jdoe"}]
                      [{?favNums 7, ?handle "jdoe", ?person "ex:jdoe"}]
                      [{?favNums 42, ?handle "jdoe", ?person "ex:jdoe"}]
                      [{?favNums 99, ?handle "jdoe", ?person "ex:jdoe"}]]
                    results))))
         (testing "basic wildcard query w/ grouping works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT *
                          WHERE {?person person:handle ?handle;
                                         person:favNums ?favNums.}
                          GROUP BY ?person ?handle"
                 results @(fluree/query db query {:format :sparql})]
             (is (= '[[{?favNums [23], ?handle "bbob", ?person "ex:bbob"}]
                      [{?favNums [0 3 5 6 7 8 9], ?handle "jbob", ?person "ex:jbob"}]
                      [{?favNums [3 7 42 99], ?handle "jdoe", ?person "ex:jdoe"}]]
                    results))))
         (testing "basic query w/ OPTIONAL works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?favNums
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
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?favNums
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
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?fullName ?favNums
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
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SHA512(?handle) AS ?handleHash)
                          WHERE {?person person:handle ?handle.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [["f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d"]
                     ["eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90"]
                     ["696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9"]
                     ["fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"]]
                    results))))
         (testing "aggregate fn query works"
           ;; Select the bound var after the AS to make sure it is bound to the result
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav) ?avgFav
                          WHERE {?person person:favNums ?favNums.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[17.66666666666667 17.66666666666667]]
                    results))))
         (testing "aggregate fn w/ GROUP BY query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[5.428571428571429] [37.75] [23]]
                    results))))
         (testing "aggregate fn w/ GROUP BY ... HAVING query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person HAVING(AVG(?favNums) > 10)"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[37.75] [23]]
                    results))))
         (testing "multi-arg fn query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (CONCAT(?handle, '-', ?fullName) AS ?hfn)
                          WHERE {?person person:handle ?handle.
                                 ?person person:fullName ?fullName.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [["bbob-Billy Bob"]
                     ["dankeshön-Ferris Bueller"]
                     ["jbob-Jenny Bob"]
                     ["jdoe-Jane Doe"]]
                    results))))
         (testing "multiple AS selections query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav) (CEIL(?avgFav) AS ?caf)
                          WHERE {?person person:favNums ?favNums.}"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[17.66666666666667 18]]
                    results))))
         (testing "mix of bindings and variables in SELECT query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?favNums (AVG(?favNums) AS ?avg) ?person ?handle (MAX(?favNums) AS ?max)
                          WHERE  {?person person:handle ?handle.
                                  ?person person:favNums ?favNums.}
                          GROUP BY ?person ?handle"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[[23] 23 "ex:bbob" "bbob" 23]
                     [[0 3 5 6 7 8 9] 5.428571428571429 "ex:jbob" "jbob" 9]
                     [[3 7 42 99] 37.75 "ex:jdoe" "jdoe" 99]]
                    results))))
         (testing "COUNT query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (COUNT(?favNums) AS ?numFavs)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[7] [4] [1]]
                    results))))
         (testing "SAMPLE query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SAMPLE(?favNums) AS ?favNum)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"
                 results @(fluree/query db query {:format :sparql})]
             (is (every? #(-> % first integer?) results))))
         (testing "SUM query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SUM(?favNums) AS ?favNum)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [[38] [151] [23]]
                    results))))
         (testing "ORDER BY ASC query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle
                          WHERE {?person person:handle ?handle.}
                          ORDER BY ASC(?handle)"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [["bbob"] ["dankeshön"] ["jbob"] ["jdoe"]]
                    results))))
         (testing "ORDER BY DESC query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle
                          WHERE {?person person:handle ?handle.}
                          ORDER BY DESC(?handle)"
                 results @(fluree/query db query {:format :sparql})]
             (is (= [["jdoe"] ["jbob"] ["dankeshön"] ["bbob"]]
                    results))))
         (let [book-data [{"id"                            "http://example.org/book/1"
                           "type"                          "http://example.org/Book"
                           "http://example.org/book/title" "For Whom the Bell Tolls"}
                          {"id"                            "http://example.org/book/2"
                           "type"                          "http://example.org/Book"
                           "http://example.org/book/title" "The Hitchhiker's Guide to the Galaxy"}]]
           (testing "BASE IRI gets prefixed onto relative IRIs"
             (let [book-db @(fluree/stage db {"@context" ["https://ns.flur.ee"
                                                          test-utils/default-str-context
                                                          {"person" "http://example.org/Person#"}]
                                               "insert" book-data})
                   query   "BASE <http://example.org/book/>
                            SELECT ?book ?title
                            WHERE {?book <title> ?title.}"
                   results @(fluree/query book-db query {:format :sparql})]
               (is (= [["1" "For Whom the Bell Tolls"]
                       ["2" "The Hitchhiker's Guide to the Galaxy"]]
                      results))))
           (testing "PREFIX declarations go into the context"
             (let [book-db @(fluree/stage db {"@context" ["https://ns.flur.ee"
                                                          test-utils/default-str-context
                                                          {"person" "http://example.org/Person#"}]
                                               "insert" book-data})
                   query   "PREFIX book: <http://example.org/book/>
                            SELECT ?book ?title
                            WHERE {?book book:title ?title.}"
                   results @(fluree/query book-db query {:format :sparql})]
               (is (= [["book:1" "For Whom the Bell Tolls"]
                       ["book:2" "The Hitchhiker's Guide to the Galaxy"]]
                      results)))))

           ;; TODO: Make these tests pass

           ;; Language tags aren't supported yet (even in the BNF)
         #_(testing "fn w/ langtag string arg query works"
             (let [query   "SELECT (CONCAT(?fullName, \"'s handle is \"@en, ?handle) AS ?hfn)
                            WHERE {?person person:handle ?handle.
                                   ?person person:fullName ?fullName.}"
                   results @(fluree/query db query {:format :sparql})]
               (is (= [["Billy Bob's handle is bbob"]
                       ["Jane Doe's handle is jdoe"]]
                      results))))

           ;; VALUES gets translated into :bind, but that expects a query fn on the right
           ;; so this string literal doesn't work
         #_(testing "VALUES query works"
             (let [query   "SELECT ?handle
                            WHERE {VALUES ?handle { \"jdoe\" }
                                  ?person person:handle ?handle.}"
                   results @(fluree/query db query {:format :sparql})]
               (is (= ["jdoe"] results))))

           ;; BIND gets translated into :bind, but that expects a query fn on the right
           ;; so this string literal doesn't work
         #_(testing "BIND query works"
             (let [query   "SELECT ?person ?handle
                           WHERE {BIND (\"jdoe\" AS ?handle)
                                  ?person person:handle ?handle.}"
                   results @(fluree/query db query {:format :sparql})]
               (is (= ["ex:jdoe" "jdoe"] results))))))))
