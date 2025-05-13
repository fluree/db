(ns fluree.db.query.sparql-test
  (:require
   #?@(:clj  [[clojure.test :refer [deftest is testing]]]
       :cljs [[cljs.test :refer-macros [deftest is testing async]]
              [clojure.core.async :refer [go <!]]
              [clojure.core.async.interop :refer [<p!]]])
   [clojure.string :as str]
   [fluree.db.api :as fluree]
   [fluree.db.query.sparql :as sparql]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.util.core :as util :refer [try* catch*]]))

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
      (is (= "*" select))))
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

(deftest parse-construct
  (testing "basic construct"
    (let [query "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                 PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                 CONSTRUCT   { <http://example.org/person#Alice> vcard:FN ?name }
                 WHERE       { ?x foaf:name ?name }"]
      (is (= [{"@id" "http://example.org/person#Alice", "vcard:FN" "?name"}]
             (:construct (sparql/->fql query))))))
  (testing "templates with blank nodes"
    (let [query "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                 PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>

                 CONSTRUCT { ?x  vcard:N _:v .
                             _:v vcard:givenName ?gname .
                             _:v vcard:familyName ?fname }
                 WHERE
                   {
                     { ?x foaf:firstname ?gname } UNION  { ?x foaf:givenname   ?gname } .
                     { ?x foaf:surname   ?fname } UNION  { ?x foaf:family_name ?fname } .
                   }"]
      (is (= [{"@id" "?x", "vcard:N" "_:v"}
              {"@id" "_:v", "vcard:givenName" "?gname"}
              {"@id" "_:v", "vcard:familyName" "?fname"}]
             (:construct (sparql/->fql query))))))
  (testing "CONSTRUCT WHERE"
    (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                 CONSTRUCT WHERE { ?x foaf:name ?name }"]
      (is (= {:context {"foaf" "http://xmlns.com/foaf/0.1/"},
              :construct [{"@id" "?x", "foaf:name" "?name"}],
              :where [{"@id" "?x", "foaf:name" "?name"}]}
             (sparql/->fql query))))))

(deftest parse-where
  (testing "simple triple"
    (let [query "SELECT ?person
                 WHERE {?person person:handle \"jdoe\".}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id"           "?person"
               "person:handle" "jdoe"}]
             where)))
    (let [query "PREFIX ex: <http://example.org/>
                 SELECT ?p ?o
                 WHERE { ex:andrew ?p ?o. }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "ex:andrew", "?p" "?o"}]
             where)
          "iri in subject position"))
    (let [query "PREFIX ex: <http://example.org/>
                 SELECT ?s
                 WHERE { ?s ex:friend <urn:12345>. }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s", "ex:friend" {"@id" "urn:12345"}}]
             where)
          "iri in object position"))
    (let [query "PREFIX ex: <http://example.org/>
                 SELECT ?s
                 WHERE { ?s a <urn:12345>;
                            ex:friend ex:brad. }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s", "@type" {"@id" "urn:12345"}}
              {"@id" "?s", "ex:friend" {"@id" "ex:brad"}}]
             where)
          "iri in object position in an object list"))
    (let [query  "SELECT ?person WHERE {?person person:handle \"Los Angeles\"@en .}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person",
               "person:handle" {"@value" "Los Angeles", "@language" "en"}}]
             where)
          "lang literal"))
    (let [query "SELECT ?person
                 WHERE {?person person:birthday \"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime .}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person",
               "person:birthday"
               {"@value" "2011-01-10T14:45:13.815-05:00", "@type" "xsd:dateTime"}}]
             where)
          "datatype literal"))
    (let [query "SELECT ?person
                 WHERE {?person person:isCool true .}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:isCool" true}]
             where)
          "boolean literal"))
    (let [query "SELECT ?person
                 WHERE { ?person a schema:Person . }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "@type" {"@id" "schema:Person"}}]
             where)
          "a as an alias for @type")))
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
  (testing "transitive property path"
    (testing "one-or-more"
      (testing "compact IRI"
        (let [query "SELECT ?uri ?broader
                     WHERE {?uri skos:broader+ ?broader.}"]

          (is (= [{"@id" "?uri", "<skos:broader+>" "?broader"}]
                 (:where (sparql/->fql query))))))
      (testing "expanded IRI"
        (let [query "SELECT ?uri ?broader
                     WHERE {?uri (<http://www.w3.org/2004/02/skos/core#broader>)+ ?broader.}"]

          (is (= [{"@id" "?uri", "<<http://www.w3.org/2004/02/skos/core#broader>+>" "?broader"}]
                 (:where (sparql/->fql query)))))))
    (testing "zero-or-more"
      (testing "compact IRI"
        (let [query "SELECT ?uri ?broader
                 WHERE {?uri (skos:broader)* ?broader.}"]

          (is (= [{"@id" "?uri", "<skos:broader*>" "?broader"}]
                 (:where (sparql/->fql query))))))
      (testing "expanded IRI"
        (let [query "SELECT ?uri ?broader
                 WHERE {?uri (<http://www.w3.org/2004/02/skos/core#broader>)* ?broader.}"]

          (is (= [{"@id" "?uri", "<<http://www.w3.org/2004/02/skos/core#broader>*>" "?broader"}]
                 (:where (sparql/->fql query))))))))
  (testing "UNION"
    (let [query "SELECT ?person ?age
                 WHERE {?person person:age 70 .
                        { ?person person:handle \"dsanchez\".}
                        UNION
                        { ?person person:handle \"anguyen\".}
                        ?person person:age ?age.}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:age" 70}
              [:union
               [{"@id" "?person", "person:handle" "dsanchez"}]
               [{"@id" "?person", "person:handle" "anguyen"}]]
              {"@id" "?person", "person:age" "?age"}]
             where)))
    (let [query "SELECT ?title ?author
                 WHERE  { { ?book dc10:title ?title .  ?book dc10:creator ?author }
                          UNION
                          { ?book dc11:title ?title .  ?book dc11:creator ?author } }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:union
               [{"@id" "?book" "dc10:title" "?title"}
                {"@id" "?book" "dc10:creator" "?author"}]
               [{"@id" "?book" "dc11:title" "?title"}
                {"@id" "?book" "dc11:creator" "?author"}]]]
             where)))
    (let [query "SELECT ?title ?author
                 WHERE  { { ?book dc10:price ?p1 .  ?book dc10:creator ?author . FILTER ( ?p1> 420 ) }
                          UNION
                          { ?book dc11:price ?p2 .  ?book dc11:creator ?author . FILTER ( ?p2 > 42 ) } }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [[:union
               [{"@id" "?book", "dc10:price" "?p1"}
                {"@id" "?book", "dc10:creator" "?author"}
                [:filter "(> ?p1 420)"]]
               [{"@id" "?book", "dc11:price" "?p2"}
                {"@id" "?book", "dc11:creator" "?author"}
                [:filter "(> ?p2 42)"]]]]
             where))))
  (testing "FILTER"
    (let [query "SELECT ?handle ?num
                 WHERE {?person person:handle ?handle.
                        ?person person:favNums ?num.
                        FILTER ( ?num > 10 ).}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:handle" "?handle"}
              {"@id" "?person", "person:favNums" "?num"}
              [:filter "(> ?num 10)"]]
             where)))
    (let [query "PREFIX schema: <http://schema.org/>
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
              [:filter "(regex ?name \"^Jon\" \"i\")"]]
             where)
          "filter by regex call"))
    (let [query "PREFIX schema: <http://schema.org/>
                 SELECT ?pred
                 FROM <cookbook/base>
                 WHERE {
                   ?s ?pred ?o.
                   FILTER (schema:pred != ?pred)
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s", "?pred" "?o"}
              [:filter "(not= \"schema:pred\" ?pred)"]]
             where)
          "filter string values"))
    (let [query "SELECT ?s
                 WHERE {
                   ?product1 ex:numProp1 ?p1.
                   ?product2 ex:numProp2 ?p2.
                   FILTER (?p1 > (?p2 - 120) && ?p1 < (?p1 + 120))
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?product1", "ex:numProp1" "?p1"}
              {"@id" "?product2", "ex:numProp2" "?p2"}
              [:filter "(and (> ?p1 (- ?p2 120)) (< ?p1 (+ ?p1 120)))"]]
             where)
          "EXISTS expression parsing"))
    (let [query "SELECT ?s
                 WHERE {
                   ?s ?p ?o
                   FILTER EXISTS { ?s ex:name \"Larry\" }
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s" "?p" "?o"}
              ["exists" [{"@id" "?s" "ex:name" "Larry"}]]]
             where)
          "EXISTS expression parsing"))
    (let [query "SELECT ?s
                 WHERE {
                   ?s ?p ?o
                   FILTER NOT EXISTS { ?s ex:name \"Larry\" }
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s" "?p" "?o"}
              ["not-exists" [{"@id" "?s" "ex:name" "Larry"}]]]
             where)
          "NOT EXISTS expression parsing"))
    (let [query "SELECT ?s
                 WHERE {
                   ?s ex:test ?testVar
                   FILTER (!bound(?testVar))
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?s", "ex:test" "?testVar"}
              [:filter "(not (bound ?testVar))"]]
             where)
          "NOT EXISTS expression parsing")))
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
                  [:filter "(> ?num 10)"]]]]
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
                  [{"@type" "@id",
                    "@value" ":personA"}
                   {"@type" "@id",
                    "@value" ":personB"}]]]
                {"@id" "?person", "person:handle" "?handle"}]
               (:where (sparql/->fql query)))
            "where pattern: single var, multiple values"))
      (let [query "SELECT ?claim
                   WHERE {
                     ?claim ci:claimDate ?date .
                     FILTER (?date >= ?stateDate && ?date <= ?endDate)
                   }
                   VALUES (?state ?startDate ?endDate) {
                      ( \"New York\" \"2023-03-01\"^^xsd:date \"2023-03-31\"^^xsd:date )
                  }"]
        (is (= [["?state" "?startDate" "?endDate"]
                [["New York" {"@value" "2023-03-01", "@type" "xsd:date"} {"@value" "2023-03-31", "@type" "xsd:date"}]]]
               (:values (sparql/->fql query)))
            "multiple vars with multiple types"))
      (let [query "SELECT * WHERE {
                     VALUES (?color ?direction) {
                     ( dm:red  \"north\" )
                     ( dm:blue  \"west\" )
                   }}"]
        (is (= [[:values
                 [["?color" "?direction"]
                  [[{"@type" "@id",
                     "@value" "dm:red"}
                    "north"]
                   [{"@type" "@id",
                     "@value" "dm:blue"}
                    "west"]]]]]
               (:where (sparql/->fql query)))
            "multiple vars, multiple values")))
    (testing "clause"
      (let [query "SELECT ?handle
                   WHERE { ?person person:handle ?handle.}
                   VALUES ?person { :personA :personB }"]
        (is (= {:where [{"@id" "?person", "person:handle" "?handle"}],
                :values
                ["?person"
                 [{"@type" "@id",
                   "@value" ":personA"}
                  {"@type" "@id",
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
    (testing "function calls"
      (let [query "SELECT ?finger
                 WHERE {?person ex:thumb ?thumb.
                        BIND (!bound(?thumb) AS ?finger)}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [{"@id" "?person", "ex:thumb" "?thumb"}
                [:bind "?finger" "(not (bound ?thumb))"]]
               where)))
      (let [query "SELECT ?person ?abs ?bnode ?bound ?ceil ?coalesce ?concat ?contains ?datatype ?day
                        ?encodeForUri ?floor ?hours ?if ?iri ?lang ?langMatches ?lcase ?md5 ?minutes
                        ?month ?now ?rand ?round ?seconds ?sha1 ?sha256 ?sha512 ?str ?strAfter ?strBefore
                        ?strDt ?strEnds ?strLang ?strLen ?strStarts ?struuid ?timezone ?tz ?ucase
                        ?uri ?uuid ?year ?isBlank ?isIri ?isLiteral ?isNumeric ?isUri ?sameTerm ?in ?notIn
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
                        BIND (?age IN (1, 2, 3, \"foo\", ex:bar) AS ?in)
                        BIND (?age NOT IN (1, 2, 3, \"foo\", ex:bar) AS ?notIn)
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
                        BIND (STRLANG(\"chat\", \"en\") AS ?strLang)
                        BIND (STRLEN(\"chat\") AS ?strLen)
                        BIND (STRSTARTS(\"foobar\", \"foo\") AS ?strStarts)
                        BIND (STRUUID() AS ?struuid)
                        BIND (TIMEZONE(\"2024-4-1T14:45:13.815-05:00\") AS ?timezone)
                        BIND (TZ(\"2024-4-1T14:45:13.815-05:00\") AS ?tz)
                        BIND (UCASE(\"foobar\") AS ?ucase)
                        BIND (URI(\"http://example.com\") AS ?uri)
                        BIND (UUID() AS ?uuid)
                        BIND (YEAR(\"2024-4-1T14:45:13.815-05:00\") AS ?year)
                        BIND (isBLANK(?bnode) AS ?isBlank)
                        BIND (isIRI(?iri) AS ?isIri)
                        BIND (isLITERAL(\"foobar\") AS ?isLiteral)
                        BIND (isNUMERIC(5) AS ?isNumeric)
                        BIND (isURI(?uri) AS ?isUri)
                        BIND (sameTerm(?str, ?str) AS ?sameTerm)
                        ?person person:age ?age.}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [[:bind "?abs" "(abs (* (/ (* (* 1 4) 3) -2) (/ -4 2)))"]
                [:bind "?bnode" "(bnode ?foobar)"]
                [:bind "?bound" "(bound ?abs)"]
                [:bind "?ceil" "(ceil 1.8)"]
                [:bind "?coalesce" "(coalesce ?num1 2)"]
                [:bind "?concat" "(concat \"foo\" \"bar\")"]
                [:bind "?contains" "(contains \"foobar\" \"foo\")"]
                [:bind "?datatype" "(datatype \"foobar\")"]
                [:bind "?day" "(day \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?encodeForUri" "(encodeForUri \"Los Angeles\")"]
                [:bind "?floor" "(floor 1.8)"]
                [:bind "?hours" "(hours \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?if" "(if \"true\" \"yes\" \"no\")"]
                [:bind "?in" "(in ?age [1 2 3 \"foo\" \"ex:bar\"])"]
                [:bind "?notIn" "(not (in ?age [1 2 3 \"foo\" \"ex:bar\"]))"]
                [:bind "?iri" "(iri \"http://example.com\")"]
                [:bind "?lang" "(lang \"Robert\"\"@en\")"]
                [:bind "?langMatches" "(langMatches ?lang \"FR\")"]
                [:bind "?lcase" "(lcase \"FOO\")"]
                [:bind "?md5" "(md5 \"abc\")"]
                [:bind "?minutes" "(minutes \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?month" "(month \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?now" "(now)"]
                [:bind "?rand" "(rand)"]
                [:bind "?round" "(round 1.8)"]
                [:bind "?seconds" "(seconds \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?sha1" "(sha1 \"abc\")"]
                [:bind "?sha256" "(sha256 \"abc\")"]
                [:bind "?sha512" "(sha512 \"abc\")"]
                [:bind "?str" "(str \"foobar\")"]
                [:bind "?strAfter" "(strAfter \"abc\" \"b\")"]
                [:bind "?strBefore" "(strBefore \"abc\" \"b\")"]
                [:bind "?strDt" "(strDt \"iiii\" \"http://example.com/romanNumeral\")"]
                [:bind "?strEnds" "(strEnds \"foobar\" \"bar\")"]
                [:bind "?strLang" "(strLang \"chat\" \"en\")"]
                [:bind "?strLen" "(strLen \"chat\")"]
                [:bind "?strStarts" "(strStarts \"foobar\" \"foo\")"]
                [:bind "?struuid" "(struuid)"]
                [:bind "?timezone" "(timezone \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?tz" "(tz \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?ucase" "(ucase \"foobar\")"]
                [:bind "?uri" "(uri \"http://example.com\")"]
                [:bind "?uuid" "(uuid)"]
                [:bind "?year" "(year \"2024-4-1T14:45:13.815-05:00\")"]
                [:bind "?isBlank" "(isBlank ?bnode)"]
                [:bind "?isIri" "(isIri ?iri)"]
                [:bind "?isLiteral" "(isLiteral \"foobar\")"]
                [:bind "?isNumeric" "(isNumeric 5)"]
                [:bind "?isUri" "(isUri ?uri)"]
                [:bind "?sameTerm" "(sameTerm ?str ?str)"]
                {"@id" "?person", "person:age" "?age"}]
               where)))))
  (testing "GRAPH"
    (let [query "SELECT ?who ?g ?mbox
                 FROM <http://example.org/dft.ttl>
                 FROM NAMED <http://example.org/alice>
                 FROM NAMED <http://example.org/bob>
                 WHERE
                 {
                    ?g dc:publisher ?who .
                    GRAPH ?g { ?x foaf:mbox ?mbox }
                 }"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?g", "dc:publisher" "?who"}
              [:graph "?g" [{"@id" "?x", "foaf:mbox" "?mbox"}]]]
             where))))
  (testing "MINUS"
    (let [query "SELECT ?handle ?num
                 WHERE {?person person:handle ?handle.
                        MINUS {?person person:favNums ?num.}}"
          {:keys [where]} (sparql/->fql query)]
      (is (= [{"@id" "?person", "person:handle" "?handle"}
              [:minus [{"@id" "?person", "person:favNums" "?num"}]]]
             where)))
    (testing "multi-clause"
      (let [query "SELECT ?person ?name ?handle ?favNums
                   WHERE {?person person:fullName ?name.
                          MINUS {?person person:handle ?handle.
                                 ?person person:favNums ?favNums.}}"
            {:keys [where]} (sparql/->fql query)]
        (is (= [{"@id" "?person", "person:fullName" "?name"}
                [:minus
                 [{"@id" "?person", "person:handle" "?handle"}
                  {"@id" "?person", "person:favNums" "?favNums"}]]]
               where))))
    (testing "MINUS + FILTER"
      (let [query "SELECT *
                 WHERE {
                   ?x :p ?n
                   MINUS {
                    ?x :q ?m .
                    FILTER(?n = ?m) } }"
            {:keys [where]} (sparql/->fql query)]
        (is (= [{"@id" "?x", ":p" "?n"}
                [:minus [{"@id" "?x", ":q" "?m"}
                         [:filter "(= ?n ?m)"]]]]
               where)))))
  (testing "subquery"
    (let [query "PREFIX : <http://people.example/>
                 SELECT ?y ?minName
                 WHERE {
                  :alice :knows ?y .
                  {
                    SELECT ?y (MIN(?name) AS ?minName)
                    WHERE {
                      ?y :name ?name .
                    } GROUP BY ?y
                  }
                }"]
      (is (= {:context {"" "http://people.example/"},
              :select ["?y" "?minName"],
              :where
              [{"@id" ":alice", ":knows" "?y"}
               [:query
                {:select ["?y" "(as (min ?name) ?minName)"],
                 :where [{"@id" "?y", ":name" "?name"}],
                 :groupBy ["?y"]}]]}
             (sparql/->fql query))))))

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
             context))))
  (testing "comments"
    (let [query "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                 PREFIX ex: <http://example.org/ns/>
                 # THIS IS IGNORED
                 SELECT ?name ?mbox # THIS TOO
                 WHERE {?x foaf:name ?name.
                        # DEFINITELY IGNORED
                        ?x foaf:mbox ?mbox}"]
      (is (= {:context {"foaf" "http://xmlns.com/foaf/0.1/", "ex" "http://example.org/ns/"},
              :select ["?name" "?mbox"],
              :where [{"@id" "?x", "foaf:name" "?name"} {"@id" "?x", "foaf:mbox" "?mbox"}]}
             (sparql/->fql query))))))

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
  (testing "LIMIT+OFFSET"
    (let [query "SELECT ?person
                 WHERE {?person person:fullName ?fullName}
                 LIMIT 10
                 OFFSET 10"]
      (is (= {:limit 10, :offset 10}
             (select-keys (sparql/->fql query) [:limit :offset]))))
    (let [query "SELECT ?person
                 WHERE {?person person:fullName ?fullName}
                 OFFSET 10
                 LIMIT 10"]
      (is (= {:limit 10, :offset 10}
             (select-keys (sparql/->fql query) [:limit :offset])))))
  (testing "ORDER BY"
    (testing "ASC"
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY ASC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= [["asc" "?favNums"]]
               orderBy)))
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY asc(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= [["asc" "?favNums"]]
               orderBy))))
    (testing "DESC"
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY DESC(?favNums)"
            {:keys [orderBy]} (sparql/->fql query)]
        (is (= [["desc" "?favNums"]]
               orderBy)))
      (let [query "SELECT ?favNums
                   WHERE {?person person:favNums ?favNums}
                   ORDER BY desc(?favNums)"
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
  (testing "multiple GROUP BY"
    (let [query "SELECT ?handle
                 WHERE {?person person:handle ?handle.}
                 GROUP BY ?person ?handle"
          {:keys [groupBy]} (sparql/->fql query)]
      (is (= ["?person" "?handle"]
             groupBy))))
  (testing "multiple HAVING constraints"
    (let [query "SELECT ?handle
                 WHERE {?person person:handle ?handle.}
                 GROUP BY ?person ?handle
                 HAVING(STRLEN(?handle) < 5 && (STRSTARTS(?handle, \"foo\") || STRSTARTS(?handle, \"bar\")))"
          {:keys [having]} (sparql/->fql query)]
      (is (= "(and (< (strLen ?handle) 5) (or (strStarts ?handle \"foo\") (strStarts ?handle \"bar\")))"
             having))))
  (testing "DISTINCT"
    (let [query "SELECT DISTINCT ?person ?fullName
                 WHERE {?person person:fullName ?fullName}"
          {:keys [selectDistinct]} (sparql/->fql query)]
      (is (= ["?person" "?fullName"]
             selectDistinct)))))

(deftest parse-recursive
  (let [query "SELECT ?followHandle
               WHERE {?person person:handle \"anguyen\".
                      ?person person:follows+ ?follows.
                      ?follows person:handle ?followHandle.}"
        {:keys [where]} (sparql/->fql query)]
    (is (= [{"@id" "?person", "person:handle" "anguyen"}
            {"@id" "?person", "<person:follows+>" "?follows"}
            {"@id" "?follows", "person:handle" "?followHandle"}]
           where)))
  (testing "depth"
    (let [query "SELECT ?followHandle
                 WHERE {?person person:handle \"anguyen\".
                        ?person person:follows+3 ?follows.
                        ?follows person:handle ?followHandle.}"]
      (is (= "Depth modifiers on transitive path elements are not supported."
             (try* (sparql/->fql query)
                   (catch* e (ex-message e))))))))

(deftest parse-update
  (testing "insert data"
    (let [query "PREFIX dc: <http://purl.org/dc/elements/1.1/>
               INSERT DATA
                 {
                   <http://example/book1> dc:title \"A new book\" ;
                                          dc:creator \"A.N.Other\" .
                 }"]
      (is (= {:context {"dc" "http://purl.org/dc/elements/1.1/"},
              :insert  [{"@id" "http://example/book1", "dc:title" "A new book"}
                        {"@id" "http://example/book1", "dc:creator" "A.N.Other"}]}
             (sparql/->fql query)))))
  (testing "delete-data"
    (let [query "PREFIX dc: <http://purl.org/dc/elements/1.1/>
               DELETE DATA
                 {
                   <http://example/book1> dc:title \"A new book\" ;
                                          dc:creator \"A.N.Other\" .
                 }"]
      (is (= {:context {"dc" "http://purl.org/dc/elements/1.1/"},
              :delete  [{"@id" "http://example/book1", "dc:title" "A new book"}
                        {"@id" "http://example/book1", "dc:creator" "A.N.Other"}]}
             (sparql/->fql query)))))
  (testing "delete/insert where"
    (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
               WITH <http://example/addresses>
               DELETE { ?person foaf:givenName 'Bill' }
               INSERT { ?person foaf:givenName 'William' }
               WHERE
                 { ?person foaf:givenName 'Bill'
                 }"]
      (is (= {:context {"foaf" "http://xmlns.com/foaf/0.1/"},
              :ledger  "http://example/addresses",
              :delete  [{"@id" "?person", "foaf:givenName" "Bill"}],
              :insert  [{"@id" "?person", "foaf:givenName" "William"}],
              :where   [{"@id" "?person", "foaf:givenName" "Bill"}]}
             (sparql/->fql query)))))
  (testing "delete where"
    (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
               DELETE WHERE { ?person foaf:givenName 'Fred';
                                      ?property      ?value }"]
      (is (= {:context {"foaf" "http://xmlns.com/foaf/0.1/"},
              :where   [{"@id" "?person", "foaf:givenName" "Fred"}
                        {"@id" "?person", "?property" "?value"}],
              :delete  [{"@id" "?person", "foaf:givenName" "Fred"}
                        {"@id" "?person", "?property" "?value"}]}
             (sparql/->fql query)))))
  (testing "using clauses"
    (testing "one USING clause"
      (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                 DELETE { ?person ?property ?value }
                 USING <http://flur.ee/ledger1>
                 WHERE { ?person foaf:givenName 'Fred';
                                 ?property      ?value }"]
        (is (= {:context {"foaf" "http://xmlns.com/foaf/0.1/"},
                :delete [{"@id" "?person", "?property" "?value"}],
                :ledger "http://flur.ee/ledger1",
                :where [{"@id" "?person", "foaf:givenName" "Fred"}
                        {"@id" "?person", "?property" "?value"}]}
               (sparql/->fql query)))))
    (testing "one USING NAMED clause"
      (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                 DELETE { ?person ?property ?value }
                 USING NAMED <http://flur.ee/ledger2>
                 WHERE { ?person foaf:givenName 'Fred';
                                 ?property      ?value }"]
        (is (= "USING NAMED is not supported in SPARQL Update."
               (try* (sparql/->fql query)
                     (catch* e (ex-message e)))))))
    (testing "more than one"
      (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                 DELETE { ?person ?property ?value }
                 USING <http://flur.ee/ledger1>
                 USING NAMED <http://flur.ee/ledger2>
                 WHERE { ?person foaf:givenName 'Fred';
                                 ?property      ?value }"]
        (is (= "More than one USING clause is not supported in SPARQL Update."
               (try* (sparql/->fql query)
                     (catch* e (ex-message e))))))))

  (testing "graph patterns"
    (let [query "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
               PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

               INSERT
                 { GRAPH <http://example/addresses>
                   {
                     ?person  foaf:name  ?name .
                     ?person  foaf:mbox  ?email
                   } }
              WHERE
                { GRAPH  <http://example/people>
                  {
                    ?person  foaf:name  ?name .
                    OPTIONAL { ?person  foaf:mbox  ?email }
                  } }"]
      (is (= "GRAPH not supported in INSERT. Use WITH or USING instead."
             (try* (sparql/->fql query)
                   (catch* e (ex-message e))))))))

(deftest parsing-error
  (testing "invalid query throws expected error"
    (let [query "SELECT ?person
                 WHERE  ?person person:fullName \"jdoe\""
          err (try (sparql/->fql query)
                   (catch #?(:clj  clojure.lang.ExceptionInfo
                             :cljs :default) e e))]
      (is (= #?(:clj (str/join "\n" ["Improperly formatted SPARQL query:"
                                     "Parse error at line 2, column 25:"
                                     "                 WHERE  ?person person:fullName \"jdoe\""
                                     "                        ^"
                                     "Expected:"
                                     "{"
                                     ""
                                     ""
                                     "Note: Fluree does not support all SPARQL features."
                                     "See here for more information:"
                                     "https://next.developers.flur.ee/docs/reference/errorcodes#query-sparql-improper"])
                :cljs
                "Improperly formatted SPARQL query:\nParse error at line 2, column 25:\n                 WHERE  ?person person:fullName \"jdoe\"\n                        ^\nExpected:\n\"{\"\n\n\nNote: Fluree does not support all SPARQL features.\nSee here for more information:\nhttps://next.developers.flur.ee/docs/reference/errorcodes#query-sparql-improper")
             (ex-message err)))
      (is (= {:status 400
              :error :db/invalid-query}
             (ex-data err))))))

(deftest ^:integration query-test
  (let [txn (str/join "\n"
                      ["PREFIX person: <http://example.org/Person#>"
                       "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
                       "INSERT DATA {"
                       "ex:jdoe a ex:Person; person:handle \"jdoe\"; person:fullName \"Jane Doe\"; person:favNums 3, 7, 42, 99."
                       "ex:bbob a ex:Person; person:handle \"bbob\"; person:fullName \"Billy Bob\"; person:favNums 23."
                       "ex:jbob a ex:Person; person:handle \"jbob\"; person:fullName \"Jenny Bob\"; person:favNums 8, 6, 7, 5, 3, 0, 9."
                       "ex:fbueller a ex:Person; person:handle \"dankeshön\"; person:fullName \"Ferris Bueller\"."
                       "ex:alice foaf:givenname \"Alice\"; foaf:family_name \"Hacker\"."
                       "ex:bob foaf:firstname \"Bob\"; foaf:surname \"Hacker\"."
                       "ex:carol ex:catchphrase \"Heyyyy\"@en."
                       "ex:carol ex:catchphrase \"¡Eyyyy!\"@es."
                       "}"])]
    #?(:cljs
       (async done
              (go
                (let [conn   (<! (test-utils/create-conn))
                      ledger (<p! (fluree/create conn "people"))
                      db     (<p! (fluree/stage (fluree/db ledger) txn {:format :sparql}))]
                  (testing "basic query works"
                    (let [query   "PREFIX person: <http://example.org/Person#>
                                    SELECT ?person ?fullName
                                    WHERE {?person person:handle \"jdoe\".
                                           ?person person:fullName ?fullName.}"
                          results (<p! (fluree/query db query {:format :sparql}))]
                      (is (= [["ex:jdoe" "Jane Doe"]]
                             results))
                      (done))))))

       :clj
       (let [conn   @(fluree/connect-memory)
             ledger @(fluree/create conn "people")
             db     @(fluree/stage (fluree/db ledger) txn {:format :sparql})]
         (testing "basic query works"
           (let [query "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?fullName
                          WHERE {?person person:handle \"jdoe\".
                                 ?person person:fullName ?fullName.}"]
             (testing "output :fql"
               (is (= [["ex:jdoe" "Jane Doe"]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["fullName" "person"]},
                       "results"
                       {"bindings"
                        [{"person" {"type" "uri", "value" "ex:jdoe"},
                          "fullName" {"value" "Jane Doe", "type" "literal"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic filter works"
           (let [query "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle ?favNum
                          WHERE {?person person:handle ?handle ;
                                         person:favNums ?favNum .
                                 FILTER ( ?favNum > 10 ) .}"]
             (testing "output :fql"
               (is (= [["bbob" 23] ["jdoe" 42] ["jdoe" 99]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNum" "handle"]},
                       "results"
                       {"bindings"
                        [{"handle" {"value" "bbob", "type" "literal"},
                          "favNum"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"handle" {"value" "jdoe", "type" "literal"},
                          "favNum"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"handle" {"value" "jdoe", "type" "literal"},
                          "favNum"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic wildcard query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT *
                          WHERE {?person person:handle ?handle;
                                         person:favNums ?favNums.}"]
             (testing "output :fql"
               (is (= '[{?favNums 23, ?handle "bbob", ?person "ex:bbob"}
                        {?favNums 0, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 3, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 5, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 6, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 7, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 8, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 9, ?handle "jbob", ?person "ex:jbob"}
                        {?favNums 3, ?handle "jdoe", ?person "ex:jdoe"}
                        {?favNums 7, ?handle "jdoe", ?person "ex:jdoe"}
                        {?favNums 42, ?handle "jdoe", ?person "ex:jdoe"}
                        {?favNums 99, ?handle "jdoe", ?person "ex:jdoe"}]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNums" "handle" "person"]},
                       "results"
                       {"bindings"
                        [{"favNums"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "bbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:bbob"}}
                         {"favNums"
                          {"value" "0",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "5",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "6",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "8",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic wildcard query w/ grouping works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT *
                          WHERE {?person person:handle ?handle;
                                         person:favNums ?favNums.}
                          GROUP BY ?person ?handle"]
             (testing "output :fql"
               (is (= '[{?favNums [23], ?handle "bbob", ?person "ex:bbob"}
                        {?favNums [0 3 5 6 7 8 9], ?handle "jbob", ?person "ex:jbob"}
                        {?favNums [3 7 42 99], ?handle "jdoe", ?person "ex:jdoe"}]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNums" "handle" "person"]},
                       "results"
                       {"bindings"
                        [{"favNums"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "bbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:bbob"}}
                         {"favNums"
                          {"value" "0",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "5",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "6",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "8",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jbob"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}
                         {"favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "person" {"type" "uri", "value" "ex:jdoe"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic query w/ OPTIONAL works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?favNums
                          WHERE {?person person:handle ?handle.
                                 OPTIONAL{?person person:favNums ?favNums.}}"]
             (testing "output :fql"
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
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNums" "person"]},
                       "results"
                       {"bindings"
                        [{"person" {"type" "uri", "value" "ex:bbob"},
                          "favNums"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:fbueller"},
                          "favNums" {"value" "", "type" "literal"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "0",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "5",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "6",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "8",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic query w/ GROUP BY & OPTIONAL works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?favNums
                          WHERE {?person person:handle ?handle.
                                 OPTIONAL{?person person:favNums ?favNums.}}
                          GROUP BY ?person"]
             (testing "output :fql"
               (is (= [["ex:bbob" [23]]
                       ["ex:fbueller" nil]
                       ["ex:jbob" [0 3 5 6 7 8 9]]
                       ["ex:jdoe" [3 7 42 99]]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNums" "person"]},
                       "results"
                       {"bindings"
                        [{"person" {"type" "uri", "value" "ex:bbob"},
                          "favNums"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:fbueller"},
                          "favNums" {"value" "", "type" "literal"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "0",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "5",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "6",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "8",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jbob"},
                          "favNums"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "basic query w/ omitted subjects works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?person ?fullName ?favNums
                          WHERE {?person person:handle \"jdoe\";
                                         person:fullName ?fullName;
                                         person:favNums ?favNums.}"]
             (testing "output :fql"
               (is (= [["ex:jdoe" "Jane Doe" 3]
                       ["ex:jdoe" "Jane Doe" 7]
                       ["ex:jdoe" "Jane Doe" 42]
                       ["ex:jdoe" "Jane Doe" 99]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNums" "fullName" "person"]},
                       "results"
                       {"bindings"
                        [{"person" {"type" "uri", "value" "ex:jdoe"},
                          "fullName" {"value" "Jane Doe", "type" "literal"},
                          "favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "fullName" {"value" "Jane Doe", "type" "literal"},
                          "favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "fullName" {"value" "Jane Doe", "type" "literal"},
                          "favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"person" {"type" "uri", "value" "ex:jdoe"},
                          "fullName" {"value" "Jane Doe", "type" "literal"},
                          "favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "scalar fn query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SHA512(?handle) AS ?handleHash)
                          WHERE {?person person:handle ?handle.}"]
             (testing "output :fql"
               (is (= [["f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d"]
                       ["eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90"]
                       ["696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9"]
                       ["fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["handleHash"]},
                       "results"
                       {"bindings"
                        [{"handleHash"
                          {"value"
                           "f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d",
                           "type" "literal"}}
                         {"handleHash"
                          {"value"
                           "eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90",
                           "type" "literal"}}
                         {"handleHash"
                          {"value"
                           "696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9",
                           "type" "literal"}}
                         {"handleHash"
                          {"value"
                           "fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401",
                           "type" "literal"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "aggregate fn query works"
           ;; Select the bound var after the AS to make sure it is bound to the result
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav) ?avgFav
                          WHERE {?person person:favNums ?favNums.}"]
             (testing "output :fql"
               (is (= [[17.66666666666667 17.66666666666667]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               ;; cannot have same key twice in a binding, binding isn't repeated
               (is (= {"head" {"vars" ["avgFav"]},
                       "results"
                       {"bindings"
                        [{"avgFav"
                          {"value" "17.66666666666667",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "aggregate fn w/ GROUP BY query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"]
             (testing "output :fql"
               (is (= [[5.428571428571429] [37.75] [23]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["avgFav"]},
                       "results"
                       {"bindings"
                        [{"avgFav"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"}}
                         {"avgFav"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"}}
                         {"avgFav"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "aggregate fn w/ GROUP BY ... HAVING query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person HAVING(AVG(?favNums) > 10)"]
             (testing "output :fql"
               (is (= [[37.75] [23]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["avgFav"]},
                       "results"
                       {"bindings"
                        [{"avgFav"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"}}
                         {"avgFav"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "multi-arg fn query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (CONCAT(?handle, '-', ?fullName) AS ?hfn)
                          WHERE {?person person:handle ?handle.
                                 ?person person:fullName ?fullName.}"]
             (testing "output :fql"
               (is (= [["bbob-Billy Bob"]
                       ["dankeshön-Ferris Bueller"]
                       ["jbob-Jenny Bob"]
                       ["jdoe-Jane Doe"]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["hfn"]},
                       "results"
                       {"bindings"
                        [{"hfn" {"value" "bbob-Billy Bob", "type" "literal"}}
                         {"hfn" {"value" "dankeshön-Ferris Bueller", "type" "literal"}}
                         {"hfn" {"value" "jbob-Jenny Bob", "type" "literal"}}
                         {"hfn" {"value" "jdoe-Jane Doe", "type" "literal"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "multiple AS selections query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (AVG(?favNums) AS ?avgFav) (CEIL(?avgFav) AS ?caf)
                          WHERE {?person person:favNums ?favNums.}"]
             (testing "output :fql"
               (is (= [[17.66666666666667 18]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["avgFav" "caf"]},
                       "results"
                       {"bindings"
                        [{"avgFav"
                          {"value" "17.66666666666667",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "caf"
                          {"value" "18",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "mix of bindings and variables in SELECT query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?favNums (AVG(?favNums) AS ?avg) ?person ?handle (MAX(?favNums) AS ?max)
                          WHERE  {?person person:handle ?handle.
                                  ?person person:favNums ?favNums.}
                          GROUP BY ?person ?handle"]
             (testing "output :fql"
               (is (= [[[23] 23 "ex:bbob" "bbob" 23]
                       [[0 3 5 6 7 8 9] 5.428571428571429 "ex:jbob" "jbob" 9]
                       [[3 7 42 99] 37.75 "ex:jdoe" "jdoe" 99]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["avg" "favNums" "handle" "max" "person"]},
                       "results"
                       {"bindings"
                        [{"favNums"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "person" {"type" "uri", "value" "ex:bbob"},
                          "handle" {"value" "bbob", "type" "literal"},
                          "max"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "0",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "5",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "6",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "8",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "5.428571428571429",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jbob"},
                          "handle" {"value" "jbob", "type" "literal"},
                          "max"
                          {"value" "9",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "3",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jdoe"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "max"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jdoe"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "max"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "42",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jdoe"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "max"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNums"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"},
                          "avg"
                          {"value" "37.75",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#double"},
                          "person" {"type" "uri", "value" "ex:jdoe"},
                          "handle" {"value" "jdoe", "type" "literal"},
                          "max"
                          {"value" "99",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "COUNT query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (COUNT(?favNums) AS ?numFavs)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"]
             (testing "output :fql"
               (is (= [[7] [4] [1]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["numFavs"]},
                       "results"
                       {"bindings"
                        [{"numFavs"
                          {"value" "7",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"numFavs"
                          {"value" "4",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"numFavs"
                          {"value" "1",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "SAMPLE query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SAMPLE(?favNums) AS ?favNum)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"]
             (testing "output :fql"
               (let [results @(fluree/query db query {:format :sparql})]
                 (is (= 3 (count results)))
                 (is (every? #(-> % first integer?) results))))
             (testing "output :sparql"
               (let [results @(fluree/query db query {:format :sparql :output :sparql})]
                 (is (= 3 (-> results (get "results") (get "bindings") count)))
                 (is (every? #(-> % (get "favNum") (get "datatype") (= "http://www.w3.org/2001/XMLSchema#integer"))
                             (-> results (get "results") (get "bindings"))))))))
         (testing "SUM query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (SUM(?favNums) AS ?favNum)
                          WHERE {?person person:favNums ?favNums.}
                          GROUP BY ?person"]
             (testing "output :fql"
               (is (= [[38] [151] [23]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["favNum"]},
                       "results"
                       {"bindings"
                        [{"favNum"
                          {"value" "38",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNum"
                          {"value" "151",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}
                         {"favNum"
                          {"value" "23",
                           "type" "literal",
                           "datatype" "http://www.w3.org/2001/XMLSchema#integer"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "CONSTRUCT query works"
           (testing "CONSTRUCT"
             (let [query "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                        PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                        CONSTRUCT { ?x vcard:givenName ?gname .
                                    ?x vcard:familyName ?fname }
                        WHERE
                        {
                          { ?x foaf:firstname ?gname } UNION  { ?x foaf:givenname   ?gname } .
                          { ?x foaf:surname   ?fname } UNION  { ?x foaf:family_name ?fname } .
                        }"]
               (is (= {"@context" {"foaf" "http://xmlns.com/foaf/0.1/",
                                   "vcard" "http://www.w3.org/2001/vcard-rdf/3.0#"}
                       "@graph"
                       [{"@id" "ex:alice",
                         "vcard:givenName" ["Alice"],
                         "vcard:familyName" ["Hacker"]}
                        {"@id" "ex:bob",
                         "vcard:givenName" ["Bob"],
                         "vcard:familyName" ["Hacker"]}]}
                      @(fluree/query db query {:format :sparql})))))
           (testing "CONSTRUCT WHERE"
             (let [query "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                        PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                        CONSTRUCT WHERE { ?x foaf:firstname ?fname }"]
               (is (= {"@context" {"foaf" "http://xmlns.com/foaf/0.1/",
                                   "vcard" "http://www.w3.org/2001/vcard-rdf/3.0#"}
                       "@graph" [{"@id" "ex:bob", "foaf:firstname" ["Bob"]}]}
                      @(fluree/query db query {:format :sparql})))))
           ;; non-deterministic output
           #_(testing "CONSTRUCT with blank nodes"
               (let [query "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                        PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                        CONSTRUCT { ?x  vcard:N _:v .
                                    _:v vcard:givenName ?gname .
                                    _:v vcard:familyName ?fname }
                        WHERE
                        {
                          { ?x foaf:firstname ?gname } UNION  { ?x foaf:givenname   ?gname } .
                          { ?x foaf:surname   ?fname } UNION  { ?x foaf:family_name ?fname } .
                        }"]
                 (is (= {"@context" {"foaf" "http://xmlns.com/foaf/0.1/",
                                     "vcard" "http://www.w3.org/2001/vcard-rdf/3.0#"}
                         "@graph"
                         [{"@id" "_:v1",
                           "vcard:givenName" ["Bob"],
                           "vcard:familyName" ["Hacker"]}
                          {"@id" "_:v2",
                           "vcard:givenName" ["Alice"],
                           "vcard:familyName" ["Hacker"]}
                          {"@id" "ex:alice", "vcard:N" [{"@id" "_:v2"}]}
                          {"@id" "ex:bob", "vcard:N" [{"@id" "_:v1"}]}]}
                        @(fluree/query db query {:format :sparql}))))))

         (testing "ORDER BY ASC query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle
                          WHERE {?person person:handle ?handle.}
                          ORDER BY ASC(?handle)"]
             (testing "output :fql"
               (is (= [["bbob"] ["dankeshön"] ["jbob"] ["jdoe"]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["handle"]},
                       "results"
                       {"bindings"
                        [{"handle" {"value" "bbob", "type" "literal"}}
                         {"handle" {"value" "dankeshön", "type" "literal"}}
                         {"handle" {"value" "jbob", "type" "literal"}}
                         {"handle" {"value" "jdoe", "type" "literal"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (testing "ORDER BY DESC query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle
                          WHERE {?person person:handle ?handle.}
                          ORDER BY DESC(?handle)"]
             (testing "output :fql"
               (is (= [["jdoe"] ["jbob"] ["dankeshön"] ["bbob"]]
                      @(fluree/query db query {:format :sparql}))))
             (testing "output :sparql"
               (is (= {"head" {"vars" ["handle"]},
                       "results"
                       {"bindings"
                        [{"handle" {"value" "jdoe", "type" "literal"}}
                         {"handle" {"value" "jbob", "type" "literal"}}
                         {"handle" {"value" "dankeshön", "type" "literal"}}
                         {"handle" {"value" "bbob", "type" "literal"}}]}}
                      @(fluree/query db query {:format :sparql :output :sparql}))))))
         (let [book-data [{"id"                            "http://example.org/book/1"
                           "type"                          "http://example.org/Book"
                           "http://example.org/book/title" "For Whom the Bell Tolls"}
                          {"id"                            "http://example.org/book/2"
                           "type"                          "http://example.org/Book"
                           "http://example.org/book/title" "The Hitchhiker's Guide to the Galaxy"}]]
           (testing "BASE IRI gets prefixed onto relative IRIs"
             (let [book-db @(fluree/stage db {"@context" [test-utils/default-str-context
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
             (let [book-db @(fluree/stage db {"@context" [test-utils/default-str-context
                                                          {"person" "http://example.org/Person#"}]
                                              "insert" book-data})
                   query   "PREFIX book: <http://example.org/book/>
                            SELECT ?book ?title
                            WHERE {?book book:title ?title.}"
                   results @(fluree/query book-db query {:format :sparql})]
               (is (= [["book:1" "For Whom the Bell Tolls"]
                       ["book:2" "The Hitchhiker's Guide to the Galaxy"]]
                      results)))))

         (testing "langstrings are returned annotated"
           (let [query "SELECT ?catchphrase WHERE {<ex:carol> <ex:catchphrase> ?catchphrase}"]
             (is (= {"head" {"vars" ["catchphrase"]},
                     "results"
                     {"bindings"
                      [{"catchphrase"
                        {"value" "Heyyyy", "type" "literal", "xml:lang" "en"}}
                       {"catchphrase"
                        {"value" "¡Eyyyy!", "type" "literal", "xml:lang" "es"}}]}}
                    @(fluree/query db query {:format :sparql :output :sparql})))))

         (testing "fn w/ langtag string arg query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                          SELECT (CONCAT(?fullName, \"'s handle is \"@en, ?handle) AS ?hfn)
                            WHERE {?person person:handle ?handle.
                                   ?person person:fullName ?fullName.}"]
             (is (= [["Billy Bob's handle is bbob"]
                     ["Ferris Bueller's handle is dankeshön"]
                     ["Jenny Bob's handle is jbob"]
                     ["Jane Doe's handle is jdoe"]]
                    @(fluree/query db query {:format :sparql})))))

         (testing "VALUES query works"
           (let [query   "PREFIX person: <http://example.org/Person#>
                            SELECT ?handle
                            WHERE {VALUES ?handle { \"jdoe\" }
                                  ?person person:handle ?handle.}"]
             (is (= [["jdoe"]]
                    @(fluree/query db query {:format :sparql})))))))))
