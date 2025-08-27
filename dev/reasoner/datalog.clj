(ns reasoner.datalog
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<? <??]]))

(comment

  (def conn @(fluree/connect-memory nil))

  (def ledger @(fluree/create conn "test/rule"))

  (def db @(fluree/stage
            @(fluree/db conn "test/rule")
            {"@context" {"ex" "http://example.org/"}
             "insert"   [{"@id"        "ex:brian"
                          "ex:name"    "Brian"
                          "ex:uncle"   {"@id" "ex:jim"}
                          "ex:sibling" [{"@id" "ex:laura"} {"@id" "ex:bob"}]
                          "ex:address" {"ex:country" {"@id" "ex:Canada"}}
                          "ex:age"     42
                          "ex:parent"  {"@id"        "ex:carol"
                                        "ex:name"    "Carol"
                                        "ex:age"     72
                                        "ex:address" {"ex:country" {"@id" "ex:Singapore"}}
                                        "ex:brother" {"@id" "ex:mike"}}}
                         {"@id"     "ex:laura"
                          "ex:name" "Laura"}
                         {"@id"       "ex:bob"
                          "ex:name"   "Bob"
                          "ex:gender" {"@id" "ex:Male"}}]}))

;; insert rules that a reasoner will evaluate with a reasoned-db
  (def db2 @(fluree/stage
             db {"insert" [uncle-rule sibling-rule brother-rule senior-rule runs-cold-rule]}
             {:meta false}))

  (def reasoned-db @(fluree/reason db2 :datalog))

;;;; parents (via sibling)
  ;; without reasoning
  @(fluree/query db2
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?parent"]
                  :where   {"@id"       "?s",
                            "ex:parent" "?parent"}})
  ;; with reasoning
  @(fluree/query reasoned-db
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?parent"]
                  :where   {"@id"       "?s",
                            "ex:parent" "?parent"}})

  ;;;; brother
  ;; without reasoning
  @(fluree/query db2
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?brother"]
                  :where   {"@id"        "?s",
                            "ex:brother" "?brother"}})
  ;; with reasoning
  @(fluree/query reasoned-db
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?brother"]
                  :where   {"@id"        "?s",
                            "ex:brother" "?brother"}})

  ;;;; uncle
  ;; without reasoning
  @(fluree/query db2
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?uncle"]
                  :where   {"@id"      "?s",
                            "ex:uncle" "?uncle"}})
  ;; with reasoning
  @(fluree/query reasoned-db
                 {:context {"ex" "http://example.org/"}
                  :select  ["?s" "?uncle"]
                  :where   {"@id"      "?s",
                            "ex:uncle" "?uncle"}})

  ;;;; seniorCitizen
  ;; without reasoning
  @(fluree/query db2
                 {:context {"ex" "http://example.org/"}
                  :select  "?s"
                  :where   {"@id"              "?s",
                            "ex:seniorCitizen" true}})
  ;; with reasoning
  @(fluree/query reasoned-db
                 {:context {"ex" "http://example.org/"}
                  :select  "?s"
                  :where   {"@id"              "?s",
                            "ex:seniorCitizen" true}}))

(def uncle-rule
  {"@context" {"f"  "http://flur.ee/ns/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:uncleRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"       "?person",
                                     "ex:parent" {"ex:brother" {"@id" "?pBrother"}}},
                         "insert"   {"@id"      "?person",
                                     "ex:uncle" "?pBrother"}}}})

(def sibling-rule
  {"@context" {"f"  "http://flur.ee/ns/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:siblingRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"        "?person",
                                     "ex:sibling" "?sibling"
                                     "ex:parent"  "?parent"},
                         "insert"   {"@id"       "?sibling",
                                     "ex:parent" "?parent"}}}})

(def brother-rule
  {"@context" {"f"  "http://flur.ee/ns/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:brotherRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"        "?person",
                                     "ex:sibling" {"@id"       "?sibling"
                                                   "ex:gender" {"@id" "ex:Male"}}}
                         "insert"   {"@id"        "?person",
                                     "ex:brother" "?sibling"}}}})

(def senior-rule
  {"@context" {"f"  "http://flur.ee/ns/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:seniorRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    [{"@id"    "?person",
                                      "ex:age" "?age"}
                                     ["filter" "(>= ?age 62)"]]
                         "insert"   {"@id"              "?person",
                                     "ex:seniorCitizen" true}}}})

(def runs-cold-rule
  {"@context" {"f"  "http://flur.ee/ns/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:seniorRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"              "?person",
                                     "ex:seniorCitizen" true}
                         "insert"   {"@id"         "?person",
                                     "ex:runsCold" true}}}})
;
;(def runs-cold2
;  {"@context" {"f"  "http://flur.ee/ns/ledger#"
;               "ex" "http://example.org/"},
;   "@id"      "ex:seniorRule"
;   "f:rule"   {"@type"  "@json"
;               "@value" {"@context" {"ex" "http://example.org/"}
;                         "where"    {"@id"        "?person",
;                                     "ex:address" {"ex:country" {"@id" "ex:Canada"}}}
;                         "insert"   {"@id"         "?person",
;                                     "ex:runsCold" true}}}})

(comment

  ;; runsCold
  @(fluree/query
    db2 {:context {"ex" "http://example.org/"}
         :select  "?s"
         :where   {"@id"         "?s",
                   "ex:runsCold" true}})

  @(fluree/query
    db2 {:context {"ex" "http://example.org/"}
         :select  {"ex:laura" ["*"]}
         :depth   3}))

