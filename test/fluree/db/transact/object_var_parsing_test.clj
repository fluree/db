(ns fluree.db.transact.object-var-parsing-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.test-utils :as test-utils]))

(def ctx {"ex" "http://example.org/ns/"
          "schema" "http://schema.org/"
          "xsd" "http://www.w3.org/2001/XMLSchema#"})

(deftest insert-does-not-parse-bare-var-by-default
  (testing "Bare object '?age' in insert remains a string literal"
    (let [txn {"@context" ctx
               "@graph"   [{"@id" "ex:s"
                            "schema:text" "?age"}]}
          {:keys [insert]} (parse/parse-insert-txn txn {:context (parse/parse-txn-opts nil nil nil)})
          [_ _ o] (first insert)]
      (is (nil? (where/get-variable o))
          "Should not parse bare '?age' as a variable in insert")
      (is (true? (where/matched-value? o))
          "Insert object should be a matched value")
      (is (= "?age" (where/get-value o))
          "Literal should equal '?age'"))))

(deftest update-bare-var-default-throws-when-unbound
  (testing "Bare object '?age' in update without binding should throw by default"
    (let [txn {"@context" ctx
               "insert"  [{"@id" "ex:s"
                           "schema:text" "?age"}]}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"variable \?age is not bound"
           (parse/parse-update-txn txn {}))))))

(deftest update-with-object-var-parsing-false-treats-bare-var-as-literal
  (testing "With objectVarParsing false, bare object '?not-a-var' in update insert is literal"
    (let [txn {"@context" ctx
               "insert"  [{"@id" "ex:s"
                           "schema:text" "?not-a-var"}]}
          {:keys [insert]} (parse/parse-update-txn txn {:object-var-parsing false})
          [_ _ o] (first insert)]
      (is (nil? (where/get-variable o))
          "Should not parse '?not-a-var' as variable under flag=false")
      (is (= "?not-a-var" (where/get-value o))
          "Literal should equal '?not-a-var'"))))

(deftest update-explicit-variable-map-parses-when-flag-false-and-bound
  (testing "Explicit {'@variable': '?d'} parses as variable when flag=false and bound in where"
    (let [txn {"@context" ctx
               "where"   [{"@id" "ex:s"
                           "schema:date" {"@variable" "?d"}}]
               "insert"  [{"@id" "ex:s"
                           "schema:foo" {"@variable" "?d"
                                          "@type" "xsd:dateTime"}}]}
          {:keys [insert]} (parse/parse-update-txn txn {:object-var-parsing false})
          [_ _ o] (first insert)]
      (is (= '?d (where/get-variable o))
          "Should parse explicit @variable as the same bound var"))))

(deftest update-id-var-still-parses-when-flag-false
  (testing "@id as '?is-a-var' remains a variable, object '?not-a-var' becomes literal when objectVarParsing is false"
    (let [txn {"@context" ctx
               "where"   [{"@id" "?is-a-var"
                           "schema:text" "?not-a-var"}]
               "insert"  [{"@id" "?is-a-var"
                           "schema:text" "?not-a-var"}]}
          {:keys [insert]} (parse/parse-update-txn txn {:object-var-parsing false})
          [s _ o] (first insert)]
      (is (= '?is-a-var (where/get-variable s))
          "Subject should be variable ?is-a-var")
      (is (= "?not-a-var" (where/get-value o))
          "Object should be literal string '?not-a-var'"))))

(deftest update-predicate-var-still-parses-when-flag-false
  (testing "Predicate '?is-a-var' remains a variable, object '?not-a-var' becomes literal when objectVarParsing is false"
    (let [txn {"@context" ctx
               "where"   [{"@id" "ex:s"
                           "?is-a-var" "?not-a-var"}]
               "insert"  [{"@id" "ex:s"
                           "?is-a-var" "?not-a-var"}]}
          {:keys [insert]} (parse/parse-update-txn txn {:object-var-parsing false})
          [_ p o] (first insert)]
      (is (= '?is-a-var (where/get-variable p))
          "Predicate should be variable ?is-a-var")
      (is (= "?not-a-var" (where/get-value o))
          "Object should be literal string '?not-a-var'"))))

(deftest ^:integration insert-literal-qmark-string-has-xsd-string-type
  (testing "insert! with bare '?not-a-var' stores as xsd:string and value '?not-a-var'"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "tx/obj-var-insert")
          db1 @(fluree/insert! conn "tx/obj-var-insert"
                               {"@context" {"ex" "http://example.org/ns/"}
                                "@graph"   [{"@id" "ex:s"
                                             "ex:prop" "?not-a-var"}]})
          results @(fluree/query db1
                                 {"@context" {"ex" "http://example.org/ns/"
                                              "xsd" "http://www.w3.org/2001/XMLSchema#"}
                                  "select"   ["?val" "?dt"]
                                  "where"    [{"@id" "ex:s"
                                               "ex:prop" {"@value" "?val"
                                                          "@type" "?dt"}}]})]
      (is (= [["?not-a-var" "xsd:string"]]
             results)
          "Inserted bare '?not-a-var' should be value '?not-a-var' with xsd:string"))))

(deftest ^:integration upsert-literal-qmark-string-has-xsd-string-type
  (testing "upsert! with bare '?not-a-var' stores as xsd:string and value '?not-a-var'"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "tx/obj-var-upsert")
          _dbi @(fluree/insert! conn "tx/obj-var-upsert"
                                {"@context" {"ex" "http://example.org/ns/"}
                                 "@graph"   [{"@id" "ex:s"
                                              "ex:prop" "String val to be replaced"}]})
          db1 @(fluree/upsert! conn "tx/obj-var-upsert"
                               {"@context" {"ex" "http://example.org/ns/"}
                                "@graph"   [{"@id" "ex:s"
                                             "ex:prop" "?not-a-var"}]})
          results @(fluree/query db1
                                 {"@context" {"ex" "http://example.org/ns/"
                                              "xsd" "http://www.w3.org/2001/XMLSchema#"}
                                  "select"   ["?val" "?dt"]
                                  "where"    [{"@id" "ex:s"
                                               "ex:prop" {"@value" "?val"
                                                          "@type" "?dt"}}]})]
      (is (= [["?not-a-var" "xsd:string"]]
             results)
          "Upserted bare '?not-a-var' should be value '?not-a-var' with xsd:string"))))

(deftest query-literal-qmark-string-with-flag-false-requires-literal-match
  (testing "With objectVarParsing false, where with bare '?not-a-var' matches literal value"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "tx/obj-var-query-literal")
          db1 @(fluree/insert! conn "tx/obj-var-query-literal"
                               {"@context" {"ex" "http://example.org/ns/"}
                                "@graph"   [{"@id" "ex:s"
                                             "ex:prop" "?not-a-var"}]})
          results @(fluree/query db1
                                 {"@context" {"ex" "http://example.org/ns/"}
                                  "opts"     {"objectVarParsing" false}
                                  "select"   ["?s"]
                                  "where"    [{"@id" "?s"
                                               "ex:prop" "?not-a-var"}]})]
      (is (= [["ex:s"]]
             results)
          "Where should treat '?not-a-var' as a literal and match the record"))))

(deftest query-explicit-variable-in-where-still-parses-when-flag-false
  (testing "With objectVarParsing false, where with {'@variable': '?v'} binds a variable"
    (let [conn   (test-utils/create-conn)
          db0 @(fluree/create conn "tx/obj-var-query-var")
          db1 @(fluree/insert! conn "tx/obj-var-query-var"
                               {"@context" {"ex" "http://example.org/ns/"}
                                "@graph"   [{"@id" "ex:s"
                                             "ex:prop" "?not-a-var"}]})
          results @(fluree/query db1
                                 {"@context" {"ex" "http://example.org/ns/"}
                                  "opts"     {"objectVarParsing" false}
                                  "select"   ["?v"]
                                  "where"    [{"@id" "ex:s"
                                               "ex:prop" {"@variable" "?v"}}]})]
      (is (= [["?not-a-var"]]
             results)
          "Explicit @variable should bind and return the literal value"))))

(deftest ^:integration update-literal-qmark-string-where-binds-and-updates
  (testing "update! with objectVarParsing false matches literal '?not-a-var' in where, binds subject, and adds new property"
    (let [conn   (test-utils/create-conn)
          _db0 @(fluree/create conn "tx/obj-var-update")
          _dbi @(fluree/insert! conn "tx/obj-var-update"
                                {"@context" {"ex" "http://example.org/ns/"}
                                 "@graph"   [{"@id" "ex:s"
                                              "ex:prop" "?not-a-var"}]})
          db2  @(fluree/update! conn "tx/obj-var-update"
                                {"@context" {"ex" "http://example.org/ns/"}
                                 "where"   [{"@id" "?s"
                                             "ex:prop" "?not-a-var"}]
                                 "insert"  [{"@id" "?s"
                                             "ex:newProp" "new"}]}
                                {:object-var-parsing false})
          results @(fluree/query db2
                                 {"@context" {"ex" "http://example.org/ns/"}
                                  "select"   {"ex:s" ["*"]}
                                  "where"    [{"@id" "ex:s"}]})]
      (is (= [{"@id" "ex:s"
               "ex:prop" "?not-a-var"
               "ex:newProp" "new"}]
             results)
          "Subject should retain literal ex:prop and include new ex:newProp"))))
