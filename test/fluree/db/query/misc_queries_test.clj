(ns fluree.db.query.misc-queries-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]
            [fluree.db.util.core :as util]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration result-formatting
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query-context")
        db     @(fluree/stage (fluree/db ledger) {"@context" ["https://ns.flur.ee"
                                                              test-utils/default-context
                                                              {:ex "http://example.org/ns/"}]
                                                  "insert"   [{:id :ex/dan :ex/x 1}
                                                              {:id :ex/wes :ex/x 2}]})]

    @(fluree/commit! ledger db)

    (testing "current query"
      (is (= [{:id   :ex/dan
               :ex/x 1}]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  {:ex/dan [:*]}}))
          "default context")
      (is (= [{:id    :foo/dan
               :foo/x 1}]
             @(fluree/query db {"@context" [test-utils/default-context
                                            {:ex "http://example.org/ns/"}
                                            {:foo "http://example.org/ns/"}]
                                :select    {:foo/dan [:*]}}))
          "default unwrapped objects")
      (is (= [{:id    :foo/dan
               :foo/x [1]}]
             @(fluree/query db {"@context" [[test-utils/default-context
                                             {:ex "http://example.org/ns/"}
                                             {:foo   "http://example.org/ns/"
                                              :foo/x {:container :set}}]]
                                :select    {:foo/dan [:*]}}))
          "override unwrapping with :set")
      (is (= [{:id     :ex/dan
               "foo:x" [1]}]
             @(fluree/query db {"@context" [test-utils/default-context
                                            {:ex "http://example.org/ns/"}
                                            {"foo"   "http://example.org/ns/"
                                             "foo:x" {"@container" "@list"}}]
                                :select    {"foo:dan" ["*"]}}))
          "override unwrapping with @list")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {:select {"http://example.org/ns/dan" ["*"]}}))
          "no context")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" {}
                                :select    {"http://example.org/ns/dan" ["*"]}}))
          "empty context")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" []
                                :select    {"http://example.org/ns/dan" ["*"]}}))
          "empty context vector"))
    (testing "history query"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x 1}]
               :f/retract []}]
             @(fluree/history ledger {:context [test-utils/default-context
                                                {:ex "http://example.org/ns/"}]
                                      :history :ex/dan :t {:from 1}}))
          "default context")
      (is (= [{"https://ns.flur.ee/ledger#t"       1
               "https://ns.flur.ee/ledger#assert"
               [{"@id"                     "http://example.org/ns/dan"
                 "http://example.org/ns/x" 1}]
               "https://ns.flur.ee/ledger#retract" []}]
             @(fluree/history ledger {"@context" nil
                                      :history   "http://example.org/ns/dan"
                                      :t         {:from 1}}))
          "nil context on history query"))))

(deftest ^:integration s+p+o-full-db-queries
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/everything")
          db     @(fluree/stage
                    (fluree/db ledger)
                    {"@context" ["https://ns.flur.ee"
                                 test-utils/default-context
                                 {:ex "http://example.org/ns/"}]
                     "insert"
                     {:graph [{:id           :ex/alice,
                               :type         :ex/User,
                               :schema/name  "Alice"
                               :schema/email "alice@flur.ee"
                               :schema/age   42}
                              {:id          :ex/bob,
                               :type        :ex/User,
                               :schema/name "Bob"
                               :schema/age  22}
                              {:id           :ex/jane,
                               :type         :ex/User,
                               :schema/name  "Jane"
                               :schema/email "jane@flur.ee"
                               :schema/age   30}]}})]
      (testing "Query that pulls entire database."
        (is (= [[:ex/alice :type :ex/User]
                [:ex/alice :schema/age 42]
                [:ex/alice :schema/email "alice@flur.ee"]
                [:ex/alice :schema/name "Alice"]
                [:ex/bob :type :ex/User]
                [:ex/bob :schema/age 22]
                [:ex/bob :schema/name "Bob"]
                [:ex/jane :type :ex/User]
                [:ex/jane :schema/age 30]
                [:ex/jane :schema/email "jane@flur.ee"]
                [:ex/jane :schema/name "Jane"]]
               @(fluree/query db {:context [test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                  :select  ['?s '?p '?o]
                                  :where   {:id '?s
                                            '?p '?o}}))
            "Entire database should be pulled.")
        (is (= [{:id :ex/alice,
                 :type :ex/User,
                 :schema/age 42,
                 :schema/email "alice@flur.ee",
                 :schema/name "Alice"}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/age 42,
                 :schema/email "alice@flur.ee",
                 :schema/name "Alice"}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/age 42,
                 :schema/email "alice@flur.ee",
                 :schema/name "Alice"}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/age 42,
                 :schema/email "alice@flur.ee",
                 :schema/name "Alice"}
                {:id :ex/bob, :type :ex/User, :schema/age 22, :schema/name "Bob"}
                {:id :ex/bob, :type :ex/User, :schema/age 22, :schema/name "Bob"}
                {:id :ex/bob, :type :ex/User, :schema/age 22, :schema/name "Bob"}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/age 30,
                 :schema/email "jane@flur.ee",
                 :schema/name "Jane"}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/age 30,
                 :schema/email "jane@flur.ee",
                 :schema/name "Jane"}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/age 30,
                 :schema/email "jane@flur.ee",
                 :schema/name "Jane"}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/age 30,
                 :schema/email "jane@flur.ee",
                 :schema/name "Jane"}]
               @(fluree/query db {:context [test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                  :select  {'?s ["*"]}
                                  :where   {:id '?s, '?p '?o}}))
            "Every triple should be returned.")
        (let [db*    @(fluree/commit! ledger db)
              result @(fluree/query db* {:context [test-utils/default-context
                                                   {:ex "http://example.org/ns/"}]
                                         :select  ['?s '?p '?o]
                                         :where   {:id '?s, '?p '?o}})]
          (is (= [["fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"
                   :f/address
                   "fluree:memory://1b2db88aa3b254e57ac5c1da9a4d5e5e091a67dd09fcc9bbeaf52093e9e9a3fe"]
                  ["fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"
                   :f/flakes
                   11]
                  ["fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"
                   :f/previous
                   "fluree:db:sha256:bbktr7nvywjwtiamogekl3gfy57di3c5b7vob4gth3dm7hwmtw5k4"]
                  ["fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"
                   :f/size
                   1266]
                  ["fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"
                   :f/t
                   1]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   "https://www.w3.org/2018/credentials#issuer"
                   "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/address
                   "fluree:memory://5acc78bb89fa8619ab186a8a5aaba2c08531c1fae18309d944bf406c6129ed34"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/alias
                   "query/everything"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/branch
                   "main"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/data
                   "fluree:db:sha256:bbzjpuyb7d4ah6yvqm677efrvjxyswtqglpj6pc6gwuuf3rdof3iy"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/previous
                   "fluree:commit:sha256:bb6dtkig73qu77wvwzpumlkmy2ftq3ikv2lhltti4eqvripnpqoqz"]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/time
                   720000]
                  ["fluree:commit:sha256:bfx4zqi3vysp2aiyxuycwjldw22l2mcoszjfvpdda76a5vedx4mi"
                   :f/v
                   1]
                  [:ex/alice :type :ex/User]
                  [:ex/alice :schema/age 42]
                  [:ex/alice :schema/email "alice@flur.ee"]
                  [:ex/alice :schema/name "Alice"]
                  [:ex/bob :type :ex/User]
                  [:ex/bob :schema/age 22]
                  [:ex/bob :schema/name "Bob"]
                  [:ex/jane :type :ex/User]
                  [:ex/jane :schema/age 30]
                  [:ex/jane :schema/email "jane@flur.ee"]
                  [:ex/jane :schema/name "Jane"]]
                 result)
              (str "query result was: " (pr-str result))))))))

(deftest ^:integration illegal-reference-test
  (testing "Illegal reference queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with non-string objects"
        (let [test-subject @(fluree/query db {:context [test-utils/default-context
                                                        {:ex "http://example.org/ns/"}]
                                              :select  ['?s '?p]
                                              :where   {:id '?s, '?p 22}})]
          (is (util/exception? test-subject)
              "return errors")
          (is (= :db/invalid-query
                 (-> test-subject ex-data :error))
              "have 'invalid query' error codes")))
      (testing "with string objects"
        (let [test-subject @(fluree/query db {:context [test-utils/default-context
                                                        {:ex "http://example.org/ns/"}]
                                              :select  ['?s '?p]
                                              :where   {:id '?s, '?p "Bob"}})]
          (is (util/exception? test-subject)
              "return errors")
          (is (= :db/invalid-query
                 (-> test-subject ex-data :error))
              "have 'invalid query' error codes"))))))

(deftest ^:integration class-queries
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/class")
        db     @(fluree/stage
                  (fluree/db ledger)
                  {"@context" ["https://ns.flur.ee"
                               test-utils/default-context
                               {:ex "http://example.org/ns/"}]
                   "insert"
                   [{:id           :ex/alice,
                     :type         :ex/User,
                     :schema/name  "Alice"
                     :schema/email "alice@flur.ee"
                     :schema/age   42}
                    {:id          :ex/bob,
                     :type        :ex/User,
                     :schema/name "Bob"
                     :schema/age  22}
                    {:id           :ex/jane,
                     :type         :ex/User,
                     :schema/name  "Jane"
                     :schema/email "jane@flur.ee"
                     :schema/age   30}
                    {:id          :ex/dave
                     :type        :ex/nonUser
                     :schema/name "Dave"}]})]
    (testing "type"
      (is (= [[:ex/User]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?class]
                                :where   '{:id :ex/jane, :type ?class}})))
      (is (= #{[:ex/jane :ex/User]
               [:ex/bob :ex/User]
               [:ex/alice :ex/User]
               [:ex/dave :ex/nonUser]}
             (set @(fluree/query db {:context [test-utils/default-context
                                               {:ex "http://example.org/ns/"}]
                                     :select  '[?s ?class]
                                     :where   '{:id ?s, :type ?class}})))))
    (testing "shacl targetClass"
      (let [shacl-db @(fluree/stage
                        (fluree/db ledger)
                        {"@context" ["https://ns.flur.ee"
                                     test-utils/default-context
                                     {:ex "http://example.org/ns/"}]
                         "insert"
                         {:id             :ex/UserShape,
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :schema/name
                                            :sh/datatype :xsd/string}]}})]
        (is (= [[:ex/User]]
               @(fluree/query shacl-db {:context [test-utils/default-context
                                                  {:ex "http://example.org/ns/"}]
                                        :select  '[?class]
                                        :where   '{:id :ex/UserShape, :sh/targetClass ?class}})))))))

(deftest ^:integration type-handling
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "type-handling")
        db0    (fluree/db ledger)
        db1    @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "http://example.org/ns/"}]
                                   "insert"   [{"id"   "ex:ace"
                                                "type" "ex:Spade"}
                                               {"id"   "ex:king"
                                                "type" "ex:Heart"}
                                               {"id"   "ex:queen"
                                                "type" "ex:Heart"}
                                               {"id"   "ex:jack"
                                                "type" "ex:Club"}]})
        db2    @(fluree/stage db1 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "http://example.org/ns/"}]
                                   "insert"   [{"id"       "ex:two"
                                                "rdf:type" "ex:Diamond"}]})
        db3    @(fluree/stage db1 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "http://example.org/ns/"}
                                               {"rdf:type" "@type"}]
                                   "insert"   {"id"       "ex:two"
                                               "rdf:type" "ex:Diamond"}})]
    (is (= #{{"id" "ex:queen" "type" "ex:Heart"}
             {"id" "ex:king" "type" "ex:Heart"}}
           (set @(fluree/query db1 {"@context" [test-utils/default-str-context
                                                {"ex" "http://example.org/ns/"}]
                                    "select"   {"?s" ["*"]}
                                    "where"    {"id" "?s", "type" "ex:Heart"}})))
        "Query with type and type in results")
    (is (= #{{"id" "ex:queen" "type" "ex:Heart"}
             {"id" "ex:king" "type" "ex:Heart"}}
           (set @(fluree/query db1 {"@context" [test-utils/default-str-context
                                                {"ex" "http://example.org/ns/"}]
                                    "select"   {"?s" ["*"]}
                                    "where"    {"id" "?s", "rdf:type" "ex:Heart"}})))
        "Query with rdf:type and type in results")
    (is (= "\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead."
           (-> db2 Throwable->map :cause)))

    (is (= [{"id" "ex:two" "type" "ex:Diamond"}]
           @(fluree/query db3 {"@context" [test-utils/default-str-context
                                           {"ex" "http://example.org/ns/"}]
                               "select"   {"?s" ["*"]}
                               "where"    {"id" "?s", "type" "ex:Diamond"}}))
        "Can transact with rdf:type aliased to type.")))

(deftest ^:integration load-with-new-connection
  (with-tmp-dir storage-path
    (let [conn0     @(fluree/connect-file {:storage-path storage-path})
          ledger-id "new3"
          ledger    @(fluree/create-with-txn conn0 {"@context" {"ex" {"ex" "http://example.org/ns/"}}
                                                    "ledger"   ledger-id
                                                    "insert"   {"ex:createdAt" "now"}})

          conn1 @(fluree/connect-file {:storage-path storage-path})]
      (is (= [{"ex:createdAt" "now"}]
             @(fluree/query-connection conn1 {"@context" {"ex" {"ex" "http://example.org/ns/"}}
                                              :from      ledger-id
                                              :where     {"@id" "?s" "ex:createdAt" "now"},
                                              :select    {"?s" ["ex:createdAt"]}}))))))

(deftest ^:integration repeated-transaction-results
  (testing "duplicate flakes with different t values"
    (let [conn   @(fluree/connect-memory)
          ledger @(fluree/create conn "dup-flakes")
          db0    (fluree/db ledger)
          tx     {"insert" {"@id" "ex:1" "ex:foo" 30}}
          db1    @(fluree/stage db0 tx)
          ;; advance the `t`
          db2    @(fluree/commit! ledger db1)
          db3    @(fluree/stage db2 tx)]
      (testing "do not become multicardinal result values"
        (is (= [{"ex:foo" 30, "@id" "ex:1"}]
               @(fluree/query db3 {"select" {"ex:1" ["*"]}})))))))

(deftest ^:integration base-context
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "base")
        db0 (fluree/db ledger)
        db1 @(fluree/stage db0 {"@context" {"@base" "https://flur.ee/" "ex" "http://example.com/"}
                                "insert" [{"@id" "freddy" "@type" "Yeti" "name" "Freddy"}
                                          {"@id" "ex:betty" "@type" "Yeti" "name" "Betty"}]})]
    (is (= [["name" "Freddy"]
            ["@type" "Yeti"]]
           @(fluree/query db1 {"@context" {"@base" "https://flur.ee/"}
                               "where" [{"@id" "freddy" "?p" "?o"}]
                               "select" ["?p" "?o"]})))
    (is (= [{"@id" "freddy"
             "@type" "Yeti"
             "name" "Freddy"}]
           @(fluree/query db1 {"@context" {"@base" "https://flur.ee/"}
                               "select" {"freddy" ["*"]}})))))
