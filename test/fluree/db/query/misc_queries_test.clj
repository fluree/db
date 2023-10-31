(ns fluree.db.query.misc-queries-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]))

(deftest ^:integration select-sid
  (testing "Select index's subject id in query using special keyword"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subid" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage
                    (fluree/db ledger)
                    {:graph [{:id          :ex/alice,
                              :type        :ex/User,
                              :schema/name "Alice"}
                             {:id           :ex/bob,
                              :type         :ex/User,
                              :schema/name  "Bob"
                              :ex/favArtist {:id          :ex/picasso
                                             :schema/name "Picasso"}}]})]
      (is (= [{:_id          211106232532993,
               :id           :ex/bob,
               :type     :ex/User,
               :schema/name  "Bob",
               :ex/favArtist {:_id         211106232532994
                              :schema/name "Picasso"}}
              {:_id         211106232532992,
               :id          :ex/alice,
               :type    :ex/User,
               :schema/name "Alice"}]
             @(fluree/query db {:select {'?s [:_id :* {:ex/favArtist [:_id :schema/name]}]}
                                :where  {:id '?s, :type :ex/User}}))))))

(deftest ^:integration result-formatting
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query-context" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage (fluree/db ledger) [{:id :ex/dan :ex/x 1}
                                                  {:id :ex/wes :ex/x 2}])]

    @(fluree/commit! ledger db)

    (testing "current query"
      (is (= [{:id   :ex/dan
               :ex/x 1}]
             @(fluree/query db {:select {:ex/dan [:*]}}))
          "default context")
      (is (= [{:id    :foo/dan
               :foo/x 1}]
             @(fluree/query db {"@context" ["" {:foo "http://example.org/ns/"}]
                                :select    {:foo/dan [:*]}}))
          "default unwrapped objects")
      (is (= [{:id    :foo/dan
               :foo/x [1]}]
             @(fluree/query db {"@context" ["" {:foo   "http://example.org/ns/"
                                                :foo/x {:container :set}}]
                                :select    {:foo/dan [:*]}}))
          "override unwrapping with :set")
      (is (= [{:id     :ex/dan
               "foo:x" [1]}]
             @(fluree/query db {"@context" ["" {"foo"   "http://example.org/ns/"
                                                "foo:x" {"@container" "@list"}}]
                                :select    {"foo:dan" ["*"]}}))
          "override unwrapping with @list")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" nil
                                :select    {"http://example.org/ns/dan" ["*"]}}))
          "clear context with nil")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" {}
                                :select    {"http://example.org/ns/dan" ["*"]}}))
          "clear context with empty context")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" []
                                :select    {"http://example.org/ns/dan" ["*"]}}))
          "clear context with empty context vector"))
    (testing "history query"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x 1}]
               :f/retract []}]
             @(fluree/history ledger {:history :ex/dan :t {:from 1}}))
          "default context")
      (is (= [{"https://ns.flur.ee/ledger#t"       1
               "https://ns.flur.ee/ledger#assert"
               [{"@id"                     "http://example.org/ns/dan"
                 :id                       "http://example.org/ns/dan"
                 "http://example.org/ns/x" 1}]
               "https://ns.flur.ee/ledger#retract" []}]
             @(fluree/history ledger {"@context" nil
                                      :history   "http://example.org/ns/dan"
                                      :t         {:from 1}}))
          "clear context on history query"))))

(deftest ^:integration s+p+o-full-db-queries
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/everything" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage
                    (fluree/db ledger)
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
                              :schema/age   30}]})]
      (testing "Query that pulls entire database."
        (is (= [[:ex/jane :id "http://example.org/ns/jane"]
                [:ex/jane :type :ex/User]
                [:ex/jane :schema/name "Jane"]
                [:ex/jane :schema/email "jane@flur.ee"]
                [:ex/jane :schema/age 30]
                [:ex/bob :id "http://example.org/ns/bob"]
                [:ex/bob :type :ex/User]
                [:ex/bob :schema/name "Bob"]
                [:ex/bob :schema/age 22]
                [:ex/alice :id "http://example.org/ns/alice"]
                [:ex/alice :type :ex/User]
                [:ex/alice :schema/name "Alice"]
                [:ex/alice :schema/email "alice@flur.ee"]
                [:ex/alice :schema/age 42]
                [:schema/age :id "http://schema.org/age"]
                [:schema/email :id "http://schema.org/email"]
                [:schema/name :id "http://schema.org/name"]
                [:ex/User :id "http://example.org/ns/User"]
                [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
                [:type :id "@type"]
                [:id :id "@id"]]
               @(fluree/query db {:select ['?s '?p '?o]
                                  :where  {:id '?s
                                           '?p '?o}}))
            "Entire database should be pulled.")
        (is (= [{:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob",
                 :schema/age  22}
                {:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob",
                 :schema/age  22}
                {:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob",
                 :schema/age  22}
                {:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob",
                 :schema/age  22}
                {:id           :ex/alice,
                 :type         :ex/User,
                 :schema/name  "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age   42}
                {:id           :ex/alice,
                 :type         :ex/User,
                 :schema/name  "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age   42}
                {:id           :ex/alice,
                 :type         :ex/User,
                 :schema/name  "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age   42}
                {:id           :ex/alice,
                 :type         :ex/User,
                 :schema/name  "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age   42}
                {:id           :ex/alice,
                 :type         :ex/User,
                 :schema/name  "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age   42}
                {:id :schema/age}
                {:id :schema/email}
                {:id :schema/name}
                {:id :ex/User}
                {:id :rdfs/Class}
                {:id :type}
                {:id :id}]
               @(fluree/query db {:select {'?s ["*"]}
                                  :where  {:id '?s, '?p '?o}}))
            "Every triple should be returned.")
        (let [db*    @(fluree/commit! ledger db)
              result @(fluree/query db* {:select ['?s '?p '?o]
                                         :where  {:id '?s, '?p '?o}})]
          (is (pred-match?
               [[:ex/jane :id "http://example.org/ns/jane"]
                [:ex/jane :type :ex/User]
                [:ex/jane :schema/name "Jane"]
                [:ex/jane :schema/email "jane@flur.ee"]
                [:ex/jane :schema/age 30]
                [:ex/bob :id "http://example.org/ns/bob"]
                [:ex/bob :type :ex/User]
                [:ex/bob :schema/name "Bob"]
                [:ex/bob :schema/age 22]
                [:ex/alice :id "http://example.org/ns/alice"]
                [:ex/alice :type :ex/User]
                [:ex/alice :schema/name "Alice"]
                [:ex/alice :schema/email "alice@flur.ee"]
                [:ex/alice :schema/age 42]
                [test-utils/context-id? :id test-utils/context-id?]
                [test-utils/context-id? :f/address test-utils/address?]
                [test-utils/did? :id test-utils/did?]
                [test-utils/db-id? :id test-utils/db-id?]
                [test-utils/db-id? :f/address test-utils/address?]
                [test-utils/db-id? :f/flakes 24]
                [test-utils/db-id? :f/size 1670]
                [test-utils/db-id? :f/t 1]
                [:schema/age :id "http://schema.org/age"]
                [:schema/email :id "http://schema.org/email"]
                [:schema/name :id "http://schema.org/name"]
                [:ex/User :id "http://example.org/ns/User"]
                [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
                [:type :id "@type"]
                [:f/t :id "https://ns.flur.ee/ledger#t"]
                [:f/size :id "https://ns.flur.ee/ledger#size"]
                [:f/flakes :id "https://ns.flur.ee/ledger#flakes"]
                [:f/defaultContext :id "https://ns.flur.ee/ledger#defaultContext"]
                [:f/branch :id "https://ns.flur.ee/ledger#branch"]
                [:f/alias :id "https://ns.flur.ee/ledger#alias"]
                [:f/data :id "https://ns.flur.ee/ledger#data"]
                [:f/address :id "https://ns.flur.ee/ledger#address"]
                [:f/v :id "https://ns.flur.ee/ledger#v"]
                ["https://www.w3.org/2018/credentials#issuer" :id "https://www.w3.org/2018/credentials#issuer"]
                [:f/time :id "https://ns.flur.ee/ledger#time"]
                [:f/message :id "https://ns.flur.ee/ledger#message"]
                [:f/previous :id "https://ns.flur.ee/ledger#previous"]
                [:id :id "@id"]
                [test-utils/commit-id? :id test-utils/commit-id?]
                [test-utils/commit-id? :f/time 720000]
                [test-utils/commit-id? "https://www.w3.org/2018/credentials#issuer" test-utils/did?]
                [test-utils/commit-id? :f/v 0]
                [test-utils/commit-id? :f/address test-utils/address?]
                [test-utils/commit-id? :f/data test-utils/db-id?]
                [test-utils/commit-id? :f/alias "query/everything"]
                [test-utils/commit-id? :f/branch "main"]
                [test-utils/commit-id? :f/defaultContext test-utils/context-id?]]
               result)
              (str "query result was: " (pr-str result))))))))

(deftest ^:integration illegal-reference-test
  (testing "Illegal reference queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with non-string objects"
        (let [test-subject @(fluree/query db {:select ['?s '?p]
                                              :where {:id '?s, '?p 22}})]
          (is (util/exception? test-subject)
              "return errors")
          (is (= :db/invalid-query
                 (-> test-subject ex-data :error))
              "have 'invalid query' error codes")))
      (testing "with string objects"
        (let [test-subject @(fluree/query db {:select ['?s '?p]
                                              :where {:id '?s, '?p "Bob"}})]
          (is (util/exception? test-subject)
              "return errors")
          (is (= :db/invalid-query
                 (-> test-subject ex-data :error))
              "have 'invalid query' error codes"))))))

(deftest ^:integration class-queries
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/class" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
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
                    :schema/name "Dave"}])]
    (testing "type"
      (is (= [[:ex/User]]
             @(fluree/query db '{:select [?class]
                                 :where  {:id :ex/jane, :type ?class}})))
      (is (= [[:ex/jane :ex/User]
              [:ex/bob :ex/User]
              [:ex/alice :ex/User]
              [:ex/dave :ex/nonUser]]
             @(fluree/query db '{:select [?s ?class]
                                 :where  {:id ?s, :type ?class}}))))
    (testing "shacl targetClass"
      (let [shacl-db @(fluree/stage
                        (fluree/db ledger)
                        {:context        {:ex "http://example.org/ns/"}
                         :id             :ex/UserShape,
                         :type           [:sh/NodeShape],
                         :sh/targetClass :ex/User
                         :sh/property    [{:sh/path     :schema/name
                                           :sh/datatype :xsd/string}]})]
        (is (= [[:ex/User]]
               @(fluree/query shacl-db '{:select [?class]
                                         :where  {:id :ex/UserShape, :sh/targetClass ?class}})))))))

(deftest ^:integration type-handling
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "type-handling" {:defaultContext [test-utils/default-str-context {"ex" "http://example.org/ns/"}]})
        db0 (fluree/db ledger)
        db1 @(fluree/stage db0 [{"id" "ex:ace"
                                 "type" "ex:Spade"}
                                {"id" "ex:king"
                                 "type" "ex:Heart"}
                                {"id" "ex:queen"
                                 "type" "ex:Heart"}
                                {"id" "ex:jack"
                                 "type" "ex:Club"}])
        db2 @(fluree/stage db1 [{"id" "ex:two"
                                 "rdf:type" "ex:Diamond"}])
        db3 @(fluree/stage db1 {"@context" ["" {"rdf:type" "@type"}]
                                "id" "ex:two"
                                "rdf:type" "ex:Diamond"})]
    (is (= [{"id" "ex:queen" "type" "ex:Heart"}
            {"id" "ex:king" "type" "ex:Heart"}]
           @(fluree/query db1 {"select" {"?s" ["*"]}
                               "where" {"id" "?s", "type" "ex:Heart"}}))
        "Query with type and type in results")
    (is (= [{"id" "ex:queen" "type" "ex:Heart"}
            {"id" "ex:king" "type" "ex:Heart"}]
           @(fluree/query db1 {"select" {"?s" ["*"]}
                               "where" {"id" "?s", "rdf:type" "ex:Heart"}}))
        "Query with rdf:type and type in results")

    (is (util/exception? db2)
        "Cannot transact with rdf:type predicate")
    (is (= "\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead."
           (-> db2 Throwable->map :cause)))

    (is (= [{"id" "ex:two" "type" "ex:Diamond"}]
           @(fluree/query db3 {"select" {"?s" ["*"]}
                               "where" {"id" "?s", "type" "ex:Diamond"}}))
        "Can transact with rdf:type aliased to type.")))
