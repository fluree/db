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
                                :where  [['?s :type :ex/User]]}))))))

(deftest ^:integration result-formatting
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query-context" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage (fluree/db ledger) [{:id :ex/dan :ex/x 1}
                                                  {:id :ex/wes :ex/x 2}])]

    @(fluree/commit! ledger db)

    (testing "current query"
      (is (= [{:id   :ex/dan
               :ex/x 1}]
             @(fluree/query db {:where  [["?s" :id :ex/dan]]
                                :select {"?s" [:*]}}))
          "default context")
      (is (= [{:id    :foo/dan
               :foo/x 1}]
             @(fluree/query db {"@context" ["" {:foo "http://example.org/ns/"}]
                                :where     [["?s" :id :foo/dan]]
                                :select    {"?s" [:*]}}))
          "default unwrapped objects")
      (is (= [{:id    :foo/dan
               :foo/x [1]}]
             @(fluree/query db {"@context" ["" {:foo   "http://example.org/ns/"
                                                :foo/x {:container :set}}]
                                :where     [["?s" :id :foo/dan]]
                                :select    {"?s" [:*]}}))
          "override unwrapping with :set")
      (is (= [{:id     :ex/dan
               "foo:x" [1]}]
             @(fluree/query db {"@context" ["" {"foo"   "http://example.org/ns/"
                                                "foo:x" {"@container" "@list"}}]
                                :where     [["?s" "@id" "foo:dan"]]
                                :select    {"?s" ["*"]}}))
          "override unwrapping with @list")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" nil
                                :where     [["?s" "@id" "http://example.org/ns/dan"]]
                                :select    {"?s" ["*"]}}))
          "clear context with nil")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" {}
                                :where     [["?s" "@id" "http://example.org/ns/dan"]]
                                :select    {"?s" ["*"]}}))
          "clear context with empty context")
      (is (= [{"@id"                     "http://example.org/ns/dan"
               "http://example.org/ns/x" 1}]
             @(fluree/query db {"@context" []
                                :where     [["?s" "@id" "http://example.org/ns/dan"]]
                                :select    {"?s" ["*"]}}))
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
        (is (= [[{:id :ex/jane} {:id :id} "http://example.org/ns/jane"]
                [{:id :ex/jane} {:id :type} {:id :ex/User}]
                [{:id :ex/jane} {:id :schema/name} "Jane"]
                [{:id :ex/jane} {:id :schema/email} "jane@flur.ee"]
                [{:id :ex/jane} {:id :schema/age} 30]
                [{:id :ex/bob} {:id :id} "http://example.org/ns/bob"]
                [{:id :ex/bob} {:id :type} {:id :ex/User}]
                [{:id :ex/bob} {:id :schema/name} "Bob"]
                [{:id :ex/bob} {:id :schema/age} 22]
                [{:id :ex/alice} {:id :id} "http://example.org/ns/alice"]
                [{:id :ex/alice} {:id :type} {:id :ex/User}]
                [{:id :ex/alice} {:id :schema/name} "Alice"]
                [{:id :ex/alice} {:id :schema/email} "alice@flur.ee"]
                [{:id :ex/alice} {:id :schema/age} 42]
                [{:id :schema/age} {:id :id} "http://schema.org/age"]
                [{:id :schema/email} {:id :id} "http://schema.org/email"]
                [{:id :schema/name} {:id :id} "http://schema.org/name"]
                [{:id :ex/User} {:id :id} "http://example.org/ns/User"]
                [{:id :rdfs/Class} {:id :id} "http://www.w3.org/2000/01/rdf-schema#Class"]
                [{:id :type} {:id :id} "@type"]
                [{:id :id} {:id :id} "@id"]]
               @(fluree/query db {:select ['?s '?p '?o]
                                  :where  [['?s '?p '?o]]}))
            "Entire database should be pulled.")
        (is (= [{:id :ex/jane,
                 :type :ex/User,
                 :schema/name "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age 30}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/name "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age 30}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/name "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age 30}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/name "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age 30}
                {:id :ex/jane,
                 :type :ex/User,
                 :schema/name "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age 30}
                {:id :ex/bob,
                 :type :ex/User,
                 :schema/name "Bob",
                 :schema/age 22}
                {:id :ex/bob,
                 :type :ex/User,
                 :schema/name "Bob",
                 :schema/age 22}
                {:id :ex/bob,
                 :type :ex/User,
                 :schema/name "Bob",
                 :schema/age 22}
                {:id :ex/bob,
                 :type :ex/User,
                 :schema/name "Bob",
                 :schema/age 22}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age 42}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age 42}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age 42}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age 42}
                {:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@flur.ee",
                 :schema/age 42}
                {:id :schema/age}
                {:id :schema/email}
                {:id :schema/name}
                {:id :ex/User}
                {:id :rdfs/Class}
                {:id :type}
                {:id :id}]
               @(fluree/query db {:select {'?s ["*"]}
                                  :where  [['?s '?p '?o]]}))
            "Every triple should be returned.")
        (let [db*    @(fluree/commit! ledger db)
              result @(fluree/query db* {:select ['?s '?p '?o]
                                         :where  [['?s '?p '?o]]})]
          (is (pred-match?
                [[{:id :ex/jane} {:id :id} "http://example.org/ns/jane"]
                 [{:id :ex/jane} {:id :type} {:id :ex/User}]
                 [{:id :ex/jane} {:id :schema/name} "Jane"]
                 [{:id :ex/jane} {:id :schema/email} "jane@flur.ee"]
                 [{:id :ex/jane} {:id :schema/age} 30]
                 [{:id :ex/bob} {:id :id} "http://example.org/ns/bob"]
                 [{:id :ex/bob} {:id :type} {:id :ex/User}]
                 [{:id :ex/bob} {:id :schema/name} "Bob"]
                 [{:id :ex/bob} {:id :schema/age} 22]
                 [{:id :ex/alice} {:id :id} "http://example.org/ns/alice"]
                 [{:id :ex/alice} {:id :type} {:id :ex/User}]
                 [{:id :ex/alice} {:id :schema/name} "Alice"]
                 [{:id :ex/alice} {:id :schema/email} "alice@flur.ee"]
                 [{:id :ex/alice} {:id :schema/age} 42]
                 [{:id test-utils/context-id?} {:id :id} test-utils/context-id?]
                 [{:id test-utils/context-id?} {:id :f/address} test-utils/address?]
                 [{:id test-utils/did?} {:id :id} test-utils/did?]
                 [{:id test-utils/db-id?} {:id :id} test-utils/db-id?]
                 [{:id test-utils/db-id?} {:id :f/address} test-utils/address?]
                 [{:id test-utils/db-id?} {:id :f/flakes} 24]
                 [{:id test-utils/db-id?} {:id :f/size} 1670]
                 [{:id test-utils/db-id?} {:id :f/t} 1]
                 [{:id :schema/age} {:id :id} "http://schema.org/age"]
                 [{:id :schema/email} {:id :id} "http://schema.org/email"]
                 [{:id :schema/name} {:id :id} "http://schema.org/name"]
                 [{:id :ex/User} {:id :id} "http://example.org/ns/User"]
                 [{:id :rdfs/Class} {:id :id} "http://www.w3.org/2000/01/rdf-schema#Class"]
                 [{:id :type} {:id :id} "@type"]
                 [{:id :f/t} {:id :id} "https://ns.flur.ee/ledger#t"]
                 [{:id :f/size} {:id :id} "https://ns.flur.ee/ledger#size"]
                 [{:id :f/flakes} {:id :id} "https://ns.flur.ee/ledger#flakes"]
                 [{:id :f/defaultContext} {:id :id} "https://ns.flur.ee/ledger#defaultContext"]
                 [{:id :f/branch} {:id :id} "https://ns.flur.ee/ledger#branch"]
                 [{:id :f/alias} {:id :id} "https://ns.flur.ee/ledger#alias"]
                 [{:id :f/data} {:id :id} "https://ns.flur.ee/ledger#data"]
                 [{:id :f/address} {:id :id} "https://ns.flur.ee/ledger#address"]
                 [{:id :f/v} {:id :id} "https://ns.flur.ee/ledger#v"]
                 [{:id "https://www.w3.org/2018/credentials#issuer"} {:id :id} "https://www.w3.org/2018/credentials#issuer"]
                 [{:id :f/time} {:id :id} "https://ns.flur.ee/ledger#time"]
                 [{:id :f/message} {:id :id} "https://ns.flur.ee/ledger#message"]
                 [{:id :f/previous} {:id :id} "https://ns.flur.ee/ledger#previous"]
                 [{:id :id} {:id :id} "@id"]
                 [{:id test-utils/commit-id?} {:id :id} test-utils/commit-id?]
                 [{:id test-utils/commit-id?} {:id :f/time} 720000]
                 [{:id test-utils/commit-id?} {:id "https://www.w3.org/2018/credentials#issuer"} {:id test-utils/did?}]
                 [{:id test-utils/commit-id?} {:id :f/v} 0]
                 [{:id test-utils/commit-id?} {:id :f/address} test-utils/address?]
                 [{:id test-utils/commit-id?} {:id :f/data} {:id test-utils/db-id?}]
                 [{:id test-utils/commit-id?} {:id :f/alias} "query/everything"]
                 [{:id test-utils/commit-id?} {:id :f/branch} "main"]
                 [{:id test-utils/commit-id?} {:id :f/defaultContext} {:id test-utils/context-id?}]]
               result)
              (str "query result was: " (pr-str result))))))))

(deftest ^:integration illegal-reference-test
  (testing "Illegal reference queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with non-string objects"
        (let [test-subject @(fluree/query db {:select ['?s '?p]
                                              :where [['?s '?p 22]]})]
          (is (util/exception? test-subject)
              "return errors")
          (is (= :db/invalid-query
                 (-> test-subject ex-data :error))
              "have 'invalid query' error codes")))
      (testing "with string objects"
        (let [test-subject @(fluree/query db {:select ['?s '?p]
                                              :where [['?s '?p "Bob"]]})]
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
      (is (= [[{:id :ex/User}]]
             @(fluree/query db '{:select [?class]
                                 :where  [[:ex/jane :type ?class]]})))
      (is (= [[{:id :ex/jane} {:id :ex/User}]
              [{:id :ex/bob} {:id :ex/User}]
              [{:id :ex/alice} {:id :ex/User}]
              [{:id :ex/dave} {:id :ex/nonUser}]]
             @(fluree/query db '{:select [?s ?class]
                                 :where  [[?s :type ?class]]}))))
    (testing "shacl targetClass"
      (let [shacl-db @(fluree/stage
                        (fluree/db ledger)
                        {:context        {:ex "http://example.org/ns/"}
                         :id             :ex/UserShape,
                         :type           [:sh/NodeShape],
                         :sh/targetClass :ex/User
                         :sh/property    [{:sh/path     :schema/name
                                           :sh/datatype :xsd/string}]})]
        (is (= [[{:id :ex/User}]]
               @(fluree/query shacl-db '{:select [?class]
                                         :where  [[:ex/UserShape :sh/targetClass ?class]]})))))))

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
                               "where" [["?s" "type" "ex:Heart"]]}))
        "Query with type and type in results")
    (is (= [{"id" "ex:queen" "type" "ex:Heart"}
            {"id" "ex:king" "type" "ex:Heart"}]
           @(fluree/query db1 {"select" {"?s" ["*"]}
                               "where" [["?s" "rdf:type" "ex:Heart"]]}))
        "Query with rdf:type and type in results")

    (is (util/exception? db2)
        "Cannot transact with rdf:type predicate")
    (is (= "\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead."
           (-> db2 Throwable->map :cause)))

    (is (= [{"id" "ex:two" "type" "ex:Diamond"}]
           @(fluree/query db3 {"select" {"?s" ["*"]}
                               "where" [["?s" "type" "ex:Diamond"]]}))
        "Can transact with rdf:type aliased to type.")))
