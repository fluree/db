(ns fluree.db.query.misc-queries-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]))

(deftest ^:integration select-sid
  (testing "Select index's subject id in query using special keyword"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subid" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage2
                    (fluree/db ledger)
                    {"@context" "https://ns.flur.ee"
                     "insert"
                     {:graph [{:id          :ex/alice,
                               :type        :ex/User,
                               :schema/name "Alice"}
                              {:id           :ex/bob,
                               :type         :ex/User,
                               :schema/name  "Bob"
                               :ex/favArtist {:id          :ex/picasso
                                              :schema/name "Picasso"}}]}})]
      (is (->> @(fluree/query db {:select {'?s [:_id {:ex/favArtist [:_id ]}]}
                                  :where  {:id '?s, :type :ex/User}})
               (reduce (fn [sids {:keys [_id] :as node}]
                         (cond-> (conj sids _id)
                           (:ex/favArtist node) (conj (:_id (:ex/favArtist node)))))
                       [])
               (every? int?))))))

(deftest ^:integration result-formatting
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query-context" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage2 (fluree/db ledger) {"@context" "https://ns.flur.ee"
                                                   "insert" [{:id :ex/dan :ex/x 1}
                                                             {:id :ex/wes :ex/x 2}]})]

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
          db     @(fluree/stage2
                    (fluree/db ledger)
                    {"@context" "https://ns.flur.ee"
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
        (is (= #{[:ex/jane :id "http://example.org/ns/jane"]
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
                 #_[:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
                 [:type :id "@type"]
                 [:id :id "@id"]}
               (set @(fluree/query db {:select ['?s '?p '?o]
                                       :where  {:id '?s
                                                '?p '?o}})))
            "Entire database should be pulled.")
        (is (= [{:id :id}
                {:id :type}
                {:id :ex/User}
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
                {:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane",
                 :schema/email "jane@flur.ee",
                 :schema/age   30}
                {:id :schema/age}
                {:id :schema/email}
                {:id :schema/name}]
               (sort-by :id @(fluree/query db {:select {'?s ["*"]}
                                       :where  {:id '?s, '?p '?o}})))
            "Every triple should be returned.")
        (let [db*    @(fluree/commit! ledger db)
              result @(fluree/query db* {:select ['?s '?p '?o]
                                         :where  {:id '?s, '?p '?o}})]
          (is (= #{[:ex/jane :id "http://example.org/ns/jane"]
                   [:ex/jane :type :ex/User]
                   [:ex/jane :schema/age 30]
                   [:ex/jane :schema/name "Jane"]
                   [:ex/jane :schema/email "jane@flur.ee"]
                   [:ex/bob :id "http://example.org/ns/bob"]
                   [:ex/bob :type :ex/User]
                   [:ex/bob :schema/age 22]
                   [:ex/bob :schema/name "Bob"]
                   [:ex/User :id "http://example.org/ns/User"]
                   [:ex/alice :id "http://example.org/ns/alice"]
                   [:ex/alice :type :ex/User]
                   [:ex/alice :schema/age 42]
                   [:ex/alice :schema/name "Alice"]
                   [:ex/alice :schema/email "alice@flur.ee"]
                   ["fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
                    :id
                    "fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"]
                   ["fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"
                    :f/address
                    "fluree:memory://68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"]
                   ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                    :id
                    "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                   ["fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
                    :id
                    "fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"]
                   ["fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
                    :f/address
                    "fluree:memory://fb15dfb3f737fca3d90e62cbd9d6ced78c16194b40e58bea2e60c4205ea5300d"]
                   ["fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
                    :f/flakes
                    20]
                   ["fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
                    :f/size
                    1318]
                   ["fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"
                    :f/t
                    1]
                   [:schema/email :id "http://schema.org/email"]
                   [:schema/name :id "http://schema.org/name"]
                   [:schema/age :id "http://schema.org/age"]
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
                   ["https://www.w3.org/2018/credentials#issuer"
                    :id
                    "https://www.w3.org/2018/credentials#issuer"]
                   [:f/time :id "https://ns.flur.ee/ledger#time"]
                   [:f/message :id "https://ns.flur.ee/ledger#message"]
                   [:f/previous :id "https://ns.flur.ee/ledger#previous"]
                   [:id :id "@id"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :id
                    "fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/time
                    720000]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    "https://www.w3.org/2018/credentials#issuer"
                    "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/v
                    0]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/address
                    "fluree:memory://308132a22e9a9c18a42718cf6be5b6fd031af3f79adb703b34b0148d389d9591"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/data
                    "fluree:db:sha256:bbgtaymrau2iz3mcdpaifv6liqrvygzl7q57vvgqvifhdpgqyvxkz"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/alias
                    "query/everything"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/branch
                    "main"]
                   ["fluree:commit:sha256:bbmsdo3ljxjmhcjnfvr2pb4yshdgfgorsckbdc3bysnairyru4jb5"
                    :f/defaultContext
                    "fluree:context:68845db506ec672e8481d6d8bce580cd24067e1010d36f869e8643752df0ae35"]}
                 (set result))
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
        db     @(fluree/stage2
                  (fluree/db ledger)
                  {"@context" "https://ns.flur.ee"
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
             @(fluree/query db '{:select [?class]
                                 :where  {:id :ex/jane, :type ?class}})))
      (is (= [[:ex/jane :ex/User]
              [:ex/bob :ex/User]
              [:ex/alice :ex/User]
              [:ex/dave :ex/nonUser]]
             @(fluree/query db '{:select [?s ?class]
                                 :where  {:id ?s, :type ?class}}))))
    (testing "shacl targetClass"
      (let [shacl-db @(fluree/stage2
                        (fluree/db ledger)
                        {"@context" "https://ns.flur.ee"
                         "insert"
                         {:context        {:ex "http://example.org/ns/"}
                          :id             :ex/UserShape,
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :schema/name
                                            :sh/datatype :xsd/string}]}})]
        (is (= [[:ex/User]]
               @(fluree/query shacl-db '{:select [?class]
                                         :where  {:id :ex/UserShape, :sh/targetClass ?class}})))))))

(deftest ^:integration type-handling
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "type-handling" {:defaultContext [test-utils/default-str-context {"ex" "http://example.org/ns/"}]})
        db0 (fluree/db ledger)
        db1 @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                 "insert" [{"id" "ex:ace"
                                            "type" "ex:Spade"}
                                           {"id" "ex:king"
                                            "type" "ex:Heart"}
                                           {"id" "ex:queen"
                                            "type" "ex:Heart"}
                                           {"id" "ex:jack"
                                            "type" "ex:Club"}]})
        db2 @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                 "insert" [{"id" "ex:two"
                                            "rdf:type" "ex:Diamond"}]})
        db3 @(fluree/stage2 db1 {"@context" ["https://ns.flur.ee" "" {"rdf:type" "@type"}]
                                 "insert" {"id" "ex:two"
                                           "rdf:type" "ex:Diamond"}})]
    (is (= #{{"id" "ex:queen" "type" "ex:Heart"}
             {"id" "ex:king" "type" "ex:Heart"}}
           (set @(fluree/query db1 {"select" {"?s" ["*"]}
                                    "where" {"id" "?s", "type" "ex:Heart"}})))
        "Query with type and type in results")
    (is (= #{{"id" "ex:queen" "type" "ex:Heart"}
             {"id" "ex:king" "type" "ex:Heart"}}
           (set @(fluree/query db1 {"select" {"?s" ["*"]}
                                    "where" {"id" "?s", "rdf:type" "ex:Heart"}})))
        "Query with rdf:type and type in results")
    (is (= "\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead."
           (-> db2 Throwable->map :cause)))

    (is (= [{"id" "ex:two" "type" "ex:Diamond"}]
           @(fluree/query db3 {"select" {"?s" ["*"]}
                               "where" {"id" "?s", "type" "ex:Diamond"}}))
        "Can transact with rdf:type aliased to type.")))
