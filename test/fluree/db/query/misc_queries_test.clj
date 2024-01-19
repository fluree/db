(ns fluree.db.query.misc-queries-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration select-sid
  (testing "Select index's subject id in query using special keyword"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subid")
          db     @(fluree/stage
                    (fluree/db ledger)
                    {"@context" ["https://ns.flur.ee"
                                 test-utils/default-context
                                 {:ex "http://example.org/ns/"}]
                     "insert"
                     {:graph [{:id          :ex/alice,
                               :type        :ex/User,
                               :schema/name "Alice"}
                              {:id           :ex/bob,
                               :type         :ex/User,
                               :schema/name  "Bob"
                               :ex/favArtist {:id          :ex/picasso
                                              :schema/name "Picasso"}}]}})]
      (is (->> @(fluree/query db {:context [test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                  :select  {'?s [:_id {:ex/favArtist [:_id ]}]}
                                  :where   {:id '?s, :type :ex/User}})
               (reduce (fn [sids {:keys [_id] :as node}]
                         (cond-> (conj sids _id)
                           (:ex/favArtist node) (conj (:_id (:ex/favArtist node)))))
                       [])
               (every? int?))))))

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
                 :id                       "http://example.org/ns/dan"
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
                 [:type :id "@type"]
                 [:id :id "@id"]}
               (set @(fluree/query db {:context [test-utils/default-context
                                                 {:ex "http://example.org/ns/"}]
                                       :select  ['?s '?p '?o]
                                       :where   {:id '?s
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
               (sort-by :id @(fluree/query db {:context [test-utils/default-context
                                                         {:ex "http://example.org/ns/"}]
                                               :select  {'?s ["*"]}
                                               :where   {:id '?s, '?p '?o}})))
            "Every triple should be returned.")
        (let [db*    @(fluree/commit! ledger db)
              result @(fluree/query db* {:context [test-utils/default-context
                                                   {:ex "http://example.org/ns/"}]
                                         :select  ['?s '?p '?o]
                                         :where   {:id '?s, '?p '?o}})]
          (is (= #{["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/v
                    0]
                   [:type :id "@type"]
                   [:f/t :id "https://ns.flur.ee/ledger#t"]
                   [:ex/jane :schema/age 30]
                   [:ex/alice :id "http://example.org/ns/alice"]
                   [:f/branch :id "https://ns.flur.ee/ledger#branch"]
                   ["fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"
                    :id
                    "fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"]
                   [:schema/email :id "http://schema.org/email"]
                   [:ex/alice :schema/name "Alice"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    "https://www.w3.org/2018/credentials#issuer"
                    "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                   [:schema/age :id "http://schema.org/age"]
                   [:schema/name :id "http://schema.org/name"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :id
                    "fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"]
                   [:f/txn :id "https://ns.flur.ee/ledger#txn"]
                   ["fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"
                    :f/flakes
                    21]
                   [:f/time :id "https://ns.flur.ee/ledger#time"]
                   ["fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"
                    :f/size
                    1370]
                   [:f/message :id "https://ns.flur.ee/ledger#message"]
                   [:f/alias :id "https://ns.flur.ee/ledger#alias"]
                   [:ex/bob :type :ex/User]
                   [:f/address :id "https://ns.flur.ee/ledger#address"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/author
                    ""]
                   [:ex/jane :type :ex/User]
                   [:ex/jane :schema/email "jane@flur.ee"]
                   [:f/author :id "https://ns.flur.ee/ledger#author"]
                   [:ex/alice :type :ex/User]
                   ["fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"
                    :f/t
                    1]
                   [:ex/jane :schema/name "Jane"]
                   [:f/v :id "https://ns.flur.ee/ledger#v"]
                   [:ex/bob :schema/name "Bob"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/time
                    720000]
                   [:ex/User :id "http://example.org/ns/User"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/txn
                    "fluree:memory://query/everything/txn/144908452423d421230317bbc681f6023039201145ab05336b69c39c3958e39c"]
                   [:ex/bob :schema/age 22]
                   [:ex/alice :schema/age 42]
                   [:f/flakes :id "https://ns.flur.ee/ledger#flakes"]
                   [:ex/jane :id "http://example.org/ns/jane"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/alias
                    "query/everything"]
                   [:f/previous :id "https://ns.flur.ee/ledger#previous"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/branch
                    "main"]
                   [:id :id "@id"]
                   [:f/data :id "https://ns.flur.ee/ledger#data"]
                   [:f/size :id "https://ns.flur.ee/ledger#size"]
                   ["https://www.w3.org/2018/credentials#issuer"
                    :id
                    "https://www.w3.org/2018/credentials#issuer"]
                   ["fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"
                    :f/address
                    "fluree:memory://eb42c9187ee0bddcc215c5d7ca829c1528a22bf8ee94f933affbe830b845030a"]
                   [:ex/bob :id "http://example.org/ns/bob"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/data
                    "fluree:db:sha256:bboe6nikw75nolggme4ohcpmqbeknnulujn4c5wqspovvd2munlkw"]
                   ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                    :id
                    "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                   [:ex/alice :schema/email "alice@flur.ee"]
                   ["fluree:commit:sha256:bs5ut2obtyi5rw5wnlybedqty3ddvwd3t5kia2u33ytonibizzqy"
                    :f/address
                    "fluree:memory://de0152de88b39ccf4e5d63a77b9284316645287b8c8cd631ef56aa8298a7d6d6"]
                   [:f/annotation :id "https://ns.flur.ee/ledger#annotation"]}
                 (set result))
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
  (let [conn   @(fluree/connect {:method :memory})
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
    (let [conn0     @(fluree/connect {:method :file :storage-path storage-path})
          ledger-id "new3"
          ledger    @(fluree/create-with-txn conn0 {"@context" {"ex" {"ex" "http://example.org/ns/"}}
                                                    "ledger"   ledger-id
                                                    "insert"   {"ex:createdAt" "now"}})

          conn1 @(fluree/connect {:method :file, :storage-path storage-path})]
      (is (= [{"ex:createdAt" "now"}]
             @(fluree/query-connection conn1 {"@context" {"ex" {"ex" "http://example.org/ns/"}}
                                              :from      ledger-id
                                              :where     {"@id" "?s" "ex:createdAt" "now"},
                                              :select    {"?s" ["ex:createdAt"]}}))))))
