(ns fluree.db.transact.transact-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.did :as did]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            [jsonista.core :as json]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration staging-data
  (testing "Disallow staging invalid transactions"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/disallow")
          db0    (fluree/db ledger)

          stage-id-only    @(fluree/stage
                              db0
                              {"@context" ["https://ns.flur.ee"
                                           test-utils/default-context
                                           {:ex "http://example.org/ns/"}]
                               "insert"   {:id :ex/alice}})
          stage-empty-txn  @(fluree/stage
                              db0
                              {"@context" ["https://ns.flur.ee"
                                           test-utils/default-context
                                           {:ex "http://example.org/ns/"}]
                               "insert"   {}})
          stage-empty-node @(fluree/stage
                              db0
                              {"@context" ["https://ns.flur.ee"
                                           test-utils/default-context
                                           {:ex "http://example.org/ns/"}]
                               "insert"
                               [{:id         :ex/alice
                                 :schema/age 42}
                                {}]})
          db-ok            @(fluree/stage
                              db0
                              {"@context" ["https://ns.flur.ee"
                                           test-utils/default-context
                                           {:ex "http://example.org/ns/"}]
                               "insert"
                               {:id         :ex/alice
                                :schema/age 42}})]
      (is (= "Invalid transaction, insert or delete clause must contain nodes with objects."
             (ex-message stage-id-only)))
      (is (= "Invalid transaction, insert or delete clause must contain nodes with objects."
             (ex-message stage-empty-txn)))
      (is (= {:flakes 1, :size 94, :indexed 0}
             (:stats stage-empty-node))
          "empty nodes are allowed as long as there is other data, they are just noops")
      (is (= [[:ex/alice :schema/age 42]]
             @(fluree/query db-ok {:context [test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                   :select  '[?s ?p ?o]
                                   :where   '{:id ?s
                                              ?p  ?o}})))))

  (testing "Allow transacting `false` values"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "tx/bools")
          db-bool @(fluree/stage
                     (fluree/db ledger)
                     {"@context" ["https://ns.flur.ee"
                                  test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                      "insert"
                      {:id        :ex/alice
                       :ex/isCool false}})]
      (is (= [[:ex/alice :ex/isCool false]]
             @(fluree/query db-bool {:context [test-utils/default-context
                                               {:ex "http://example.org/ns/"}]
                                     :select  '[?s ?p ?o]
                                     :where   '{:id ?s, ?p ?o}})))))

  (testing "mixed data types (ref & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts")
          db     @(fluree/stage (fluree/db ledger)
                                {"@context" ["https://ns.flur.ee"
                                             test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
                                 {:id               :ex/brian
                                  :ex/favCoffeeShop [:wiki/Q37158
                                                     "Clemmons Coffee"]}})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  {:context [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                  :select  {:ex/brian [:*]}}]
      (is (= [{:id               :ex/brian
               :ex/favCoffeeShop [{:id :wiki/Q37158} "Clemmons Coffee"]}]
             @(fluree/query db query)))))

  (testing "mixed data types (num & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts")
          db     @(fluree/stage (fluree/db ledger)
                                {"@context" ["https://ns.flur.ee"
                                             test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
                                 {:id :ex/wes
                                  :ex/aFewOfMyFavoriteThings
                                  {"@list" [2011 "jabalí"]}}})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  {:context [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                  :select  {:ex/wes [:*]}}]
      (is (= [{:id                        :ex/wes
               :ex/aFewOfMyFavoriteThings [2011 "jabalí"]}]
             @(fluree/query db query)))))

  (testing "mixed data types (ref & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts")
          db     @(fluree/stage (fluree/db ledger)
                                {"@context" ["https://ns.flur.ee"
                                             test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
                                 {:id               :ex/brian
                                  :ex/favCoffeeShop [:wiki/Q37158
                                                     "Clemmons Coffee"]}})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  {:context [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                  :select  {:ex/brian [:*]}}]
      (is (= [{:id               :ex/brian
               :ex/favCoffeeShop [{:id :wiki/Q37158} "Clemmons Coffee"]}]
             @(fluree/query db query)))))

  (testing "mixed data types (num & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts")
          db     @(fluree/stage (fluree/db ledger)
                                {"@context" ["https://ns.flur.ee"
                                             test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
                                 {:id :ex/wes
                                  :ex/aFewOfMyFavoriteThings
                                  {"@list" [2011 "jabalí"]}}})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  {:context [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                  :select  {:ex/wes [:*]}}]
      (is (= [{:id                        :ex/wes
               :ex/aFewOfMyFavoriteThings [2011 "jabalí"]}]
             @(fluree/query db query)))))
  (testing "iri value maps are handled correctly"
    (let [conn @(fluree/connect-memory)
          ledger @(fluree/create conn "any-iri")
          db0 (fluree/db ledger)

          db1 @(fluree/stage db0 {"@context" {"ex" "http://example.com/"}
                                  "insert" [{"@id" "ex:foo"
                                             "ex:bar" {"@type" "@id"
                                                       "@value" "ex:baz"}}]})]
      (is (= [{"@id" "http://example.com/foo"
               "http://example.com/bar" {"@id" "http://example.com/baz"}}]
             @(fluree/query db1 {"@context" nil
                                 "select" {"http://example.com/foo" ["*"]}}))
          "ex:baz is properly expanded and wrapped in an id-map"))))

(deftest object-var-test
  (testing "var in object position works"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "var-in-obj")
          db1    @(fluree/stage
                   (fluree/db ledger)
                   {"@context" ["https://ns.flur.ee" {"ex" "http://example.org/"}]
                    "insert"   {"@id"       "ex:jane"
                                "ex:friend" {"@id"           "ex:alice"
                                             "ex:bestFriend" {"@id" "ex:bob"}}}})
          db2    @(fluree/stage
                   db1
                   {"@context" ["https://ns.flur.ee" {"ex" "http://example.org/"}]
                    "where"    {"@id"       "?s"
                                "ex:friend" {"ex:bestFriend" "?bestFriend"}}
                    "insert"   {"@id"          "?s"
                                "ex:friendBFF" {"@id" "?bestFriend"}}})]
      (is (= [{"@id"          "ex:jane"
               "ex:friend"    {"@id" "ex:alice", "ex:bestFriend" {"@id" "ex:bob"}}
               "ex:friendBFF" {"@id" "ex:bob"}}]
             @(fluree/query
               db2
               {"@context" {"ex" "http://example.org/"}
                "select"   {"ex:jane" ["*"]}
                "depth"    3}))))))

(deftest policy-ordering-test
  (testing "transaction order does not affect query results"
    (let [conn            (test-utils/create-conn)
          ledger          @(fluree/create conn "tx/policy-order")
          alice-did       (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          data            [{:id          :ex/alice,
                            :type        :ex/User,
                            :schema/name "Alice"}
                           {:id          :ex/john,
                            :type        :ex/User,
                            :schema/name "John"}
                           {:id      alice-did
                            :ex/user :ex/alice
                            :f/role  :ex/userRole}]
          policy          [{:id            :ex/UserPolicy,
                            :type          [:f/Policy],
                            :f/targetClass :ex/User
                            :f/allow       [{:id           :ex/globalViewAllow
                                             :f/targetRole :ex/userRole
                                             :f/action     [:f/view]}]}]
          db-data-first   @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee"
                                          test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                              "insert"   (into data policy)})
          db-policy-first @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee"
                                          test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                              "insert"   (into policy data)})
          user-query      {:context [test-utils/default-context
                                     {:ex "http://example.org/ns/"}]
                           :select  '{?s [:*]}
                           :where   '{:id ?s, :type :ex/User}}]
      (let [users #{{:id :ex/john, :type :ex/User, :schema/name "John"}
                    {:id :ex/alice, :type :ex/User, :schema/name "Alice"}}]
        (is (= users
               (set @(fluree/query db-data-first user-query))))
        (is (= users
               (set @(fluree/query db-policy-first user-query))))))))

(deftest ^:integration transact-large-dataset-test
  (with-tmp-dir storage-path
    (testing "can transact a big movies dataset w/ SHACL constraints"
      (let [shacl   (-> "movies2-schema.json" io/resource json/read-value)
            movies  (-> "movies2.json" io/resource json/read-value)
            ;; TODO: Once :method :memory supports indexing, switch to that.
            conn    @(fluree/connect {:method       :file
                                      :storage-path storage-path})
            ledger  @(fluree/create conn "movies2")
            db      (fluree/db ledger)
            db0     @(fluree/stage db {"@context" ["https://ns.flur.ee"
                                                   test-utils/default-str-context
                                                   {"ex" "https://example.com/"}]
                                       "insert"   shacl})
            _       (assert (not (util/exception? db0)))
            db1     @(fluree/commit! ledger db0)
            _       (assert (not (util/exception? db1)))
            db2     @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                    test-utils/default-str-context
                                                    {"ex"        "https://example.com/"
                                                     "ex:rating" {"@type" "xsd:float"}}]
                                        "insert"   movies})
            _       (assert (not (util/exception? db2)))
            query   {"@context" [test-utils/default-str-context
                                 {"ex" "https://example.com/"}]
                     "select"   "?title"
                     "where"    {"@id"      "?m"
                                 "type"     "ex:Movie"
                                 "ex:title" "?title"}}
            results @(fluree/query db2 query)]
        (is (= 100 (count results)))
        (is (every? (set results)
                    ["Interstellar" "Wreck-It Ralph" "The Jungle Book" "WALL·E"
                     "Iron Man" "Avatar"]))))))

(deftest ^:integration transact-api-test
  (let [conn        (test-utils/create-conn)
        ledger-name "example-ledger"
        ledger      @(fluree/create conn ledger-name)
        context     (dissoc test-utils/default-context :f)
        ;; can't `transact!` until ledger can be loaded (ie has at least one commit)
        db          @(fluree/stage (fluree/db ledger)
                                   {"@context" ["https://ns.flur.ee"
                                                context
                                                {:ex "http://example.org/ns/"}]
                                    "insert"
                                    {:id   :ex/firstTransaction
                                     :type :ex/Nothing}})
        _           @(fluree/commit! ledger db)
        user-query  {:context [context {:ex "http://example.org/ns/"}]
                     :select  '{?s [:*]}
                     :where   '{:id ?s, :type :ex/User}}]
    (testing "Top-level context is used for transaction nodes"
      (let [txn {"@context" ["https://ns.flur.ee"
                             context
                             {:ex "http://example.org/ns/"}
                             {:f   "https://ns.flur.ee/ledger#"
                              :foo "http://foo.com/"
                              :id  "@id"}]
                 "ledger"   ledger-name
                 "insert"   [{:id          :ex/alice
                              :type        :ex/User
                              :foo/bar     "foo"
                              :schema/name "Alice"}
                             {:id          :ex/bob
                              :type        :ex/User
                              :foo/baz     "baz"
                              :schema/name "Bob"}]}
            db  @(fluree/transact! conn txn)]
        (is (= #{{:id          :ex/bob,
                  :type        :ex/User,
                  :schema/name "Bob",
                  :foo/baz     "baz"}
                 {:id          :ex/alice,
                  :type        :ex/User,
                  :foo/bar     "foo",
                  :schema/name "Alice"}}
               (set @(fluree/query db (assoc user-query
                                             :context [context
                                                       {:ex "http://example.org/ns/"}
                                                       {:foo "http://foo.com/"}])))))))
    (testing "Aliased @id are correctly identified"
      (let [txn {"@context" ["https://ns.flur.ee"
                             context
                             {:ex "http://example.org/ns/"}
                             {:id-alias "@id"}]
                 "ledger"   ledger-name
                 "insert"   {:id-alias         :ex/alice
                             :schema/givenName "Alicia"}}
            db  @(fluree/transact! conn txn)]
        (is (= #{{:id          :ex/bob,
                  :type        :ex/User,
                  :schema/name "Bob",
                  :foo/baz     "baz"}
                 {:id               :ex/alice,
                  :type             :ex/User,
                  :schema/name      "Alice",
                  :foo/bar          "foo",
                  :schema/givenName "Alicia"}}
               (set @(fluree/query db (assoc user-query
                                             :context [context
                                                       {:ex "http://example.org/ns/"}
                                                       {:foo "http://foo.com/"
                                                        :bar "http://bar.com/"}])))))))
    (testing "@context inside node is correctly handled"
      (let [txn {"@context" ["https://ns.flur.ee"
                             {:f "https://ns.flur.ee/ledger#"}]
                 "ledger"   ledger-name
                 "insert"   [{:context    [context
                                           {:ex "http://example.org/ns/"}
                                           {:quux "http://quux.com/"}]
                              :id         :ex/alice
                              :quux/corge "grault"}]}
            db  @(fluree/transact! conn txn)]
        (is (= #{{:id          :ex/bob
                  :type        :ex/User
                  :schema/name "Bob"
                  :foo/baz     "baz"}
                 {:id               :ex/alice
                  :type             :ex/User
                  :schema/name      "Alice"
                  :schema/givenName "Alicia"
                  :quux/corge       "grault"
                  :foo/bar          "foo"}}
               (set @(fluree/query db (assoc user-query
                                             :context [context
                                                       {:ex "http://example.org/ns/"}
                                                       {:foo  "http://foo.com/"
                                                        :bar  "http://bar.com/"
                                                        :quux "http://quux.com/"}])))))))
    (testing "fuel tracking works on transactions"
      (let [txn {"@context" ["https://ns.flur.ee"
                             {:f "https://ns.flur.ee/ledger#"}]
                 "ledger"   ledger-name
                 "insert"   [{:context    [context
                                           {:ex "http://example.org/ns/"}
                                           {:quux "http://quux.com/"}]
                              :id         :ex/alice
                              :quux/corge "grault"}]}
            committed  @(fluree/transact! conn txn {:meta true})]
        (is (= [:fuel :result :status :time]
               (sort (keys committed))))))

    (testing "Throws on invalid txn"
      (let [txn {"@context" ["https://ns.flur.ee" "" {:quux "http://quux.com/"}]
                 "insert"   {:id :ex/cam :quux/corge "grault"}}]
        (is (= "Invalid transaction, missing required key: ledger."
               (ex-message @(fluree/transact! conn txn))))))))


(deftest ^:integration base-and-vocab-test
  (testing "@base & @vocab work w/ stage"
    (let [conn        @(fluree/connect-memory)
          ctx         {"@base"  "http://example.org/"
                       "@vocab" "http://example.org/terms/"
                       "f"      "https://ns.flur.ee/ledger#"}
          ledger-name "cookbook/base"
          txn         {"@context" ctx
                       "f:ledger" ledger-name
                       "@graph"   [{"@id"     "nessie"
                                    "@type"   "SeaMonster"
                                    "isScary" false}]}
          ledger      @(fluree/create conn ledger-name)
          db0         (fluree/db ledger)
          db1         @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                      ctx]
                                          "insert"   txn})]
      (is (= [{"@id"                              "http://example.org/nessie"
               "@type"                            "http://example.org/terms/SeaMonster"
               "http://example.org/terms/isScary" false}]
             @(fluree/query db1 {"select"   '{?m ["*"]}
                                 "where"    '{"@id"   ?m
                                              "@type" "http://example.org/terms/SeaMonster"}})))))
  (testing "@base & @vocab work w/ stage"
    (let [conn        @(fluree/connect-memory)
          ctx         ["https://ns.flur.ee"
                       {"@base"  "http://example.org/"
                        "@vocab" "http://example.org/terms/"}]
          ledger-name "cookbook/base"
          txn         {"@context" ctx
                       "ledger"   ledger-name
                       "insert"   {"@id"     "nessie"
                                   "@type"   "SeaMonster"
                                   "isScary" false}}
          ledger      @(fluree/create conn ledger-name)
          db0         (fluree/db ledger)
          db1         @(fluree/stage db0 txn)]
      (is (= [{"@id"                              "http://example.org/nessie"
               "@type"                            "http://example.org/terms/SeaMonster"
               "http://example.org/terms/isScary" false}]

             @(fluree/query db1 '{"select"   {?m ["*"]}
                                  "where"    {"@id"   ?m
                                              "@type" "http://example.org/terms/SeaMonster"}}))))))

(deftest json-objects
  (testing "Allow transacting `json` values"
    (let [conn   @(fluree/connect-memory)
          ledger @(fluree/create conn "jsonpls")
          db0    (fluree/db ledger)
          db1    @(fluree/stage
                    db0
                    {"@context" ["https://ns.flur.ee"
                                 test-utils/default-str-context
                                 {"ex" "http://example.org/ns/"}]
                     "insert"
                     [{"@id"     "ex:alice"
                       "@type"   "ex:Person"
                       "ex:json" {"@type"  "@json"
                                  "@value" {"json" "data"
                                            "is"   ["cool" "right?" 1 false 1.0]}}}
                      {"@id"     "ex:bob"
                       "@type"   "ex:Person"
                       "ex:json" {"@type"  "@json"
                                  "@value" {:edn "data"
                                            :is  ["cool" "right?" 1 false 1.0]}}}]})]
      (is (= [{"id"     "ex:alice",
               "type"   "ex:Person",
               "ex:json" {"json" "data", "is" ["cool" "right?" 1 false 1]}}
              {"id"     "ex:bob",
               "type"   "ex:Person",
               "ex:json" {":edn" "data", ":is" ["cool" "right?" 1 false 1]}}]
             @(fluree/query db1 {"@context" [test-utils/default-str-context
                                             {"ex" "http://example.org/ns/"}]
                                 "where"  {"@id" "?s" "@type" "ex:Person"}
                                 "select" {"?s" ["*"]}}))
          "comes out as data from subject crawl")
      (is (= [{":edn" "data", ":is" ["cool" "right?" 1 false 1]}
              {"json" "data", "is" ["cool" "right?" 1 false 1]}]
             @(fluree/query db1 {"@context" {"ex" "http://example.org/ns/"}
                                 "select"   "?json"
                                 "where"  {"@id" "?s" "ex:json" "?json"}}))
          "comes out as data from select clause"))))

(deftest ^:integration no-where-solutions
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "insert-delete")
        context {"ex"     "http://example.org/ns/"
                 "schema" "http://schema.org/"}
        db0     (fluree/db ledger)

        db1 @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                "insert"   [{"@id" "ex:andrew" "schema:name" "Andrew"}]})

        db2 @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                "where"    {"@id"                "ex:andrew"
                                            "schema:description" "?o"}
                                "delete"   {"@id"                "ex:andrew"
                                            "schema:description" "?o"}
                                "insert"   {"@id"                "ex:andrew"
                                            "schema:description" "He's great!"}})]
    (is (= {"@id"                "ex:andrew"
            "schema:name"        "Andrew"
            "schema:description" "He's great!"}
           @(fluree/query db2 {"@context"  context
                               "selectOne" {"ex:andrew" ["*"]}})))))

(deftest ^:integration shacl-datatype-coercion
  (let [conn      @(fluree/connect-memory)
        context   {"ex"     "http://example.org/",
                   "f"      "https://ns.flur.ee/ledger#",
                   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                   "schema" "http://schema.org/",
                   "sh"     "http://www.w3.org/ns/shacl#",
                   "xsd"    "http://www.w3.org/2001/XMLSchema#"}
        ledger-id "sh-datatype"
        ledger    @(fluree/create conn ledger-id)

        db0 (fluree/db ledger)
        db1 @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                            context],
                                "ledger"   ledger-id
                                "insert"   {"@id"            "ex:NodeShape/Yeti",
                                            "@type"          "sh:NodeShape",
                                            "sh:targetClass" {"@id" "ex:Yeti"},
                                            "sh:property"    [{"@id"         "ex:PropertyShape/age",
                                                               "sh:path"     {"@id" "schema:age"},
                                                               "sh:datatype" {"@id" "xsd:integer"}}]}})


        db2 @(fluree/stage db1 {"@context" ["https://ns.flur.ee"
                                            context],
                                "ledger"   ledger-id
                                "insert"   {"@id"         "ex:freddy",
                                            "@type"       "ex:Yeti",
                                            "schema:name" "Freddy",
                                            "schema:age"  8}})

        _      @(fluree/commit! ledger db2)
        loaded @(fluree/load conn ledger-id)

        db3 @(fluree/stage (fluree/db loaded) {"@context" ["https://ns.flur.ee"
                                                           context],
                                               "ledger"   ledger-id
                                               "insert"   {"@id"         "ex:letti",
                                                           "@type"       "ex:Yeti",
                                                           "schema:name" "Letti",
                                                           "schema:age"  "alot"}})]
    (is (= {"schema:age" 8}
           @(fluree/query db2 {"@context"  context
                               "selectOne" {"ex:freddy" ["schema:age"]}}))
        "8 is converted from a long to an int.")
    (is (= "Subject ex:letti path [\"schema:age\"] violates constraint sh:datatype of shape ex:PropertyShape/age - the following values do not have expected datatype xsd:integer: alot."
           (ex-message db3))
        "datatype constraint is restored after a load")))
