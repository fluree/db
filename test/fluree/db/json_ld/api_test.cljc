(ns fluree.db.json-ld.api-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing use-fixtures]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            #?@(:cljs [[clojure.core.async :refer [go <!]]
                       [clojure.core.async.interop :refer [<p!]]])
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            #?@(:clj ([fluree.db.database.async :as async-db]
                      [clojure.core.async :as async]))
            #?(:clj  [test-with-files.tools :refer [with-tmp-dir]
                      :as twf]
               :cljs [test-with-files.tools :as-alias twf])))

#?(:clj (use-fixtures :each test-utils/deterministic-blank-node-fixture))

(deftest exists?-test
  (testing "returns false before committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             check1       @(fluree/exists? conn ledger-alias)
             ledger       @(fluree/create conn ledger-alias)
             check2       @(fluree/exists? conn ledger-alias)
             _            @(fluree/stage (fluree/db ledger)
                                         {"@context" ["https://ns.flur.ee"
                                                      test-utils/default-context]
                                          "insert"
                                          [{:id :f/me
                                            :type :schema/Person
                                            :schema/fname "Me"}]})
             check3       @(fluree/exists? conn ledger-alias)]
         (is (every? false? [check1 check2 check3])))))
  (testing "returns true after committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage (fluree/db ledger)
                                         {"@context" ["https://ns.flur.ee"
                                                      test-utils/default-context]
                                          "insert"
                                          [{:id           :f/me
                                            :type         :schema/Person
                                            :schema/fname "Me"}]})]
         @(fluree/commit! ledger db)
         (is (test-utils/retry-exists? conn ledger-alias 100))
         (is (not @(fluree/exists? conn "notaledger"))))

       :cljs
       (async done
         (go
          (let [conn         (<! (test-utils/create-conn))
                ledger-alias "testledger"
                ledger       (<p! (fluree/create conn ledger-alias))
                db           (<p! (fluree/stage (fluree/db ledger)
                                                {"@context" ["https://ns.flur.ee"
                                                             test-utils/default-context]
                                                 "insert"
                                                 [{:id           :f/me
                                                   :type         :schema/Person
                                                   :schema/fname "Me"}]}))]
            (<p! (fluree/commit! ledger db))
            (is (test-utils/retry-exists? conn ledger-alias 100))
            (is (not (<p! (fluree/exists? conn "notaledger"))))
            (done)))))))

#?(:clj
   (deftest load-from-file-test
     (testing "can load a file ledger with single cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect {:method :file, :storage-path storage-path})
               ledger-alias "load-from-file-test-single-card"
               ledger       @(fluree/create conn ledger-alias )
               db           @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                "insert"
                                [{:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :schema/email "brian@example.org"
                                  :schema/age   50
                                  :ex/favNums   7
                                  :ex/height    6.2}

                                 {:id           :ex/cam
                                  :type         :ex/User
                                  :schema/name  "Cam"
                                  :schema/email "cam@example.org"
                                  :schema/age   34
                                  :ex/favNums   5
                                  :ex/friend    :ex/brian}]})
               db        @(fluree/commit! ledger db)
               db        @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee"
                                         test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                             "insert"
                             {:id         :ex/brian
                              :ex/favNums 7}})
               db        @(fluree/commit! ledger db)
               target-t  (:t db)
               ;; TODO: Replace this w/ :syncTo equivalent once we have it
               loaded    (test-utils/load-to-t conn ledger-alias target-t 100)
               loaded-db (fluree/db loaded)]
           (is (= target-t (:t loaded-db))))))

     (testing "can load a file ledger with multi-cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect {:method :file, :storage-path storage-path})
               ledger-alias "load-from-file-test-multi-card"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                "insert"
                                [{:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :schema/email "brian@example.org"
                                  :schema/age   50
                                  :ex/favNums   7}

                                 {:id           :ex/alice
                                  :type         :ex/User
                                  :schema/name  "Alice"
                                  :schema/email "alice@example.org"
                                  :schema/age   50
                                  :ex/favNums   [42 76 9]}

                                 {:id           :ex/cam
                                  :type         :ex/User
                                  :schema/name  "Cam"
                                  :schema/email "cam@example.org"
                                  :schema/age   34
                                  :ex/favNums   [5 10]
                                  :ex/friend    [:ex/brian :ex/alice]}]})
               db        @(fluree/commit! ledger db)
               db        @(fluree/stage
                            db
                            ;; test a multi-cardinality retraction
                            {"@context" ["https://ns.flur.ee"
                                         test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                             "insert"
                             [{:id         :ex/alice
                               :ex/favNums [42 76 9]}]})
               db        @(fluree/commit! ledger db)
               target-t  (:t db)
               ;; TODO: Replace this w/ :syncTo equivalent once we have it
               loaded    (test-utils/load-to-t conn ledger-alias target-t 100)
               loaded-db (fluree/db loaded)]
           (is (= target-t (:t loaded-db))))))

     (testing "query returns the correct results from a loaded ledger"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect {:method :file, :storage-path storage-path})
               ledger-alias "load-from-file-query"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            {:id     "@id"
                                             :type   "@type"
                                             :ex     "http://example.com/"
                                             :schema "http://schema.org/"}]
                                "insert"
                                [{:id          :ex/Andrew
                                  :type        :schema/Person
                                  :schema/name "Andrew"
                                  :ex/friend   {:id          :ex/Jonathan
                                                :type        :schema/Person
                                                :schema/name "Jonathan"}}]})
               query        {:context {:ex "http://example.com/"}
                             :select  {:ex/Andrew [:*]}}
               res1         @(fluree/query db query)
               _            @(fluree/commit! ledger db)
               loaded       (test-utils/retry-load conn ledger-alias 100)
               loaded-db    (fluree/db loaded)
               res2         @(fluree/query loaded-db query)]
           (is (= res1 res2)))))

     (testing "can load a ledger with `list` values"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect {:method       :file
                                              :storage-path storage-path})
               ledger-alias "load-lists-test"
               ledger       @(fluree/create conn ledger-alias
                                            {:reindex-min-bytes 0}) ; force reindex on every commit
               db           @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                "insert"
                                [{:id         :ex/alice,
                                  :type       :ex/User,
                                  :ex/friends {:list [:ex/john :ex/cam]}}
                                 {:id         :ex/cam,
                                  :type       :ex/User
                                  :ex/numList {:list [7 8 9 10]}}
                                 {:id   :ex/john,
                                  :type :ex/User}]})
               db           @(fluree/commit! ledger db)
               target-t     (:t db)
               loaded       (test-utils/load-to-t conn ledger-alias target-t 100)
               loaded-db    (fluree/db loaded)]
           (is (= target-t (:t loaded-db)))
           (testing "query returns expected `list` values"
             (is (= [{:id         :ex/alice,
                      :type       :ex/User,
                      :ex/friends [{:id :ex/john} {:id :ex/cam}]}
                     {:id         :ex/cam,
                      :type       :ex/User,
                      :ex/numList [7 8 9 10]}
                     {:id :ex/john, :type :ex/User}]
                    @(fluree/query loaded-db {:context [test-utils/default-context
                                                        {:ex "http://example.org/ns/"}]
                                              :select  '{?s [:*]}
                                              :where   '{:id ?s, :type :ex/User}}))))))

       (testing "can load with policies"
         (with-tmp-dir storage-path
           (let [conn         @(fluree/connect {:method       :file
                                                :storage-path storage-path})
                 ledger-alias "load-policy-test"
                 ledger       @(fluree/create conn ledger-alias)
                 db           @(fluree/stage
                                 (fluree/db ledger)
                                 {"@context" ["https://ns.flur.ee"
                                              test-utils/default-context
                                              {:ex "http://example.org/ns/"}]
                                  "insert"
                                  [{:id          :ex/alice,
                                    :type        :ex/User,
                                    :schema/name "Alice"
                                    :schema/ssn  "111-11-1111"
                                    :ex/friend   :ex/john}
                                   {:id          :ex/john,
                                    :schema/name "John"
                                    :type        :ex/User,
                                    :schema/ssn  "888-88-8888"}
                                   {:id      "did:fluree:123"
                                    :ex/user :ex/alice
                                    :f/role  :ex/userRole}]})
                 db+policy    @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee"
                                              test-utils/default-context
                                              {:ex "http://example.org/ns/"}]
                                  "insert"
                                  [{:id            :ex/UserPolicy,
                                    :type          :f/Policy,
                                    :f/targetClass :ex/User
                                    :f/allow
                                    [{:id           :ex/globalViewAllow
                                      :f/targetRole :ex/userRole
                                      :f/action     [:f/view]}]
                                    :f/property
                                    [{:f/path :schema/ssn
                                      :f/allow
                                      [{:id           :ex/ssnViewRule
                                        :f/targetRole :ex/userRole
                                        :f/action     [:f/view]
                                        :f/equals
                                        {:list [:f/$identity :ex/user]}}]}]}]})
                 db+policy    @(fluree/commit! ledger db+policy)
                 target-t     (:t db+policy)
                 loaded       (test-utils/load-to-t conn ledger-alias target-t
                                                    100)
                 loaded-db    (fluree/db loaded)]
             (is (= target-t (:t loaded-db)))
             (testing "query returns expected policy"
               (is (= [{:id            :ex/UserPolicy,
                        :type          :f/Policy,
                        :f/allow       {:id           :ex/globalViewAllow,
                                        :f/action     {:id :f/view},
                                        :f/targetRole {:id :ex/userRole}},
                        :f/property    {:f/path  {:id :schema/ssn}
                                        :f/allow {:id           :ex/ssnViewRule,
                                                  :f/action     {:id :f/view},
                                                  :f/targetRole {:id :ex/userRole}
                                                  :f/equals     [{:id :f/$identity} {:id :ex/user}]},},
                        :f/targetClass {:id :ex/User}}]
                      @(fluree/query loaded-db
                                     {:context [test-utils/default-context
                                                {:ex "http://example.org/ns/"}]
                                      :select  '{?s [:id
                                                     :type
                                                     :f/targetClass
                                                     {:f/allow [:*]}
                                                     {:f/property
                                                      [:f/path
                                                       {:f/allow [:*]}]}]}
                                      :where   '{:id ?s, :type :f/Policy}}))))))))

     (testing "Can load a ledger with time values"
       (with-tmp-dir storage-path
         (let [conn   @(fluree/connect {:method       :file
                                        :storage-path storage-path})
               ledger @(fluree/create conn "index/datetimes")
               db     @(fluree/stage
                         (fluree/db ledger)
                         {"@context" ["https://ns.flur.ee"
                                      test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"}]
                          "insert"
                          [{"@id"   "ex:Foo",
                            "@type" "ex:Bar",

                            "ex:offsetDateTime"  {"@type"  "xsd:dateTime"
                                                  "@value" "2023-04-01T00:00:00.000Z"}
                            "ex:localDateTime"   {"@type"  "xsd:dateTime"
                                                  "@value" "2021-09-24T11:14:32.833"}
                            "ex:offsetDateTime2" {"@type"  "xsd:date"
                                                  "@value" "2022-01-05Z"}
                            "ex:localDate"       {"@type"  "xsd:date"
                                                  "@value" "2024-02-02"}
                            "ex:offsetTime"      {"@type"  "xsd:time"
                                                  "@value" "12:42:00Z"}
                            "ex:localTime"       {"@type"  "xsd:time"
                                                  "@value" "12:42:00"}}]})
               db-commit @(fluree/commit! ledger db)
               loaded    (test-utils/retry-load conn (:alias ledger) 100)
               q         {"@context" [test-utils/default-str-context
                                      {"ex" "http://example.org/ns/"}]
                          "select"   {"?s" ["*"]}
                          "where"    {"@id" "?s", "type" "ex:Bar"}}]
           (is (= @(fluree/query (fluree/db loaded) q)
                  @(fluree/query db q))))))))

#?(:clj
   (deftest load-from-memory-test
     (testing "can load a memory ledger with single cardinality predicates"
       (let [conn         @(fluree/connect {:method :memory})
             ledger-alias "load-from-memory-test-single-card"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee"
                                          test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                              "insert"
                              [{:id           :ex/brian
                                :type         :ex/User
                                :schema/name  "Brian"
                                :schema/email "brian@example.org"
                                :schema/age   50
                                :ex/favNums   7}

                               {:id           :ex/cam
                                :type         :ex/User
                                :schema/name  "Cam"
                                :schema/email "cam@example.org"
                                :schema/age   34
                                :ex/favNums   5
                                :ex/friend    :ex/brian}]})
             db        @(fluree/commit! ledger db)
             db        @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee"
                                       test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                           "insert"
                           {:id         :ex/brian
                            :ex/favNums 7}})
             db        @(fluree/commit! ledger db)
             target-t  (:t db)
             ;; TODO: Replace this w/ :syncTo equivalent once we have it
             loaded    (test-utils/load-to-t conn ledger-alias target-t 100)
             loaded-db (fluree/db loaded)]
         (is (= target-t (:t loaded-db)))))

     (testing "can load a memory ledger with multi-cardinality predicates"
       (let [conn         @(fluree/connect {:method :memory})
             ledger-alias "load-from-memory-test-multi-card"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee"
                                          test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                              "insert"
                              [{:id           :ex/brian
                                :type         :ex/User
                                :schema/name  "Brian"
                                :schema/email "brian@example.org"
                                :schema/age   50
                                :ex/favNums   7}

                               {:id           :ex/alice
                                :type         :ex/User
                                :schema/name  "Alice"
                                :schema/email "alice@example.org"
                                :schema/age   50
                                :ex/favNums   [42 76 9]}

                               {:id           :ex/cam
                                :type         :ex/User
                                :schema/name  "Cam"
                                :schema/email "cam@example.org"
                                :schema/age   34
                                :ex/favNums   [5 10]
                                :ex/friend    [:ex/brian :ex/alice]}]})
             db        @(fluree/commit! ledger db)
             db        @(fluree/stage
                          db
                          ;; test a multi-cardinality retraction
                          {"@context" ["https://ns.flur.ee"
                                       test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                           "insert"
                           [{:id         :ex/alice
                             :ex/favNums [42 76 9]}]})
             db        @(fluree/commit! ledger db)
             target-t  (:t db)
             ;; TODO: Replace this w/ :syncTo equivalent once we have it
             loaded    (test-utils/load-to-t conn ledger-alias target-t 100)
             loaded-db (fluree/db loaded)]
         (is (= target-t (:t loaded-db)))))

     (testing "query returns the correct results from a loaded ledger"
       (let [conn           @(fluree/connect {:method :memory})
             ledger-alias   "load-from-memory-query"
             ledger         @(fluree/create conn ledger-alias)
             db             @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            {:id     "@id"
                                             :type   "@type"
                                             :ex     "http://example.com/"
                                             :schema "http://schema.org/"}]
                                "insert"
                                [{:id          :ex/Andrew
                                  :type        :schema/Person
                                  :schema/name "Andrew"
                                  :ex/friend   {:id          :ex/Jonathan
                                                :type        :schema/Person
                                                :schema/name "Jonathan"}}]})
             query          {:context {:ex "http://example.com/"}
                             :select '{:ex/Andrew [:*]}}
             res1           @(fluree/query db query)
             _              @(fluree/commit! ledger db)
             loaded         (test-utils/retry-load conn ledger-alias 100)
             loaded-db      (fluree/db loaded)
             res2           @(fluree/query loaded-db query)]
         (is (= res1 res2))))

     (testing "can load a ledger with `list` values"
       (let [conn         @(fluree/connect {:method :memory})
             ledger-alias "load-lists-test"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee"
                                          test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                              "insert"
                              [{:id         :ex/alice,
                                :type       :ex/User,
                                :ex/friends {:list [:ex/john :ex/cam]}}
                               {:id         :ex/cam,
                                :type       :ex/User
                                :ex/numList {:list [7 8 9 10]}}
                               {:id   :ex/john,
                                :type :ex/User}]})
             db           @(fluree/commit! ledger db)
             target-t     (:t db)
             loaded       (test-utils/load-to-t conn ledger-alias target-t 100)
             loaded-db    (fluree/db loaded)]
         (is (= target-t (:t loaded-db)))
         (testing "query returns expected `list` values"
           (is (= #{{:id         :ex/cam,
                     :type       :ex/User,
                     :ex/numList [7 8 9 10]}
                    {:id :ex/john, :type :ex/User}
                    {:id         :ex/alice,
                     :type       :ex/User,
                     :ex/friends [{:id :ex/john} {:id :ex/cam}]}}
                  (set @(fluree/query loaded-db {:context [test-utils/default-context
                                                           {:ex "http://example.org/ns/"}]
                                                 :select  '{?s [:*]}
                                                 :where   '{:id ?s, :type :ex/User}}))))))

       (testing "can load with policies"
         (let [conn         @(fluree/connect {:method :memory})
               ledger-alias "load-policy-test"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                               (fluree/db ledger)
                               {"@context" ["https://ns.flur.ee"
                                            test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                "insert"
                                [{:id          :ex/alice,
                                  :type        :ex/User,
                                  :schema/name "Alice"
                                  :schema/ssn  "111-11-1111"
                                  :ex/friend   :ex/john}
                                 {:id          :ex/john,
                                  :schema/name "John"
                                  :type        :ex/User,
                                  :schema/ssn  "888-88-8888"}
                                 {:id      "did:fluree:123"
                                  :ex/user :ex/alice
                                  :f/role  :ex/userRole}]})
               db+policy    @(fluree/stage
                               db
                               {"@context" ["https://ns.flur.ee"
                                            test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                "insert"
                                [{:id            :ex/UserPolicy,
                                  :type          [:f/Policy],
                                  :f/targetClass :ex/User
                                  :f/allow       [{:id           :ex/globalViewAllow
                                                   :f/targetRole :ex/userRole
                                                   :f/action     [:f/view]}]
                                  :f/property    [{:f/path  :schema/ssn
                                                   :f/allow [{:id           :ex/ssnViewRule
                                                              :f/targetRole :ex/userRole
                                                              :f/action     [:f/view]
                                                              :f/equals     {:list [:f/$identity :ex/user]}}]}]}]})
               db+policy    @(fluree/commit! ledger db+policy)
               target-t     (:t db+policy)
               loaded       (test-utils/load-to-t conn ledger-alias target-t 100)
               loaded-db    (fluree/db loaded)]
           (is (= target-t (:t loaded-db)))
           (testing "query returns expected policy"
             (is (= [{:id            :ex/UserPolicy
                      :type          :f/Policy
                      :f/allow
                      {:id           :ex/globalViewAllow
                       :f/action     {:id :f/view}
                       :f/targetRole {:id :ex/userRole}}
                      :f/property
                      {:f/allow
                       {:id           :ex/ssnViewRule
                        :f/action     {:id :f/view}
                        :f/targetRole {:id :ex/userRole}
                        :f/equals     [{:id :f/$identity} {:id :ex/user}]}
                       :f/path {:id :schema/ssn}}
                      :f/targetClass {:id :ex/User}}]
                    @(fluree/query loaded-db {:context [test-utils/default-context
                                                        {:ex "http://example.org/ns/"}]
                                              :select  '{?s [:id
                                                             :type
                                                             :f/targetClass
                                                             {:f/allow [:id :f/targetRole :f/action]}
                                                             {:f/property [:f/path {:f/allow [:*]}]}]}
                                              :where   '{:id ?s :type :f/Policy}})))))))
     (testing "loading predefined properties"
       (let [conn           (test-utils/create-conn)
             ledger-alias   "shacl/a"
             ledger         @(fluree/create conn "shacl/a")
             db1            @(test-utils/transact ledger
                                                  {"@context" ["https://ns.flur.ee"
                                                               test-utils/default-str-context
                                                               {"ex" "http://example.org/ns/"}]
                                                   "insert"
                                                   {"@type"          "sh:NodeShape",
                                                    "sh:targetClass" {"id" "schema:Person"}
                                                    "sh:property"
                                                    [{"sh:path"     {"id" "schema:familyName"}
                                                      "sh:datatype" {"id" "xsd:string"}}]}})
             property-query {"@context" [test-utils/default-str-context
                                         {"ex" "http://example.org/ns/"}]
                             :select    {"?s" ["*" {"sh:property" ["sh:path" "sh:datatype"]}]}
                             :where     {"id" "?s", "sh:property" "?property"}}
             shape-id       (-> @(fluree/query db1 property-query)
                                first
                                (get "id"))
             loaded1        (test-utils/retry-load conn ledger-alias 100)]
         (is (= [{"id"             shape-id
                  "type"           "sh:NodeShape",
                  "sh:targetClass" {"id" "schema:Person"},
                  "sh:property"    {"sh:path" {"id" "schema:familyName"}, "sh:datatype" {"id" "xsd:string"}}}]
                @(fluree/query db1 property-query)))
         (is (= [{"id"             shape-id
                  "type"           "sh:NodeShape",
                  "sh:targetClass" {"id" "schema:Person"},
                  "sh:property"    {"sh:path" {"id" "schema:familyName"}, "sh:datatype" {"id" "xsd:string"}}}]
                @(fluree/query (fluree/db loaded1) property-query)))
         (testing "load ref retracts"
           (let [db2     @(test-utils/transact loaded1
                                               {"@context" ["https://ns.flur.ee"
                                                            test-utils/default-str-context
                                                            {"ex" "http://example.org/ns/"}]
                                                "where"    [{"@id"         shape-id
                                                             "sh:property" "?prop"}
                                                            {"@id" "?prop"
                                                             "?p"  "?o"}]
                                                "delete"
                                                {"@id" "?prop" "?p" "?o"}
                                                "insert"
                                                {"@id"         "?prop"
                                                 "sh:path"     {"id" "schema:age"}
                                                 "sh:datatype" {"id" "xsd:string"}}})
                 loaded2 (test-utils/retry-load conn ledger-alias 100)]
             (is (= [{"id"             shape-id
                      "type"           "sh:NodeShape",
                      "sh:targetClass" {"id" "schema:Person"},
                      "sh:property"    {"sh:path" {"id" "schema:age"}, "sh:datatype" {"id" "xsd:string"}}}]
                    @(fluree/query (fluree/db loaded2) property-query)))))))
    (testing "can load after deletion of entire subjects"
      (let [conn              @(fluree/connect {:method :memory})
            ledger-alias      "tx/delete"
            ledger            @(fluree/create conn ledger-alias)
            db1               @(fluree/stage
                                 (fluree/db ledger)
                                 {"@context" ["https://ns.flur.ee"
                                              test-utils/default-context
                                              {:ex "http://example.org/ns/"}]
                                  "insert"
                                  {:graph
                                   [{:id                 :ex/fluree
                                     :type               :schema/Organization
                                     :schema/description "We ❤️ Data"}
                                    {:id                 :ex/w3c
                                     :type               :schema/Organization
                                     :schema/description "We ❤️ Internet"}
                                    {:id                 :ex/mosquitos
                                     :type               :ex/Monster
                                     :schema/description "We ❤️ human blood"}
                                    {:id                 :ex/kittens
                                     :type               :ex/Animal
                                     :schema/description "We ❤️ catnip"}]}})
            description-query {:context [test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                               :select  '{?s [:id]}
                               :where   '{:id ?s, :schema/description ?description}}
            _                 @(fluree/commit! ledger db1)
            loaded1           (test-utils/retry-load conn ledger-alias 100)
            loaded-db1        (fluree/db loaded1)
            db2               @(fluree/stage
                                 loaded-db1
                                 {"@context" ["https://ns.flur.ee"
                                              test-utils/default-context
                                              {:ex "http://example.org/ns/"}]
                                  "where"    {:id :ex/mosquitos, "?p" "?o"}
                                  "delete"   {:id :ex/mosquitos, "?p" "?o"}})
            _                 @(fluree/commit! ledger db2)
            loaded2           (test-utils/retry-load conn ledger-alias 100)
            loaded-db2        (fluree/db loaded2)]
        (is (= [{:id :ex/fluree} {:id :ex/w3c} {:id :ex/kittens}]
                @(fluree/query loaded-db2 description-query))
             "The id :ex/mosquitos should be removed")
        (let [db3        @(fluree/stage
                            loaded-db2
                            {"@context" ["https://ns.flur.ee"
                                         test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                             "delete"   {:id "?s", "?p" "?o"}
                             "where"    {:id   "?s"
                                         :type :schema/Organization
                                         "?p"  "?o"}})
              _          @(fluree/commit! ledger db3)
              loaded3    (test-utils/retry-load conn ledger-alias 100)
              loaded-db3 (fluree/db loaded3)]
          (is (= [{:id :ex/kittens}]
                  @(fluree/query loaded-db3 description-query))
               "Only :ex/kittens should be left"))))))

(deftest ^:integration query-test
  (let [query    {:context [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                  :select  ["?person" "?name"]
                  :where   {:id          "?person"
                            :type        :ex/User
                            :schema/name "?name"}}
        expected [[:ex/alice "Alice"]
                  [:ex/brian "Brian"]
                  [:ex/cam "Cam"]
                  [:ex/liam "Liam"]]]
    (testing "basic query works"
      #?(:clj
         (let [conn    (test-utils/create-conn)
               ledger  (test-utils/load-people conn)
               results @(fluree/query (fluree/db ledger) query)]
           (is (= expected results)))
         :cljs
         (async done
           (go
             (let [conn    (<! (test-utils/create-conn))
                   ledger  (<! (test-utils/load-people conn))
                   results (<p! (fluree/query (fluree/db ledger) query))]
               (is (= expected results))
               (done))))))))

(deftest ^:integration fuel-test
  #?(:clj
     (testing "fuel tracking"
       (let [conn   (test-utils/create-conn)
             ledger @(fluree/create conn "test/fuel-tracking")
             db0    (async/<!! (async-db/deref-async (fluree/db ledger)))]
         (testing "transactions"
           (testing "with the `:meta` option"
             (let [response    @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                               test-utils/default-context
                                                               {:ex "http://example.org/ns/"}]
                                                   "insert"   test-utils/people}
                                              {:meta true})
                   db          (:result response)
                   flake-total (- (-> db :stats :flakes)
                                  (-> db0 :stats :flakes))]

               (is (= flake-total
                      (:fuel response))
                   "Reports fuel for all the generated flakes")))
           (testing "without the `:meta` option"
             (let [response @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                            test-utils/default-context
                                                            {:ex "http://example.org/ns/"}]
                                                "insert"   test-utils/people})]
               (is (nil? (:fuel response))
                   "Returns no fuel")))
           (testing "short-circuits if request fuel exhausted"
             (let [response @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                            test-utils/default-context
                                                            {:ex "http://example.org/ns/"}]
                                                "insert"   test-utils/people}
                                           {:maxFuel 1})]
               (is (re-find #"Fuel limit exceeded"
                            (-> response ex-cause ex-message))))))
         (testing "queries"
           (let [db          @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                             test-utils/default-context
                                                             {:ex "http://example.org/ns/"}]
                                                 "insert"   test-utils/people})
                 flake-total (-> db :stats :flakes)
                 query       {:context test-utils/default-context
                              :select  '[?s ?p ?o]
                              :where   '{:id ?s, ?p ?o}}]
             (testing "queries not returning metadata"
               (let [sut @(fluree/query db query)]
                 (is (nil? (:fuel sut))
                     "Reports no fuel")))
             (testing "queries returning metadata"
               (let [query* (assoc-in query [:opts :meta] true)
                     sut    @(fluree/query db query*)]
                 (is (= flake-total (:fuel sut))
                     "Reports that all flakes were traversed"))))
           (testing "short-circuits if request fuel exhausted"
             (let [query   {:context test-utils/default-context
                            :select  '[?s ?p ?o]
                            :where   '{:id ?s
                                       ?p  ?o}
                            :opts    {:max-fuel 1}}
                   db      @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                           test-utils/default-context
                                                           {:ex "http://example.org/ns/"}]
                                               "insert" test-utils/people})
                   results @(fluree/query db query)]
               (is (util/exception? results))
               (is (re-find #"Fuel limit exceeded"
                            (-> results ex-cause ex-message))))))))
     :cljs
     (async done
       (go
         (testing "fuel tracking"
           (let [conn   (<! (test-utils/create-conn))
                 ledger (<p! (fluree/create conn "test/fuel-tracking"))
                 db0    (fluree/db ledger)]
             (testing "transactions"
               (testing "with the `:meta` option"
                 (let [response    (<p! (fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                                       test-utils/default-context
                                                                       {:ex "http://example.org/ns/"}]
                                                           "insert"   test-utils/people} {:meta true}))
                       db          (:result response)
                       flake-total (- (-> db :stats :flakes)
                                      (-> db0 :stats :flakes))]
                   (is (= flake-total (:fuel response))
                       "Reports fuel for all the generated flakes")))
               (testing "without the `:meta` option"
                 (let [response (<p! (fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                                    test-utils/default-context
                                                                    {:ex "http://example.org/ns/"}]
                                                        "insert"   test-utils/people}))]
                   (is (nil? (:fuel response))
                       "Returns no fuel")))
               (testing "short-circuits if request fuel exhausted"
                 (let [response (try
                                  (<p! (fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                                      test-utils/default-context
                                                                      {:ex "http://example.org/ns/"}]
                                                          "insert"   test-utils/people}
                                                     {:maxFuel 1}))
                                  (catch :default e (ex-cause e)))]
                   (is (re-find #"Fuel limit exceeded"
                                (-> response ex-cause ex-message))))))
             (testing "queries"
               (let [db          (<p! (fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                                     test-utils/default-context
                                                                     {:ex "http://example.org/ns/"}]
                                                         "insert"   test-utils/people}))
                     flake-total (-> db :stats :flakes)
                     query       {:context [test-utils/default-context
                                            {:ex "http://example.org/ns/"}]
                                  :select '[?s ?p ?o]
                                  :where  '{:id ?s
                                            ?p  ?o}}]
                 (testing "queries not returning metadata"
                   (let [sut (<p! (fluree/query db query))]
                     (is (nil? (:fuel sut))
                         "Reports no fuel")))
                 (testing "queries returning metadata"
                   (let [query* (assoc-in query [:opts :meta] true)
                         sut    (<p! (fluree/query db query*))]
                     (is (= flake-total (:fuel sut))
                         "Reports that all flakes were traversed"))))
               (testing "short-circuits if request fuel exhausted"
                 (let [query   {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select '[?s ?p ?o]
                                :where  '{:id ?s
                                          ?p  ?o}
                                :opts   {:max-fuel 1}}
                       db      (<p! (fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                                                   test-utils/default-context
                                                                   {:ex "http://example.org/ns/"}]
                                                       "insert"   test-utils/people}))
                       results (try
                                 (<p! (fluree/query db query))
                                 (catch :default e (ex-cause e)))]
                   (is (util/exception? results))
                   (is (re-find #"Fuel limit exceeded"
                                (-> results ex-cause ex-message))))))))
         (done)))))

#?(:clj
   (deftest transaction-test
     (let [conn      @(fluree/connect {:method :memory})
           ledger-id "update-syntax"
           ledger    @(fluree/create conn ledger-id)
           db0       (fluree/db ledger)

           db1 @(fluree/stage db0 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}]
                                   "insert"   [{"@id"       "ex:dp"
                                                "ex:name"   "Dan"
                                                "ex:child"  [{"@id" "ex:ap" "ex:name" "AP"}
                                                             {"@id" "ex:np" "ex:name" "NP"}]
                                                "ex:spouse" [{"@id"       "ex:kp" "ex:name" "KP"
                                                              "ex:spouse" {"@id" "ex:dp"}}]}]})

           db2 @(fluree/stage db1 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}]
                                   "where"    {"id" "?s", "ex:name" "?name"}
                                   "delete"   {"@id" "?s" "ex:name" "?name"}
                                   "insert"   {"@graph"
                                               [{"@id" "?s" "ex:name" "BORG"}
                                                {"@id"         "ex:mp"
                                                 "@type"       "ex:Cat"
                                                 "ex:isPerson" false
                                                 "ex:isOrange" true
                                                 "ex:nickname" {"@language" "en" "@value" "The Wretch"}
                                                 "ex:name"     "Murray"
                                                 "ex:address"
                                                 {"ex:street" "55 Bashford"
                                                  "ex:city"   "St. Paul"
                                                  "ex:zip"    {"@value" 55105 "@type" "ex:PostalCode"}
                                                  "ex:state"  "MN"}
                                                 "ex:favs"     {"@list" ["Persey" {"@id" "ex:dp"}]}}]}})
           db3 @(fluree/stage db2 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}]
                                   "insert"   {"@type"          "sh:NodeShape"
                                               "sh:targetClass" {"@id" "ex:Friend"}
                                               "sh:property"
                                               [{"sh:path"     {"@id" "ex:nickname"}
                                                 "sh:maxCount" 1
                                                 "sh:datatype" {"@id" "xsd:string"}}]}})

           db4 @(fluree/stage db3 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}]
                                   "insert"   {"@id"         "ex:mp"
                                               "@type"       "ex:Friend"
                                               "ex:nickname" "Murrseph Gordon-Levitt"}})

           root-did  (:id (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))
           alice-did (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))

           db5 @(fluree/stage db3 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}]
                                   "insert"   {"@graph"
                                               [{"@id" root-did "f:role" {"@id" "ex:rootRole"}}
                                                {"@id" alice-did "f:role" {"@id" "ex:userRole"} "ex:user" {"@id" "ex:alice"}}
                                                {"@id"              "ex:alice"
                                                 "@type"            "ex:User"
                                                 "schema:name"      "Alice"
                                                 "schema:email"     "alice@flur.ee"
                                                 "schema:birthDate" "2022-08-17"
                                                 "schema:ssn"       "111-11-1111"
                                                 "ex:address"       {"ex:state" "NC" "ex:country" "USA"}}
                                                {"@id"              "ex:john"
                                                 "@type"            "ex:User"
                                                 "schema:name"      "John"
                                                 "schema:email"     "john@flur.ee"
                                                 "schema:birthDate" "2022-08-17"
                                                 "schema:ssn"       "222-22-2222"
                                                 "ex:address"       {"ex:state" "SC" "ex:country" "USA"}}]}})

           db6 @(fluree/stage db5 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}],
                                   "insert"   [{"@id" "schema:givenName", "@type" "rdf:Property"}
                                               {"@id"                    "ex:firstName",
                                                "@type"                  "rdf:Property",
                                                "owl:equivalentProperty" {"@id" "schema:givenName"}}
                                               {"@id"                    "foaf:name",
                                                "@type"                  "rdf:Property",
                                                "owl:equivalentProperty" {"@id" "ex:firstName"}}]})

           db7 @(fluree/stage db6 {"@context" ["https://ns.flur.ee"
                                               test-utils/default-str-context
                                               {"ex" "ns:ex/"}],
                                   "insert"   [{"@id" "ex:andrew", "schema:givenName" "Andrew"}
                                               {"@id" "ex:freddy", "foaf:name" "Freddy"}
                                               {"@id" "ex:letty", "ex:firstName" "Leticia"}
                                               {"@id" "ex:betty", "ex:firstName" "Betty"}]})

           committed @(fluree/commit! ledger db7)
           loaded    @(fluree/load conn ledger-id)]
       (is (= #{"AP" "Dan" "KP" "NP"}
              (into #{} @(fluree/query db1 {"@context" [test-utils/default-str-context
                                                        {"ex" "ns:ex/"}]
                                            "where"    {"id" "?s", "ex:name" "?name"}
                                            "select"   "?name"}))))

       (is (= {"BORG" 4 "Murray" 1}
              (frequencies @(fluree/query db2 {"@context" [test-utils/default-str-context
                                                           {"ex" "ns:ex/"}]
                                               "where"    {"id" "?s", "ex:name" "?name"}
                                               "select"   "?name"}))))
       (is (= [{"id"          "ex:mp"
                "type"        "ex:Cat"
                "ex:name"     "Murray"
                "ex:favs"     ["Persey" {"id" "ex:dp"}]
                "ex:address"  {"ex:street" "55 Bashford" "ex:city" "St. Paul" "ex:zip" 55105 "ex:state" "MN"}
                "ex:isOrange" true
                "ex:isPerson" false
                "ex:nickname" "The Wretch"}]
              @(fluree/query db2 {"@context" [test-utils/default-str-context
                                              {"ex" "ns:ex/"}]
                                  "where"    {"id" "?s", "ex:name" "Murray"}
                                  "select"   {"?s" ["*" {"ex:address" ["ex:street" "ex:city" "ex:state" "ex:zip"]}]}})))

       (is (= "Subject ex:mp path [\"ex:nickname\"] violates constraint sh:datatype of shape _:fdb-5 - the following values do not have expected datatype xsd:string: The Wretch.
Subject ex:mp path [\"ex:nickname\"] violates constraint sh:maxCount of shape _:fdb-5 - count 2 is greater than maximum count of 1."
              (ex-message db4)))


       (is (= #{"Freddy" "Betty" "Leticia" "Andrew"}
              (set @(fluree/query db7 {"@context"       test-utils/default-str-context
                                       "selectDistinct" "?name",
                                       "where"          {"id"               "?s"
                                                         "schema:givenName" "?name"}})))
           "equivalentProperty annotations work")

       (is (= 54
              (-> @(fluree/history ledger {:context        test-utils/default-str-context
                                           :commit-details true
                                           :t              {:from :latest}})
                  (first)
                  (get "f:commit")
                  (get "f:data")
                  (get "f:flakes"))))
       (is (= 54
              (-> @(fluree/history loaded {:context        test-utils/default-str-context
                                           :commit-details true
                                           :t              {:from :latest}})
                  (first)
                  (get "f:commit")
                  (get "f:data")
                  (get "f:flakes")))))))
