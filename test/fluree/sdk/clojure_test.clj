(ns fluree.sdk.clojure-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.dbproto :as dbproto]
            [fluree.sdk.clojure :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))

(deftest exists?-test
  (testing "returns false before committing data to a ledger"
    (let [conn         (test-utils/create-conn)
          ledger-alias "testledger"
          check1       @(fluree/exists? conn ledger-alias)
          ledger       @(fluree/create conn ledger-alias)
          check2       @(fluree/exists? conn ledger-alias)
          _            @(fluree/stage (fluree/db ledger)
                                      [{:id           :f/me
                                        :type         :schema/Person
                                        :schema/fname "Me"}])
          check3       @(fluree/exists? conn ledger-alias)]
      (is (every? false? [check1 check2 check3]))))
  (testing "returns true after committing data to a ledger"
    (let [conn         (test-utils/create-conn)
          ledger-alias "testledger"
          ledger       @(fluree/create conn ledger-alias)
          db           @(fluree/stage (fluree/db ledger)
                                      [{:id           :f/me
                                        :type         :schema/Person
                                        :schema/fname "Me"}])]
      @(fluree/commit! ledger db)
      (is (test-utils/retry-exists? conn ledger-alias 100))
      (is (not @(fluree/exists? conn "notaledger"))))))

(deftest create-test
  (testing "string ledger context gets correctly merged with keyword conn context"
    (let [conn           (test-utils/create-conn)
          ledger-alias   "testledger"
          ledger-context {:ex  "http://example.com/"
                          :foo "http://foobar.com/"}
          ledger         @(fluree/create conn ledger-alias
                                         {:defaults
                                          {:context ["" ledger-context]}})
          merged-context (merge test-utils/default-context
                                (util/stringify-keys ledger-context))]
      (is (= merged-context (dbproto/-default-context (fluree/db ledger)))
          (str "merged context is: " (pr-str merged-context))))))

(deftest load-from-file-test
  (testing "can load a file ledger with single cardinality predicates"
    (with-tmp-dir storage-path
      (let [conn         @(fluree/connect
                           {:method :file, :storage-path storage-path
                            :defaults
                            {:context test-utils/default-context}})
            ledger-alias "load-from-file-test-single-card"
            ledger       @(fluree/create conn ledger-alias
                                         {:defaults
                                          {:context
                                           ["" {:ex "http://example.org/ns/"}]}})
            db           @(fluree/stage
                           (fluree/db ledger)
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
                             :ex/friend    :ex/brian}])
            db           @(fluree/commit! ledger db)
            db           @(fluree/stage
                           db
                           ;; test a retraction
                           {:f/retract {:id         :ex/brian
                                        :ex/favNums 7}})
            _            @(fluree/commit! ledger db)
            ;; TODO: Replace this w/ :syncTo equivalent once we have it
            loaded       (test-utils/retry-load conn ledger-alias 100)
            loaded-db    (fluree/db loaded)]
        (is (= (:t db) (:t loaded-db)))
        (is (= (dbproto/-default-context db) (dbproto/-default-context loaded-db))))))

  (testing "can load a file ledger with multi-cardinality predicates"
    (with-tmp-dir storage-path
      (let [conn         @(fluree/connect
                           {:method :file, :storage-path storage-path
                            :defaults
                            {:context test-utils/default-context}})
            ledger-alias "load-from-file-test-multi-card"
            ledger       @(fluree/create conn ledger-alias)
            db           @(fluree/stage
                           (fluree/db ledger)
                           [{:context      {:ex "http://example.org/ns/"}
                             :id           :ex/brian
                             :type         :ex/User
                             :schema/name  "Brian"
                             :schema/email "brian@example.org"
                             :schema/age   50
                             :ex/favNums   7}

                            {:context      {:ex "http://example.org/ns/"}
                             :id           :ex/alice
                             :type         :ex/User
                             :schema/name  "Alice"
                             :schema/email "alice@example.org"
                             :schema/age   50
                             :ex/favNums   [42 76 9]}

                            {:context      {:ex "http://example.org/ns/"}
                             :id           :ex/cam
                             :type         :ex/User
                             :schema/name  "Cam"
                             :schema/email "cam@example.org"
                             :schema/age   34
                             :ex/favNums   [5 10]
                             :ex/friend    [:ex/brian :ex/alice]}])
            db           @(fluree/commit! ledger db)
            db           @(fluree/stage
                           db
                           ;; test a multi-cardinality retraction
                           [{:context   {:ex "http://example.org/ns/"}
                             :f/retract {:id         :ex/alice
                                         :ex/favNums [42 76 9]}}])
            _            @(fluree/commit! ledger db)
            ;; TODO: Replace this w/ :syncTo equivalent once we have it
            loaded       (test-utils/retry-load conn ledger-alias 100)
            loaded-db    (fluree/db loaded)]
        (is (= (:t db) (:t loaded-db)))
        (is (= (:context ledger) (:context loaded))))))

  (testing "can load a file ledger with its own context"
    (with-tmp-dir storage-path #_{::twf/delete-dir false}
      #_(println "storage path:" storage-path)
      (let [conn-context   {:id  "@id", :type "@type"
                            :xsd "http://www.w3.org/2001/XMLSchema#"}
            ledger-context {:ex     "http://example.com/"
                            :schema "http://schema.org/"}
            conn           @(fluree/connect
                             {:method   :file :storage-path storage-path
                              :defaults {:context conn-context}})
            ledger-alias   "load-from-file-with-context"
            ledger         @(fluree/create conn ledger-alias
                                           {:defaults {:context
                                                       ["" ledger-context]}})
            db             @(fluree/stage
                             (fluree/db ledger)
                             [{:id             :ex/wes
                               :type           :ex/User
                               :schema/name    "Wes"
                               :schema/email   "wes@example.org"
                               :schema/age     42
                               :schema/favNums [1 2 3]
                               :ex/friend      {:id           :ex/jake
                                                :type         :ex/User
                                                :schema/name  "Jake"
                                                :schema/email "jake@example.org"}}])
            db             @(fluree/commit! ledger db)
            loaded         (test-utils/retry-load conn ledger-alias 100)
            loaded-db      (fluree/db loaded)
            merged-ctx     (merge (util/stringify-keys conn-context)
                                  (util/stringify-keys ledger-context))
            query          {:where  '[[?p :schema/email "wes@example.org"]]
                            :select '{?p [:*]}}
            results        @(fluree/query loaded-db query)
            full-type-url  "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
        (is (= (:t db) (:t loaded-db)))
        (is (= merged-ctx (dbproto/-default-context (fluree/db loaded))))
        (is (= [{full-type-url   [:ex/User]
                 :id             :ex/wes
                 :schema/age     42
                 :schema/email   "wes@example.org"
                 :schema/favNums [1 2 3]
                 :schema/name    "Wes"
                 :ex/friend      {:id :ex/jake}}]
               results)))))

  (testing "query returns the correct results from a loaded ledger"
    (with-tmp-dir storage-path
      (let [conn-context   {:id "@id", :type "@type"}
            ledger-context {:ex     "http://example.com/"
                            :schema "http://schema.org/"}
            conn           @(fluree/connect
                             {:method   :file :storage-path storage-path
                              :defaults {:context conn-context}})
            ledger-alias   "load-from-file-query"
            ledger         @(fluree/create conn ledger-alias
                                           {:defaults {:context
                                                       ["" ledger-context]}})
            db             @(fluree/stage
                             (fluree/db ledger)
                             [{:id          :ex/Andrew
                               :type        :schema/Person
                               :schema/name "Andrew"
                               :ex/friend   {:id          :ex/Jonathan
                                             :type        :schema/Person
                                             :schema/name "Jonathan"}}])
            query          {:select '{?s [:*]}
                            :where  '[[?s :id :ex/Andrew]]}
            res1           @(fluree/query db query)
            _              @(fluree/commit! ledger db)
            loaded         (test-utils/retry-load conn ledger-alias 100)
            loaded-db      (fluree/db loaded)
            res2           @(fluree/query loaded-db query)]
        (is (= res1 res2)))))

  (testing "can load a ledger with `list` values"
    (with-tmp-dir storage-path
      (let [conn         @(fluree/connect
                           {:method       :file
                            :storage-path storage-path
                            :defaults
                            {:context (merge (util/keywordize-keys
                                              test-utils/default-context)
                                             {:ex   "http://example.org/ns/"
                                              :list "@list"})}})
            ledger-alias "load-lists-test"
            ledger       @(fluree/create conn ledger-alias)
            db           @(fluree/stage
                           (fluree/db ledger)
                           [{:id         :ex/alice
                             :type       :ex/User
                             :ex/friends {:list [:ex/john :ex/cam]}}
                            {:id         :ex/cam
                             :type       :ex/User
                             :ex/numList {:list [7 8 9 10]}}
                            {:id   :ex/john
                             :type :ex/User}])
            db           @(fluree/commit! ledger db)
            loaded       (test-utils/retry-load conn ledger-alias 100)
            loaded-db    (fluree/db loaded)]
        (is (= (:t db) (:t loaded-db)))
        (testing "query returns expected `list` values"
          (is (= #{{:id         :ex/cam
                    :rdf/type   [:ex/User]
                    :ex/numList [7 8 9 10]}
                   {:id :ex/john :rdf/type [:ex/User]}
                   {:id         :ex/alice
                    :rdf/type   [:ex/User]
                    :ex/friends [:ex/john :ex/cam]}}
                 (set
                  @(fluree/query loaded-db '{:select {?s [:*]}
                                             :where  [[?s :rdf/type :ex/User]]})))))))

    (testing "can load with policies"
      (with-tmp-dir storage-path
        (let [conn         @(fluree/connect
                             {:method       :file
                              :storage-path storage-path
                              :defaults
                              {:context (merge (util/keywordize-keys
                                                test-utils/default-context)
                                               {:ex   "http://example.org/ns/"
                                                :list "@list"})}})
              ledger-alias "load-policy-test"
              ledger       @(fluree/create conn ledger-alias)
              db           @(fluree/stage
                             (fluree/db ledger)
                             [{:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :schema/ssn  "111-11-1111"
                               :ex/friend   {:id :ex/john}}
                              {:id          :ex/john
                               :schema/name "John"
                               :type        :ex/User
                               :schema/ssn  "888-88-8888"}
                              {:id      "did:fluree:123"
                               :ex/user {:id :ex/alice}
                               :f/role  {:id :ex/userRole}}])
              db+policy    @(fluree/stage
                             db
                             [{:id            :ex/UserPolicy
                               :type          [:f/Policy]
                               :f/targetClass {:id :ex/User}
                               :f/allow       [{:id           :ex/globalViewAllow
                                                :f/targetRole {:id :ex/userRole}
                                                :f/action     [{:id :f/view}]}]
                               :f/property
                               [{:f/path {:id :schema/ssn}
                                 :f/allow
                                 [{:id           :ex/ssnViewRule
                                   :f/targetRole {:id :ex/userRole}
                                   :f/action     [{:id :f/view}]
                                   :f/equals     {:list [{:id :f/$identity}
                                                         {:id :ex/user}]}}]}]}])
              db+policy    @(fluree/commit! ledger db+policy)
              loaded       (test-utils/retry-load conn ledger-alias 100)
              loaded-db    (fluree/db loaded)]
          (is (= (:t db) (:t loaded-db)))
          (testing "query returns expected policy"
            (is (= [{:id            :ex/UserPolicy
                     :rdf/type      [:f/Policy]
                     :f/allow
                     {:id           :ex/globalViewAllow
                      :f/action     {:id :f/view}
                      ;; TODO: We can likely make "_id" come back as :_id instead
                      ;;       but need to think through where this should happen
                      :f/targetRole {"_id" 211106232532995}}
                     :f/property
                     {:id     "_:f211106232532999"
                      :f/allow
                      {:id           :ex/ssnViewRule
                       :f/action     {:id :f/view}
                       :f/targetRole {"_id" 211106232532995}
                       :f/equals     [{:id :f/$identity} {:id :ex/user}]}
                      :f/path {:id :schema/ssn}},
                     :f/targetClass {:id :ex/User}}]
                   @(fluree/query
                     loaded-db
                     '{:select {?s [:*
                                    {:rdf/type ["_id"]}
                                    {:f/allow [:* {:f/targetRole ["_id"]}]}
                                    {:f/property [:* {:f/allow [:* {:f/targetRole ["_id"]}]}]}]}
                       :where  [[?s :rdf/type :f/Policy]]}))))))))

  (testing "can load a ledger with `list` values"
    (with-tmp-dir storage-path
      (let [conn         @(fluree/connect
                           {:method       :file
                            :storage-path storage-path
                            :defaults
                            {:context (merge (util/keywordize-keys
                                              test-utils/default-context)
                                             {:ex   "http://example.org/ns/"
                                              :list "@list"})}})
            ledger-alias "load-lists-test"
            ledger       @(fluree/create conn ledger-alias)
            db           @(fluree/stage
                           (fluree/db ledger)
                           [{:id         :ex/alice
                             :type       :ex/User
                             ;; TODO: We can likely add support of inference of
                             ;;       nodes w/ keywords here if we want to
                             :ex/friends {:list [{:id :ex/john} {:id :ex/cam}]}}
                            {:id         :ex/cam
                             :type       :ex/User
                             :ex/numList {:list [7 8 9 10]}}
                            {:id   :ex/john
                             :type :ex/User}])
            db           @(fluree/commit! ledger db)
            loaded       (test-utils/retry-load conn ledger-alias 100)
            loaded-db    (fluree/db loaded)]
        (is (= (:t db) (:t loaded-db)))
        (testing "query returns expected `list` values"
          (is (= #{{:id         :ex/cam
                    :rdf/type   [:ex/User]
                    :ex/numList [7 8 9 10]}
                   {:id :ex/john, :rdf/type [:ex/User]}
                   {:id         :ex/alice
                    :rdf/type   [:ex/User]
                    :ex/friends [{:id :ex/john} {:id :ex/cam}]}}
                 (set
                  @(fluree/query loaded-db '{:select {?s [:*]}
                                             :where  [[?s :rdf/type :ex/User]]})))))))

    (testing "can load with policies"
      (with-tmp-dir storage-path
        (let [conn         @(fluree/connect
                             {:method       :file
                              :storage-path storage-path
                              :defaults
                              {:context (merge (util/keywordize-keys
                                                test-utils/default-context)
                                               {:ex   "http://example.org/ns/"
                                                :list "@list"})}})
              ledger-alias "load-policy-test"
              ledger       @(fluree/create conn ledger-alias)
              db           @(fluree/stage
                             (fluree/db ledger)
                             [{:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :schema/ssn  "111-11-1111"
                               :ex/friend   {:id :ex/john}}
                              {:id          :ex/john
                               :schema/name "John"
                               :type        :ex/User
                               :schema/ssn  "888-88-8888"}
                              {:id      "did:fluree:123"
                               :ex/user {:id :ex/alice}
                               :f/role  {:id :ex/userRole}}])
              db+policy    @(fluree/stage
                             db
                             [{:id            :ex/UserPolicy
                               :type          [:f/Policy]
                               :f/targetClass {:id :ex/User}
                               :f/allow       [{:id           :ex/globalViewAllow
                                                :f/targetRole {:id :ex/userRole}
                                                :f/action     [{:id :f/view}]}]
                               :f/property
                               [{:f/path {:id :schema/ssn}
                                 :f/allow
                                 [{:id           :ex/ssnViewRule
                                   :f/targetRole {:id :ex/userRole}
                                   :f/action     [{:id :f/view}]
                                   :f/equals     {:list [{:id :f/$identity}
                                                         {:id :ex/user}]}}]}]}])
              db+policy    @(fluree/commit! ledger db+policy)
              loaded       (test-utils/retry-load conn ledger-alias 100)
              loaded-db    (fluree/db loaded)]
          (is (= (:t db) (:t loaded-db)))
          (testing "query returns expected policy"
            (is (= [{:id       :ex/UserPolicy
                     :rdf/type [:f/Policy]
                     :f/allow
                     {:id           :ex/globalViewAllow
                      :f/action     {:id :f/view}
                      :f/targetRole {"_id" 211106232532995}}
                     :f/property
                     {:id     "_:f211106232532999"
                      :f/allow
                      {:id           :ex/ssnViewRule
                       :f/action     {:id :f/view}
                       :f/targetRole {"_id" 211106232532995}
                       :f/equals     [{:id :f/$identity} {:id :ex/user}]}
                      :f/path {:id :schema/ssn}}
                     :f/targetClass {:id :ex/User}}]
                   @(fluree/query
                     loaded-db
                     '{:select {?s [:*
                                    {:rdf/type ["_id"]}
                                    {:f/allow [:* {:f/targetRole ["_id"]}]}
                                    {:f/property [:* {:f/allow [:* {:f/targetRole ["_id"]}]}]}]}
                       :where  [[?s :rdf/type :f/Policy]]})))))))))

(deftest multi-query-test
  (let [conn   (test-utils/create-conn
                {:context (merge test-utils/default-context
                                 {"ex" "http://example.org/ns/"})})
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "multi queries"
      (let [q       '{:alice {:select {?s [:*]}
                              :where  [[?s :schema/email "alice@example.org"]]}
                      :brian {:select {?s [:*]}
                              :where  [[?s :schema/email "brian@example.org"]]}}
            subject @(fluree/multi-query db q)]
        (is (= {:alice [{:id           :ex/alice
                         :rdf/type     [:ex/User]
                         :ex/favNums   [9 42 76]
                         :schema/age   50
                         :schema/email "alice@example.org"
                         :schema/name  "Alice"}]
                :brian [{:id           :ex/brian
                         :rdf/type     [:ex/User]
                         :ex/favNums   7
                         :schema/age   50
                         :schema/email "brian@example.org"
                         :schema/name  "Brian"}]}
               subject)
            "returns all results in a map keyed by alias.")))))

(deftest history-query-test
  (let [ts-primeval (util/current-time-iso)

        conn        (test-utils/create-conn)
        ledger      @(fluree/create conn "historytest"
                                    {:defaults
                                     {:context
                                      ["" {:ex "http://example.org/ns/"}]}})

        db1         @(test-utils/transact-clj ledger [{:id   :ex/dan
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}
                                                      {:id   :ex/cat
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}
                                                      {:id   :ex/dog
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}])
        db2         @(test-utils/transact-clj ledger {:id   :ex/dan
                                                      :ex/x "foo-2"
                                                      :ex/y "bar-2"})
        ts2         (-> db2 :commit :time)
        db3         @(test-utils/transact-clj ledger {:id   :ex/dan
                                                      :ex/x "foo-3"
                                                      :ex/y "bar-3"})

        ts3         (-> db3 :commit :time)
        db4         @(test-utils/transact-clj ledger [{:id   :ex/cat
                                                       :ex/x "foo-cat"
                                                       :ex/y "bar-cat"}
                                                      {:id   :ex/dog
                                                       :ex/x "foo-dog"
                                                       :ex/y "bar-dog"}])
        db5         @(test-utils/transact-clj ledger {:id   :ex/dan
                                                      :ex/x "foo-cat"
                                                      :ex/y "bar-cat"})]
    (testing "subject history"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]
               :f/retract [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]}
              {:f/t       3
               :f/assert  [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]
               :f/retract [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]}
              {:f/t       5
               :f/assert  [{:id :ex/dan :ex/x "foo-cat" :ex/y "bar-cat"}]
               :f/retract [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]}]
             @(fluree/history ledger {:history :ex/dan, :t {:from 1}}))))
    (testing "one-tuple flake history"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan] :t {:from 1}}))))
    (testing "two-tuple flake history"
      (is (= [{:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dan}] :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 1}})))

      (is (= [{:f/t       1 :f/assert [{:ex/x "foo-1" :id :ex/dog}
                                       {:ex/x "foo-1" :id :ex/cat}
                                       {:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       4
               :f/assert  [{:ex/x "foo-dog" :id :ex/dog}
                           {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog}
                           {:ex/x "foo-1" :id :ex/cat}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 1}}))))
    (testing "three-tuple flake history"
      (is (= [{:f/t 4 :f/assert [{:ex/x "foo-cat" :id :ex/cat}] :f/retract []}
              {:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x "foo-cat"] :t {:from 1}})))
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract []}
              {:f/t       3
               :f/assert  []
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x "foo-2"] :t {:from 1}})))
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x "foo-cat"] :t {:from 1}}))))

    (testing "at-t"
      (let [expected [{:f/t       3
                       :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
                       :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]]
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3 :to 3}})))
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:at 3}})))))
    (testing "from-t"
      (is (= [{:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3}}))))
    (testing "to-t"
      (is (= [{:f/t       1
               :f/assert  [{:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:to 3}}))))
    (testing "t-range"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       4
               :f/assert  [{:ex/x "foo-dog" :id :ex/dog} {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog} {:ex/x "foo-1" :id :ex/cat}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 2 :to 4}}))))
    (testing "datetime-t"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from ts2 :to ts3}}))
          "does not include t 1 4 or 5")
      (is (= [{:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from (util/current-time-iso)}}))
          "timestamp translates to first t before ts")

      (is (= (str "There is no data as of " ts-primeval)
             (-> @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from ts-primeval}})
                 (Throwable->map)
                 :cause))))

    (testing "invalid query"
      (is (thrown? Exception @(fluree/history ledger {:history []}))))

    (testing "small cache"
      (let [conn   (test-utils/create-conn)
            ledger @(fluree/create conn "historycachetest"
                                   {:defaults
                                    {:context
                                     ["" {:ex "http://example.org/ns/"}]}})

            db1    @(test-utils/transact-clj ledger [{:id   :ex/dan
                                                      :ex/x "foo-1"
                                                      :ex/y "bar-1"}])
            db2    @(test-utils/transact-clj ledger {:id   :ex/dan
                                                     :ex/x "foo-2"
                                                     :ex/y "bar-2"})]
        (testing "no t-range cache collision"
          (is (= [{:f/t       2
                   :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:history [:ex/dan] :t {:from 2}}))))))))
