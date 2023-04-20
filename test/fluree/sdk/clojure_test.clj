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
        (is (= (:context ledger) (:context loaded))))))

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

(deftest ^:integration multi-query-test
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
