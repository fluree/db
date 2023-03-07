(ns fluree.db.json-ld.api-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            #?@(:cljs [[clojure.core.async :refer [go <!]]
                       [clojure.core.async.interop :refer [<p!]]])
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            #?(:clj  [test-with-files.tools :refer [with-tmp-dir]
                      :as twf]
               :cljs [test-with-files.tools :as-alias twf])))

(deftest exists?-test
  (testing "returns false before committing data to a ledger"
    #?(:clj
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
         (is (every? false? [check1 check2 check3])))))
  (testing "returns true after committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage (fluree/db ledger)
                                         [{:id           :f/me
                                           :type         :schema/Person
                                           :schema/fname "Me"}])]
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
                                                 [{:id           :f/me
                                                   :type         :schema/Person
                                                   :schema/fname "Me"}]))]
             (<p! (fluree/commit! ledger db))
             (is (test-utils/retry-exists? conn ledger-alias 100))
             (is (not (<p! (fluree/exists? conn "notaledger"))))
             (done)))))))

(deftest create-test
  (testing "string ledger context gets correctly merged with keyword conn context"
    #?(:clj
       (let [conn           (test-utils/create-conn)
             ledger-alias   "testledger"
             ledger-context {"ex"  "http://example.com/"
                             "foo" "http://foobar.com/"}
             ledger         @(fluree/create conn ledger-alias
                                            {:context-type :string
                                             :context      ledger-context})
             merged-context (merge test-utils/default-context
                                   (util/keywordize-keys ledger-context))]
         (is (= merged-context (:context ledger)))))))

(deftest context-test
  (testing "transact context"
    (let [conn (test-utils/create-conn)
          ledger @(fluree/create conn "context-test" {:context-type :string})
          db0 (fluree/db ledger)
          db1 @(fluree/stage db0 {"@context" {"ex" "http://example.com/"}
                                   "@id" "ex:dan"
                                   "ex:x" 1})
          db2 @(fluree/stage db1 {"@context" {"foo" "http://example.com/"}
                                  "@id" "foo:dan"
                                  "foo:y" "y"})]
      (testing "keyword context"
        (is (= [{:id :foo/dan, :foo/x 1 :foo/y "y"}]
               @(fluree/query db2 {:context {:foo "http://example.com/"}
                                    :where [['?s :id :foo/dan]]
                                    :select {'?s [:*]}}))))

      (testing "string context"
        (is (= [{:id "foo:dan", "foo:x" 1 "foo:y" "y"}]
               @(fluree/query db2 {"@context" {"foo" "http://example.com/"}
                                    :where [["?s" "@id" "foo:dan"]]
                                    :select {"?s" ["*"]}}))))
      (testing "string context, context-type string"
        (is (= [{"id" "foo:dan", "foo:x" 1 "foo:y" "y"}]
               @(fluree/query db2 {"@context" {"foo" "http://example.com/"}
                                    :where [["?s" "@id" "foo:dan"]]
                                    :select {"?s" ["*"]}
                                    :opts {:context-type :string}}))))
      (testing "nil context"
        (is (= [{"@id" "http://example.com/dan"
                 "http://example.com/x" 1
                 "http://example.com/y" "y"}]
               @(fluree/query db2 {"@context" nil
                                    :where [["?s" "@id" "http://example.com/dan"]]
                                    :select {"?s" ["*"]}
                                    :opts {:context-type :string}}))
            "should be fully expanded"))
      (testing "vector context"
        (is (= [{"@id" "foo:dan", "foo:x" 1, "foo:y" "y"}]
               @(fluree/query db2 {"@context" [nil {"foo" "http://example.com/"}]
                                    :where [["?s" "@id" "http://example.com/dan"]]
                                    :select {"?s" ["*"]}
                                    :opts {:context-type :string}})))
        (is (= [{"@id" "http://example.com/dan"
                 "http://example.com/x" 1
                 "http://example.com/y" "y"}]
               @(fluree/query db2 {"@context" [{"foo" "http://example.com/"} nil]
                                    :where [["?s" "@id" "http://example.com/dan"]]
                                    :select {"?s" ["*"]}
                                    :opts {:context-type :string}})))))))

#?(:clj
   (deftest load-from-file-test
     (testing "can load a file ledger with single cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                               {:method :file :storage-path storage-path
                                :defaults
                                {:context test-utils/default-context}})
               ledger-alias "load-from-file-test-single-card"
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
                                 :id           :ex/cam
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
                               {:context   {:ex "http://example.org/ns/"}
                                :f/retract {:id         :ex/brian
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
                               {:method :file :storage-path storage-path
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
                                              {:context ledger-context})
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
               merged-ctx     (merge conn-context ledger-context)
               query          {:where  '[[?p :schema/email "wes@example.org"]]
                               :select '{?p [:*]}}
               results        @(fluree/query loaded-db query)
               full-type-url  "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
           (is (= (:t db) (:t loaded-db)))
           (is (= merged-ctx (:context loaded)))
           (is (= (get-in db [:schema :context])
                  (get-in loaded-db [:schema :context])))
           (is (= (get-in db [:schema :context-str])
                  (get-in loaded-db [:schema :context-str])))
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
                                              {:context ledger-context})
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
           (is (= res1 res2)))))))
