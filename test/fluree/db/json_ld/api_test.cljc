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
         (is @(fluree/exists? conn ledger-alias))
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
             (is (<p! (fluree/exists? conn ledger-alias)))
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
         (println "ledger context:" (pr-str (:context ledger)))
         (is (= merged-context (:context ledger)))))))

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
         (let [conn-context   {:id "@id", :type "@type"}
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
                                   :schema/favNums [1 2 3]}])
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
                    :schema/name    "Wes"}]
                  results)))))))
