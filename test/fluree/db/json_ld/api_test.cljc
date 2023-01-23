(ns fluree.db.json-ld.api-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            #?@(:cljs [[clojure.core.async :refer [go <!]]
                       [clojure.core.async.interop :refer [<p!]]])
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            #?(:clj [test-with-files.tools :refer [with-tmp-dir]])))

(deftest exists?-test
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

#?(:clj
   (deftest load-from-file-test
     (testing "can load a file ledger with single cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                               {:method :file :storage-path storage-path
                                :defaults
                                {:context test-utils/default-context}})
               ledger-alias "transact-basic-test-single-card"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                               (fluree/db ledger)
                               [{:context      {:ex "http://example.org/ns/"}
                                 :id           :ex/brian,
                                 :type         :ex/User,
                                 :schema/name  "Brian"
                                 :schema/email "brian@example.org"
                                 :schema/age   50
                                 :ex/favNums   7}

                                {:context      {:ex "http://example.org/ns/"}
                                 :id           :ex/cam,
                                 :type         :ex/User,
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
               _            (Thread/sleep 1000)
               loaded       @(fluree/load conn ledger-alias)]
           (if (instance? Throwable loaded)
             (throw loaded)
             (is (= (:t db) (:t (fluree/db loaded))))))))

     (testing "can load a file ledger with multi-cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                               {:method :file :storage-path storage-path
                                :defaults
                                {:context test-utils/default-context}})
               ledger-alias "transact-basic-test-multi-card"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                               (fluree/db ledger)
                               [{:context      {:ex "http://example.org/ns/"}
                                 :id           :ex/brian,
                                 :type         :ex/User,
                                 :schema/name  "Brian"
                                 :schema/email "brian@example.org"
                                 :schema/age   50
                                 :ex/favNums   7}

                                {:context      {:ex "http://example.org/ns/"}
                                 :id           :ex/alice,
                                 :type         :ex/User,
                                 :schema/name  "Alice"
                                 :schema/email "alice@example.org"
                                 :schema/age   50
                                 :ex/favNums   [42, 76, 9]}

                                {:context      {:ex "http://example.org/ns/"}
                                 :id           :ex/cam,
                                 :type         :ex/User,
                                 :schema/name  "Cam"
                                 :schema/email "cam@example.org"
                                 :schema/age   34
                                 :ex/favNums   [5, 10]
                                 :ex/friend    [:ex/brian :ex/alice]}])
               db           @(fluree/commit! ledger db)
               db           @(fluree/stage
                               db
                               ;; test a multi-cardinality retraction
                               [{:context   {:ex "http://example.org/ns/"}
                                 :f/retract {:id         :ex/alice
                                             :ex/favNums [42, 76, 9]}}])
               _            @(fluree/commit! ledger db)
               ;; TODO: Replace this w/ :syncTo equivalent once we have it
               _            (Thread/sleep 1000)
               loaded       @(fluree/load conn ledger-alias)]
           (if (instance? Throwable loaded)
             (throw loaded)
             (is (= (:t db) (:t (fluree/db loaded))))))))))
