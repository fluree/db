(ns fluree.db.query.history-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.util :as util]
            [fluree.db.util.json :as json]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration history-query-test
  (let [ts-primeval (util/current-time-iso)

        conn      (test-utils/create-conn)
        ledger-id "historytest"
        context   [test-utils/default-context {:ex "http://example.org/ns/"}]

        _db1 @(fluree/create-with-txn conn {"@context" context
                                            "ledger"   ledger-id
                                            "insert"   [{:id   :ex/dan
                                                         :ex/x "foo-1"
                                                         :ex/y "bar-1"}
                                                        {:id   :ex/cat
                                                         :ex/x "foo-1"
                                                         :ex/y "bar-1"}
                                                        {:id   :ex/dog
                                                         :ex/x "foo-1"
                                                         :ex/y "bar-1"}]})
        db2  @(fluree/transact! conn {"@context" context
                                      "ledger"   ledger-id
                                      "delete"   {:id   :ex/dan
                                                  :ex/x "foo-1"
                                                  :ex/y "bar-1"}
                                      "insert"   {:id   :ex/dan
                                                  :ex/x "foo-2"
                                                  :ex/y "bar-2"}})
        ts2  (-> db2 :commit :time)
        db3  @(fluree/transact! conn {"@context" context
                                      "ledger"   ledger-id
                                      "delete"   {:id   :ex/dan
                                                  :ex/x "foo-2"
                                                  :ex/y "bar-2"}
                                      "insert"   {:id   :ex/dan
                                                  :ex/x "foo-3"
                                                  :ex/y "bar-3"}})

        ts3    (-> db3 :commit :time)
        _db4   @(fluree/transact! conn {"@context" context
                                        "ledger"   ledger-id
                                        "delete"   [{:id   :ex/cat
                                                     :ex/x "foo-1"
                                                     :ex/y "bar-1"}
                                                    {:id   :ex/dog
                                                     :ex/x "foo-1"
                                                     :ex/y "bar-1"}]
                                        "insert"   [{:id   :ex/cat
                                                     :ex/x "foo-cat"
                                                     :ex/y "bar-cat"}
                                                    {:id   :ex/dog
                                                     :ex/x "foo-dog"
                                                     :ex/y "bar-dog"}]})
        _db5   @(fluree/transact! conn {"@context" context
                                        "ledger"   ledger-id
                                        "delete"   {:id   :ex/dan
                                                    :ex/x "foo-3"
                                                    :ex/y "bar-3"}
                                        "insert"   {:id   :ex/dan
                                                    :ex/x "foo-cat"
                                                    :ex/y "bar-cat"}})
        ledger @(fluree/load conn ledger-id)]
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
             @(fluree/history ledger {:context context
                                      :history :ex/dan
                                      :t       {:from 1}}))))
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
             @(fluree/history ledger {:context context
                                      :history [:ex/dan]
                                      :t       {:from 1}}))))
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
             @(fluree/history ledger {:context context
                                      :history [:ex/dan :ex/x]
                                      :t       {:from 1}})))

      (is (= [{:f/t       1
               :f/assert  #{{:ex/x "foo-1" :id :ex/dog}
                            {:ex/x "foo-1" :id :ex/cat}
                            {:ex/x "foo-1" :id :ex/dan}}
               :f/retract #{}}
              {:f/t       2
               :f/assert  #{{:ex/x "foo-2" :id :ex/dan}}
               :f/retract #{{:ex/x "foo-1" :id :ex/dan}}}
              {:f/t       3
               :f/assert  #{{:ex/x "foo-3" :id :ex/dan}}
               :f/retract #{{:ex/x "foo-2" :id :ex/dan}}}
              {:f/t       4
               :f/assert  #{{:ex/x "foo-cat" :id :ex/cat}
                            {:ex/x "foo-dog" :id :ex/dog}}
               :f/retract #{{:ex/x "foo-1" :id :ex/dog}
                            {:ex/x "foo-1" :id :ex/cat}}}
              {:f/t       5
               :f/assert  #{{:ex/x "foo-cat" :id :ex/dan}}
               :f/retract #{{:ex/x "foo-3" :id :ex/dan}}}]
             (->> @(fluree/history ledger {:context context
                                           :history [nil :ex/x]
                                           :t       {:from 1}})
                  (mapv #(-> % (update :f/assert set) (update :f/retract set)))))))
    (testing "three-tuple flake history"
      (is (= [{:f/t 4 :f/assert [{:ex/x "foo-cat" :id :ex/cat}] :f/retract []}
              {:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:context context
                                      :history [nil :ex/x "foo-cat"]
                                      :t       {:from 1}})))
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract []}
              {:f/t       3
               :f/assert  []
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:context context
                                      :history [nil :ex/x "foo-2"]
                                      :t       {:from 1}})))
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:context context
                                      :history [:ex/dan :ex/x "foo-cat"]
                                      :t       {:from 1}}))))

    (testing "at-t"
      (let [expected [{:f/t       3
                       :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
                       :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]]
        (is (= expected
               @(fluree/history ledger {:context context
                                        :history [:ex/dan :ex/x]
                                        :t       {:from 3 :to 3}})))
        (is (= expected
               @(fluree/history ledger {:context context
                                        :history [:ex/dan :ex/x]
                                        :t       {:at 3}})))))
    (testing "from-t"
      (is (= [{:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:context context
                                      :history [:ex/dan :ex/x]
                                      :t       {:from 3}}))))
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
             @(fluree/history ledger {:context context
                                      :history [:ex/dan :ex/x]
                                      :t       {:to 3}}))))
    (testing "t-range"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       4
               :f/assert  [{:ex/x "foo-cat" :id :ex/cat} {:ex/x "foo-dog" :id :ex/dog}]
               :f/retract [{:ex/x "foo-1" :id :ex/cat} {:ex/x "foo-1" :id :ex/dog}]}]
             @(fluree/history ledger {:context context
                                      :history [nil :ex/x]
                                      :t       {:from 2 :to 4}}))))
    (testing "datetime-t"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:context context
                                      :history [nil :ex/x]
                                      :t       {:from ts2 :to ts3}}))
          "does not include t 1 4 or 5")
      (is (= [{:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:context context
                                      :history [:ex/dan :ex/x]
                                      :t       {:from (util/current-time-iso)}}))
          "timestamp translates to first t before ts")

      (is (= (str "There is no data as of " ts-primeval)
             (-> @(fluree/history ledger {:context context
                                          :history [:ex/dan :ex/x]
                                          :t       {:from ts-primeval}})
                 Throwable->map
                 :cause))))

    #_(testing "invalid query"
        (is (= "History query not properly formatted. Provided {:history []}"
               (-> @(fluree/history ledger {:history []})
                   Throwable->map
                   :cause))))

    (testing "small cache"
      (let [conn      (test-utils/create-conn)
            ledger-id "historycachetest"
            context   [test-utils/default-context {:ex "http://example.org/ns/"}]

            _db1   @(fluree/create-with-txn conn {"@context" context
                                                  "ledger"   ledger-id
                                                  "insert"   [{:id   :ex/dan
                                                               :ex/x "foo-1"
                                                               :ex/y "bar-1"}]})
            _db2   @(fluree/transact! conn {"@context" context
                                            "ledger"   ledger-id
                                            "delete"   {:id   :ex/dan
                                                        :ex/x "foo-1"
                                                        :ex/y "bar-1"}
                                            "insert"   {:id   :ex/dan
                                                        :ex/x "foo-2"
                                                        :ex/y "bar-2"}})
            ledger @(fluree/load conn ledger-id)]

        (testing "no t-range cache collision"
          (is (= [{:f/t       2
                   :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:context context
                                          :history [:ex/dan]
                                          :t       {:from 2}}))))))))

(deftest ^:integration ^:kaocha/pending commit-details-test
  (with-redefs [fluree.db.util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn      (test-utils/create-conn)
          ledger-id "committest"
          context   [test-utils/default-context {:ex "http://example.org/ns/"}]

          _db1   @(fluree/create-with-txn conn {"@context" context
                                                "ledger"   ledger-id
                                                "insert"   {:id   :ex/alice
                                                            :ex/x "foo-1"
                                                            :ex/y "bar-1"}})
          _db2   @(fluree/transact! conn {"@context" context
                                          "ledger"   ledger-id
                                          "insert"   {:id   :ex/alice
                                                      :ex/x "foo-2"
                                                      :ex/y "bar-2"}})
          _db3   @(fluree/transact! conn {"@context" context
                                          "ledger"   ledger-id
                                          "insert"   {:id   :ex/alice
                                                      :ex/x "foo-3"
                                                      :ex/y "bar-3"}})
          _db4   @(fluree/transact! conn {"@context" context
                                          "ledger"   ledger-id
                                          "insert"   {:id   :ex/cat
                                                      :ex/x "foo-cat"
                                                      :ex/y "bar-cat"}})
          _db5   @(fluree/transact! conn {"@context" context
                                          "ledger"   ledger-id
                                          "insert"   {:id   :ex/alice
                                                      :ex/x "foo-cat"
                                                      :ex/y "bar-cat"}}
                                    {:message "meow"})
          ledger @(fluree/load conn ledger-id)]

      (testing "at time t"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address test-utils/address?
                          :f/alias   "committest"
                          :f/branch  "main"
                          :f/previous
                          {:id test-utils/commit-id?}
                          :f/data    {:f/address test-utils/address?
                                      :f/assert  [{:ex/x "foo-1"
                                                   :ex/y "bar-1"
                                                   :id   :ex/alice}]
                                      :f/flakes  2
                                      :f/retract []
                                      :f/size    pos-int?
                                      :f/t       1
                                      :id        test-utils/db-id?}
                          :f/time    720000
                          :f/v       1
                          :id        test-utils/commit-id?}}]
             @(fluree/history ledger {:context        context
                                      :commit-details true
                                      :t              {:from 1 :to 1}})))
        (let [commit-5 {:f/commit {"https://www.w3.org/2018/credentials#issuer" {:id test-utils/did?}
                                   :f/address                                   test-utils/address?
                                   :f/alias                                     "committest"
                                   :f/branch                                    "main"
                                   :f/data                                      {:f/address test-utils/address?
                                                                                 :f/assert  [{:ex/x "foo-cat"
                                                                                              :ex/y "bar-cat"
                                                                                              :id   :ex/alice}]
                                                                                 :f/flakes  58
                                                                                 :f/retract [#_{:ex/x "foo-3"
                                                                                                :ex/y "bar-3"
                                                                                                :id   :ex/alice}]
                                                                                 :f/size    pos-int?
                                                                                 :f/t       5
                                                                                 :id        test-utils/db-id?}
                                   :f/message                                   "meow"
                                   :f/previous                                  {:id test-utils/commit-id?}
                                   :f/time                                      720000
                                   :f/v                                         1
                                   :id                                          test-utils/commit-id?}}
              commit-4 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id test-utils/did?}
                                   :f/address  test-utils/address?
                                   :f/alias    "committest"
                                   :f/branch   "main"
                                   :f/data     {:f/address test-utils/address?
                                                :f/assert  [{:ex/x "foo-cat"
                                                             :ex/y "bar-cat"
                                                             :id   :ex/cat}]
                                                :f/flakes  44
                                                :f/retract []
                                                :f/size    pos-int?
                                                :f/t       4
                                                :id        test-utils/db-id?}
                                   :f/previous {:id test-utils/commit-id?}
                                   :f/time     720000
                                   :f/v        1
                                   :id         test-utils/commit-id?}}]
          (is (pred-match?
               [commit-4 commit-5]
               @(fluree/history ledger {:context        context
                                        :commit-details true
                                        :t              {:from 4 :to 5}})))
          (is (pred-match?
               [commit-5]
               @(fluree/history ledger {:context        context
                                        :commit-details true
                                        :t              {:at :latest}})))))

      (testing "time range"
        (let [[c2 c3 c4 :as response] @(fluree/history
                                        ledger
                                        {:context        context
                                         :commit-details true
                                         :t              {:from 2 :to 4}})]
          (testing "all commits in time range are returned"
            (is (= 3 (count response)))
            (is (pred-match?
                 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id test-utils/did?}
                             :f/address  test-utils/address?
                             :f/alias    "committest"
                             :f/branch   "main"
                             :f/data     {:f/address test-utils/address?
                                          :f/assert  [{:ex/x "foo-cat"
                                                       :ex/y "bar-cat"
                                                       :id   :ex/cat}]
                                          :f/flakes  44
                                          :f/retract []
                                          :f/size    pos-int?
                                          :f/t       4
                                          :id        test-utils/db-id?}
                             :f/previous {:id test-utils/commit-id?}
                             :f/time     720000
                             :f/v        1
                             :id         test-utils/commit-id?}}
                 c4)))
          (is (pred-match?
               {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address  test-utils/address?
                           :f/alias    "committest"
                           :f/branch   "main"
                           :f/data     {:f/address test-utils/address?
                                        :f/assert  [{:ex/x "foo-3"
                                                     :ex/y "bar-3"
                                                     :id   :ex/alice}]
                                        :f/flakes  30
                                        :f/retract [{:ex/x "foo-2"
                                                     :ex/y "bar-2"
                                                     :id   :ex/alice}]
                                        :f/size    pos-int?
                                        :f/t       3
                                        :id        test-utils/db-id?}
                           :f/previous {:id test-utils/commit-id?}
                           :f/time     720000
                           :f/v        1
                           :id         test-utils/commit-id?}}
               c3))
          (is (pred-match?
               {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address  test-utils/address?
                           :f/alias    "committest"
                           :f/branch   "main"
                           :f/data     {:f/address test-utils/address?
                                        :f/assert  [{:ex/x "foo-2"
                                                     :ex/y "bar-2"
                                                     :id   :ex/alice}]
                                        :f/flakes  16
                                        :f/retract [{:ex/x "foo-1"
                                                     :ex/y "bar-1"
                                                     :id   :ex/alice}]
                                        :f/size    pos-int?
                                        :f/t       2
                                        :id        test-utils/db-id?}
                           :f/previous {:id test-utils/commit-id?}
                           :f/time     720000
                           :f/v        1
                           :id         test-utils/commit-id?}}
               c2))))

      (testing "time range from"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address  test-utils/address?
                          :f/alias    "committest"
                          :f/branch   "main"
                          :f/data     {:f/address  test-utils/address?
                                       :f/assert   [{:ex/x "foo-cat"
                                                     :ex/y "bar-cat"
                                                     :id   :ex/cat}]
                                       :f/flakes   44
                                       :f/previous {:id test-utils/db-id?}
                                       :f/retract  []
                                       :f/size     pos-int?
                                       :f/t        4
                                       :id         test-utils/db-id?}
                          :f/previous {:id test-utils/commit-id?}
                          :f/time     720000
                          :f/v        1
                          :id         test-utils/commit-id?}}
              {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address  test-utils/address?
                          :f/alias    "committest"
                          :f/branch   "main"
                          :f/data     {:f/address  test-utils/address?
                                       :f/assert   [{:ex/x "foo-cat"
                                                     :ex/y "bar-cat"
                                                     :id   :ex/alice}]
                                       :f/flakes   58
                                       :f/previous {:id test-utils/db-id?}
                                       :f/retract  [{:ex/x "foo-3"
                                                     :ex/y "bar-3"
                                                     :id   :ex/alice}]
                                       :f/size     pos-int?
                                       :f/t        5
                                       :id         test-utils/db-id?}
                          :f/message  "meow"
                          :f/previous {:id test-utils/commit-id?}
                          :f/time     720000
                          :f/v        1
                          :id         test-utils/commit-id?}}]
             @(fluree/history ledger {:context        context
                                      :commit-details true
                                      :t              {:from 4}}))))

      (testing "time range to"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address  test-utils/address?
                          :f/alias    "committest"
                          :f/branch   "main"
                          :f/previous {:id test-utils/commit-id?}
                          :f/data     {:f/address test-utils/address?
                                       :f/assert  [{:ex/x "foo-1"
                                                    :ex/y "bar-1"
                                                    :id   :ex/alice}]
                                       :f/flakes  2
                                       :f/retract []
                                       :f/size    pos-int?
                                       :f/t       1
                                       :id        test-utils/db-id?}
                          :f/time     720000
                          :f/v        1
                          :id         test-utils/commit-id?}}]
             @(fluree/history ledger {:context        context
                                      :commit-details true
                                      :t              {:to 1}}))))

      (testing "history commit details"
        (is (pred-match?
             [#:f{:assert  [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    "committest"
                            :f/branch   "main"
                            :f/data     {:f/address test-utils/address?
                                         :f/assert  [{:ex/x "foo-3"
                                                      :ex/y "bar-3"
                                                      :id   :ex/alice}]
                                         :f/flakes  30
                                         :f/retract [{:ex/x "foo-2"
                                                      :ex/y "bar-2"
                                                      :id   :ex/alice}]
                                         :f/size    pos-int?
                                         :f/t       3
                                         :id        test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    "committest"
                            :f/branch   "main"
                            :f/data     {:f/address test-utils/address?
                                         :f/assert  [{:ex/x "foo-cat"
                                                      :ex/y "bar-cat"
                                                      :id   :ex/alice}]
                                         :f/flakes  58
                                         :f/retract [{:ex/x "foo-3"
                                                      :ex/y "bar-3"
                                                      :id   :ex/alice}]
                                         :f/size    pos-int?
                                         :f/t       5
                                         :id        test-utils/db-id?}
                            :f/message  "meow"
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history ledger {:context        context
                                      :history        :ex/alice
                                      :commit-details true
                                      :t              {:from 3}}))))

      (testing "multiple history results"
        (let [history-with-commits @(fluree/history ledger {:context        context
                                                            :history        :ex/alice
                                                            :commit-details true
                                                            :t              {:from 1 :to 5}})]
          (testing "all `t`s with changes to subject are returned"
            (is (= [1 2 3 5]
                   (mapv :f/t history-with-commits))))
          (testing "all expected commits are present and associated with the correct results"
            (is (= [[1 1] [2 2] [3 3] [5 5]]
                   (map (fn [history-map]
                          (let [commit-t (get-in history-map [:f/commit :f/data :f/t])]
                            (vector (:f/t history-map) commit-t)))
                        history-with-commits)))))))))

(deftest ^:kaocha/pending loaded-mem-ledger-history-test
  (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded memory ledger"
      (let [ledger-name   "loaded-history-mem"
            conn          @(fluree/connect-memory)
            context       [test-utils/default-context {:ex "http://example.org/ns/"}]
            _             @(fluree/create-with-txn conn {"@context" context
                                                         "ledger"   ledger-name
                                                         "insert"   {:id   :ex/alice
                                                                     :ex/x "foo-1"
                                                                     :ex/y "bar-1"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "delete"   {:id   :ex/alice
                                                               :ex/x "foo-1"
                                                               :ex/y "bar-1"}
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-2"
                                                               :ex/y "bar-2"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "delete"   {:id   :ex/alice
                                                               :ex/x "foo-2"
                                                               :ex/y "bar-2"}
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-3"
                                                               :ex/y "bar-3"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "insert"   {:id   :ex/cat
                                                               :ex/x "foo-cat"
                                                               :ex/y "bar-cat"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "delete"   {:id   :ex/alice
                                                               :ex/x "foo-3"
                                                               :ex/y "bar-3"}
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-cat"
                                                               :ex/y "bar-cat"}}
                                             {:message "meow"})
            loaded-ledger (test-utils/retry-load conn ledger-name 100)]

        (is (pred-match?
             [#:f{:assert  [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :commit  {:f/address  test-utils/address?
                            :f/alias    ledger-name
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/flakes   34
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        3
                                         :id         test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {:f/address  test-utils/address?
                            :f/alias    ledger-name
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-cat"
                                                       :ex/y "bar-cat"
                                                       :id   :ex/alice}]
                                         :f/flakes   64
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        5
                                         :id         test-utils/db-id?}
                            :f/message  "meow"
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history loaded-ledger {:context        context
                                             :history        :ex/alice
                                             :commit-details true
                                             :t              {:from 3}})))))

    (testing "history commit details on a loaded memory ledger w/ issuer"
      (let [ledger-name "loaded-history-mem-issuer"
            conn        @(fluree/connect-memory {:defaults {:did (did/private->did-map test-utils/default-private-key)}})
            context     [test-utils/default-context {:ex "http://example.org/ns/"}]

            _             @(fluree/create-with-txn conn {"@context" context
                                                         "ledger"   ledger-name
                                                         "insert"   {:id   :ex/alice
                                                                     :ex/x "foo-1"
                                                                     :ex/y "bar-1"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-2"
                                                               :ex/y "bar-2"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-3"
                                                               :ex/y "bar-3"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "insert"   {:id   :ex/cat
                                                               :ex/x "foo-cat"
                                                               :ex/y "bar-cat"}})
            _             @(fluree/transact! conn {"@context" context
                                                   "ledger"   ledger-name
                                                   "insert"   {:id   :ex/alice
                                                               :ex/x "foo-cat"
                                                               :ex/y "bar-cat"}}
                                             {:message "meow"})
            loaded-ledger (test-utils/retry-load conn ledger-name 100)]
        (is (pred-match?
             [#:f{:assert  [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    ledger-name
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/flakes   32
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        3
                                         :id         test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    ledger-name
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-cat"
                                                       :ex/y "bar-cat"
                                                       :id   :ex/alice}]
                                         :f/flakes   62
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        5
                                         :id         test-utils/db-id?}
                            :f/message  "meow"
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/v        1
                            :id         test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history loaded-ledger {:context        context
                                             :history        :ex/alice
                                             :commit-details true
                                             :t              {:from 3}})))))))

(deftest loaded-file-ledger-history-test
  (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded file ledger"
      (with-tmp-dir storage-path
        (let [ledger-name "loaded-history-file"
              conn        @(fluree/connect-file {:storage-path storage-path
                                                 :defaults     {:identity (did/private->did-map
                                                                           test-utils/default-private-key)}})
              context     [test-utils/default-context {:ex   "http://example.org/ns/"
                                                       :cred "https://www.w3.org/2018/credentials#"}]

              _a            @(fluree/create-with-txn conn {"@context" context
                                                           "ledger"   ledger-name
                                                           "insert"   {:id   :ex/alice
                                                                       :ex/x "foo-1"
                                                                       :ex/y "bar-1"}})
              _b            @(fluree/transact! conn {"@context" context
                                                     "ledger"   ledger-name
                                                     "delete"   {:id   :ex/alice
                                                                 :ex/x "foo-1"
                                                                 :ex/y "bar-1"}
                                                     "insert"   {:id   :ex/alice
                                                                 :ex/x "foo-2"
                                                                 :ex/y "bar-2"}})
              _c            @(fluree/transact! conn {"@context" context
                                                     "ledger"   ledger-name
                                                     "delete"   {:id   :ex/alice
                                                                 :ex/x "foo-2"
                                                                 :ex/y "bar-2"}
                                                     "insert"   {:id   :ex/alice
                                                                 :ex/x "foo-3"
                                                                 :ex/y "bar-3"}})
              _d            @(fluree/transact! conn {"@context" context
                                                     "ledger"   ledger-name
                                                     "insert"   {:id   :ex/cat
                                                                 :ex/x "foo-cat"
                                                                 :ex/y "bar-cat"}})
              _e            @(fluree/transact! conn {"@context" context
                                                     "ledger"   ledger-name
                                                     "delete"   {:id   :ex/alice
                                                                 :ex/x "foo-3"
                                                                 :ex/y "bar-3"}
                                                     "insert"   {:id   :ex/alice
                                                                 :ex/x "foo-cat"
                                                                 :ex/y "bar-cat"}}
                                               {:message "meow"})
              loaded-ledger (test-utils/retry-load conn ledger-name 100)]
          (is (pred-match?
               [#:f{:assert  [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :commit  {:cred/issuer {:id test-utils/did?}
                              :f/address   test-utils/address?
                              :f/alias     ledger-name
                              :f/branch    "main"
                              :f/data      {:f/address test-utils/address?
                                            :f/assert  [{:ex/x "foo-3"
                                                         :ex/y "bar-3"
                                                         :id   :ex/alice}]
                                            :f/flakes  34
                                            :f/retract [{:ex/x "foo-2"
                                                         :ex/y "bar-2"
                                                         :id   :ex/alice}]
                                            :f/size    pos-int?
                                            :f/t       3
                                            :id        test-utils/db-id?}
                              :f/previous  {:id test-utils/commit-id?}
                              :f/time      720000
                              :f/v         1
                              :id          test-utils/commit-id?}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {:cred/issuer {:id test-utils/did?}
                              :f/address   test-utils/address?
                              :f/alias     ledger-name
                              :f/branch    "main"
                              :f/data      {:f/address test-utils/address?
                                            :f/assert  [{:ex/x "foo-cat"
                                                         :ex/y "bar-cat"
                                                         :id   :ex/alice}]
                                            :f/flakes  64
                                            :f/retract [{:ex/x "foo-3"
                                                         :ex/y "bar-3"
                                                         :id   :ex/alice}]
                                            :f/size    pos-int?
                                            :f/t       5
                                            :id        test-utils/db-id?}
                              :f/message   "meow"
                              :f/previous  {:id test-utils/commit-id?}
                              :f/time      720000
                              :f/v         1
                              :id          test-utils/commit-id?}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history loaded-ledger {:context        context
                                               :history        :ex/alice
                                               :commit-details true
                                               :t              {:from 3}}))))))))

(deftest ^:integration author-and-txn-id
  (with-redefs [fluree.db.util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn         @(fluree/connect-memory)
          ledger-name  "authortest"
          ledger       @(fluree/create conn ledger-name)
          context      [test-utils/default-str-context {"ex" "http://example.org/ns/"
                                                        "f"  "https://ns.flur.ee/ledger#"}]
          root-privkey "89e0ab9ac36fb82b172890c89e9e231224264c7c757d58cfd8fcd6f3d4442199"
          root-did     (:id (did/private->did-map root-privkey))

          db0  (fluree/db ledger)
          db1  @(fluree/stage db0 {"@context" context
                                   "insert"   [{"@id"         "ex:betty"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Betty"
                                                "schema:age"  55}
                                               {"@id"         "ex:freddy"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Freddy"
                                                "schema:age"  1002}
                                               {"@id"         "ex:letty"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Leticia"
                                                "schema:age"  38}
                                               {"@id"           root-did
                                                "f:policyClass" [{"@id" "ex:RootPolicy"}]}]})
          _db2 (->> @(fluree/stage db1 {"@context" context
                                        "insert"   [{"@id"      "ex:defaultAllowViewModify"
                                                     "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                                     "f:action" [{"@id" "f:view"}, {"@id" "f:modify"}]
                                                     "f:query"  {"@type"  "@json"
                                                                 "@value" {}}}]})
                    (fluree/commit! ledger)
                    (deref))

          _db3 @(fluree/credential-transact! conn (crypto/create-jws
                                                   (json/stringify {"@context" context
                                                                    "ledger"   ledger-name
                                                                    "insert"   {"ex:foo" 3}})
                                                   root-privkey))

          _db4 @(fluree/credential-transact! conn (crypto/create-jws
                                                   (json/stringify {"@context" context
                                                                    "ledger"   ledger-name
                                                                    "insert"   {"ex:foo" 5}})
                                                   root-privkey))]
      (is (= [{"f:data" {"f:t" 1}}
              {"f:author" "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
               "f:data"   {"f:t" 2},
               "f:txn"    "fluree:memory://byfkd5sj5lwq3aaxgbqkwoteakwjqqjrrvsrbhl7eirp3aizykj3"}
              {"f:author" "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
               "f:data"   {"f:t" 3},
               "f:txn"    "fluree:memory://bsb5zixq25bktdvwtzbquwgvjii6cv4mi7mu3zbpu562oa77y5nq"}]
             (->> @(fluree/history ledger {:context        context
                                           :commit-details true
                                           :t              {:from 1 :to :latest}})
                  (mapv (fn [c]
                          (-> (get c "f:commit")
                              (update "f:data" select-keys ["f:t"])
                              (select-keys ["f:author" "f:txn" "f:data"]))))))))))

(deftest ^:integration ^:kaocha/pending include-api
  (with-redefs [fluree.db.util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn         @(fluree/connect-memory)
          ledger-name  "authortest"
          ledger       @(fluree/create conn ledger-name)
          context      [test-utils/default-str-context {"ex" "http://example.org/ns/"
                                                        "f"  "https://ns.flur.ee/ledger#"}]
          root-privkey "89e0ab9ac36fb82b172890c89e9e231224264c7c757d58cfd8fcd6f3d4442199"
          root-did     (:id (did/private->did-map root-privkey))

          db0  (fluree/db ledger)
          db1  @(fluree/stage db0 {"@context" context
                                   "insert"   [{"@id"         "ex:betty"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Betty"
                                                "schema:age"  55}
                                               {"@id"         "ex:freddy"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Freddy"
                                                "schema:age"  1002}
                                               {"@id"         "ex:letty"
                                                "@type"       "ex:Yeti"
                                                "schema:name" "Leticia"
                                                "schema:age"  38}
                                               {"@id"           root-did
                                                "f:policyClass" [{"@id" "ex:RootPolicy"}]}]})
          _db2 (->> @(fluree/stage db1 {"@context" context
                                        "insert"   [{"@id"      "ex:defaultAllowViewModify"
                                                     "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                                     "f:action" [{"@id" "f:view"}, {"@id" "f:modify"}]
                                                     "f:query"  {"@type"  "@json"
                                                                 "@value" {}}}]})
                    (fluree/commit! ledger)
                    (deref))

          jws1 (crypto/create-jws
                (json/stringify {"@context" context
                                 "ledger"   ledger-name
                                 "insert"   {"ex:foo" 3}})
                root-privkey)
          _db3 @(fluree/credential-transact! conn jws1)

          jws2 (crypto/create-jws
                (json/stringify {"@context" context
                                 "ledger"   ledger-name
                                 "insert"   {"ex:foo" 5}})
                root-privkey)
          _db4 @(fluree/credential-transact! conn jws2)]

      (testing ":txn returns the raw transaction"
        (is (= [{"f:txn" nil}
                {"f:txn" jws1}
                {"f:txn" jws2}]
               @(fluree/history ledger {:context context
                                        :txn     true
                                        :t       {:from 1 :to :latest}}))))

      (testing ":commit returns just the commit wrapper"
        (is (pred-match?
             [{"f:commit"
               {"f:alias"    "authortest",
                "f:time"     720000,
                "f:previous" {"id" test-utils/commit-id?},
                "id"         test-utils/commit-id?
                "f:v"        1,
                "f:branch"   "main",
                "f:address"  test-utils/address?
                "f:data"     {"f:address"  test-utils/address?
                              "f:flakes"   15,
                              "f:previous" {"id" test-utils/db-id?},
                              "f:size"     pos-int?,
                              "f:t"        1,
                              "id"         test-utils/db-id?}}}
              {"f:commit"
               {"f:alias"    "authortest",
                "f:author"   "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
                "f:time"     720000,
                "f:txn"      test-utils/address?
                "f:previous" {"id" test-utils/commit-id?}
                "id"         test-utils/commit-id?
                "f:v"        1,
                "f:branch"   "main",
                "f:address"  test-utils/address?
                "f:data"     {"f:address"  test-utils/address?
                              "f:flakes"   28,
                              "f:previous" {"id" test-utils/db-id?},
                              "f:size"     pos-int?
                              "f:t"        2,
                              "id"         test-utils/db-id?}}}
              {"f:commit"
               {"f:alias"    "authortest",
                "f:author"   "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
                "f:time"     720000,
                "f:txn"      test-utils/address?
                "f:previous" {"id" test-utils/commit-id?},
                "id"         test-utils/commit-id?
                "f:v"        1,
                "f:branch"   "main",
                "f:address"  test-utils/address?
                "f:data"     {"f:address"  test-utils/address?
                              "f:flakes"   43,
                              "f:previous" {"id" test-utils/db-id?},
                              "f:size"     pos-int?,
                              "f:t"        3,
                              "id"         test-utils/db-id?}}}]
             @(fluree/history ledger {:context context
                                      :commit  true
                                      :t       {:from 1 :to :latest}}))))

      (testing ":data returns just the asserts and retracts"
        (is (pred-match? [{"f:data" {"f:t"       1
                                     "f:assert"  [{"id"            "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb"
                                                   "f:policyClass" {"id" "ex:RootPolicy"}}
                                                  {"type"        "ex:Yeti",
                                                   "schema:age"  55,
                                                   "schema:name" "Betty",
                                                   "id"          "ex:betty"}
                                                  {"id"       "ex:defaultAllowViewModify"
                                                   "type"     ["f:AccessPolicy" "ex:RootPolicy"],
                                                   "f:action" [{"id" "f:modify"} {"id" "f:view"}],
                                                   "f:query"  {}}
                                                  {"id"          "ex:freddy"
                                                   "type"        "ex:Yeti",
                                                   "schema:age"  1002,
                                                   "schema:name" "Freddy"}
                                                  {"id"          "ex:letty"
                                                   "type"        "ex:Yeti",
                                                   "schema:age"  38,
                                                   "schema:name" "Leticia"}]
                                     "f:retract" []}}
                          {"f:data" {"f:t"       2
                                     "f:assert"  [{"ex:foo" 3, "id" test-utils/blank-node-id?}],
                                     "f:retract" []}}
                          {"f:data" {"f:t"       3
                                     "f:assert"  [{"ex:foo" 5, "id" test-utils/blank-node-id?}],
                                     "f:retract" []}}]
                         @(fluree/history ledger {:context context
                                                  :data    true
                                                  :t       {:from 1 :to :latest}}))))

      (testing ":commit :data :and txn can be composed together"
        (is (pred-match?
             [{"f:txn"    nil
               "f:commit" {"f:alias"    "authortest",
                           "f:time"     720000,
                           "f:previous" {"id" test-utils/commit-id?},
                           "id"         test-utils/commit-id?
                           "f:v"        1,
                           "f:branch"   "main",
                           "f:address"  test-utils/address?
                           "f:data"
                           {"f:address"  test-utils/address?
                            "f:flakes"   15,
                            "f:previous" {"id" test-utils/db-id?},
                            "f:size"     pos-int?
                            "f:t"        1,
                            "id"         test-utils/db-id?}},
               "f:data"   {"f:t"       1,
                           "f:assert"
                           [{"id"            "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb"
                             "f:policyClass" {"id" "ex:RootPolicy"}}
                            {"type"        "ex:Yeti",
                             "schema:age"  55,
                             "schema:name" "Betty",
                             "id"          "ex:betty"}
                            {"id"       "ex:defaultAllowViewModify"
                             "type"     ["f:AccessPolicy" "ex:RootPolicy"],
                             "f:action" [{"id" "f:modify"} {"id" "f:view"}],
                             "f:query"  {}}
                            {"type"        "ex:Yeti",
                             "schema:age"  1002,
                             "schema:name" "Freddy",
                             "id"          "ex:freddy"}
                            {"type"        "ex:Yeti",
                             "schema:age"  38,
                             "schema:name" "Leticia",
                             "id"          "ex:letty"}]
                           "f:retract" []}}
              {"f:txn"    jws1
               "f:commit" {"f:alias"    "authortest",
                           "f:author"   "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
                           "f:time"     720000,
                           "f:txn"      test-utils/address?
                           "f:previous" {"id" test-utils/commit-id?},
                           "id"         test-utils/commit-id?
                           "f:v"        1,
                           "f:branch"   "main",
                           "f:address"  test-utils/address?
                           "f:data"
                           {"f:address"  test-utils/address?
                            "f:flakes"   28,
                            "f:previous" {"id" test-utils/db-id?},
                            "f:size"     pos-int?
                            "f:t"        2,
                            "id"         test-utils/db-id?}},
               "f:data"   {"f:t"       2
                           "f:assert"  [{"ex:foo" 3, "id" test-utils/blank-node-id?}],
                           "f:retract" []}}
              {"f:txn"    jws2
               "f:commit" {"f:alias"    "authortest",
                           "f:author"   "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
                           "f:time"     720000,
                           "f:txn"      test-utils/address?
                           "f:previous" {"id" test-utils/commit-id?},
                           "id"         test-utils/commit-id?
                           "f:v"        1,
                           "f:branch"   "main",
                           "f:address"  test-utils/address?
                           "f:data"
                           {"f:address"  test-utils/address?
                            "f:flakes"   43,
                            "f:previous" {"id" test-utils/db-id?},
                            "f:size"     pos-int?
                            "f:t"        3,
                            "id"         test-utils/db-id?}},
               "f:data"   {"f:t"       3
                           "f:assert"  [{"ex:foo" 5, "id" test-utils/blank-node-id?}],
                           "f:retract" []}}]
             @(fluree/history ledger {:context context
                                      :txn     true
                                      :data    true
                                      :commit  true
                                      :t       {:from 1 :to :latest}}))))

      (testing ":commit :data :and txn can be composed together with history"
        (is (pred-match?
             [{"f:t"       1,
               "f:assert"  [{"type"        "ex:Yeti",
                             "schema:age"  1002,
                             "schema:name" "Freddy",
                             "id"          "ex:freddy"}],
               "f:retract" [],
               "f:txn"     nil,
               "f:commit"  {"f:alias"    "authortest",
                            "f:time"     720000,
                            "f:previous" {"id" test-utils/commit-id?},
                            "id"         test-utils/commit-id?
                            "f:v"        1,
                            "f:branch"   "main",
                            "f:address"  test-utils/address?
                            "f:data"
                            {"f:address"  test-utils/address?
                             "f:flakes"   15,
                             "f:previous" {"id" test-utils/db-id?},
                             "f:size"     pos-int?,
                             "f:t"        1,
                             "id"         test-utils/db-id?}},
               "f:data"    {"f:t"       1,
                            "f:assert"
                            [{"id"            "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb"
                              "f:policyClass" {"id" "ex:RootPolicy"}}
                             {"type"        "ex:Yeti",
                              "schema:age"  55,
                              "schema:name" "Betty",
                              "id"          "ex:betty"}
                             {"id"       "ex:defaultAllowViewModify"
                              "type"     ["f:AccessPolicy" "ex:RootPolicy"],
                              "f:action" [{"id" "f:modify"} {"id" "f:view"}],
                              "f:query"  {}}
                             {"type"        "ex:Yeti",
                              "schema:age"  1002,
                              "schema:name" "Freddy",
                              "id"          "ex:freddy"}
                             {"type"        "ex:Yeti",
                              "schema:age"  38,
                              "schema:name" "Leticia",
                              "id"          "ex:letty"}],
                            "f:retract" []}}]
             @(fluree/history ledger {:context context
                                      :history "ex:freddy"
                                      :txn     true
                                      :data    true
                                      :commit  true
                                      :t       {:from 1 :to :latest}}))))

      (testing ":commit :data :and txn can be composed together with commit-details"
        (is (pred-match?
             [{"f:t"       1,
               "f:assert"  [{"type"        "ex:Yeti",
                             "schema:age"  1002,
                             "schema:name" "Freddy",
                             "id"          "ex:freddy"}],
               "f:retract" [],
               "f:txn"     nil,
               "f:commit"  {"f:alias"    "authortest",
                            "f:time"     720000,
                            "f:previous" {"id" test-utils/commit-id?},
                            "id"         test-utils/commit-id?
                            "f:v"        1,
                            "f:branch"   "main",
                            "f:address"  test-utils/address?
                            "f:data"
                            {"f:address"  test-utils/address?
                             "f:flakes"   15,
                             "f:previous" {"id" test-utils/db-id?},
                             "f:size"     pos-int?
                             "f:t"        1,
                             "id"         test-utils/db-id?}}
               "f:data"    {"f:t"       1,
                            "f:assert"
                            [{"id"            "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb"
                              "f:policyClass" {"id" "ex:RootPolicy"}}
                             {"type"        "ex:Yeti",
                              "schema:age"  55,
                              "schema:name" "Betty",
                              "id"          "ex:betty"}
                             {"id"       "ex:defaultAllowViewModify"
                              "type"     ["f:AccessPolicy" "ex:RootPolicy"],
                              "f:action" [{"id" "f:modify"} {"id" "f:view"}],
                              "f:query"  {}}
                             {"type"        "ex:Yeti",
                              "schema:age"  1002,
                              "schema:name" "Freddy",
                              "id"          "ex:freddy"}
                             {"type"        "ex:Yeti",
                              "schema:age"  38,
                              "schema:name" "Leticia",
                              "id"          "ex:letty"}
                             {"f:action"     [{"id" "f:modify"} {"id" "f:view"}],
                              "f:targetRole" {"id" "ex:rootRole"},
                              "id"           "ex:rootAccessAllow"}
                             {"type"         "f:AccessPolicy",
                              "f:allow"      {"id" "ex:rootAccessAllow"},
                              "f:targetNode" {"id" "f:allNodes"},
                              "id"           "ex:rootPolicy"}],
                            "f:retract" []}}]
             @(fluree/history ledger {:context context
                                      :history "ex:freddy"
                                      :txn     true
                                      :data    true
                                      :commit  true
                                      :t       {:from 1 :to :latest}})))))))

(deftest ^:integration txn-annotation
  (let [conn        @(fluree/connect-memory)
        ledger-name "annotationtest"
        ledger      @(fluree/create conn ledger-name)
        context     [test-utils/default-str-context {"ex" "http://example.org/ns/"}]

        db0 (fluree/db ledger)]
    (testing "valid annotations"
      (with-redefs [fluree.db.util/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
        (let [db1 (->> @(fluree/stage db0 {"@context" context
                                           "insert"   [{"@id"         "ex:betty"
                                                        "@type"       "ex:Yeti"
                                                        "schema:name" "Betty"
                                                        "schema:age"  55}]})
                       (fluree/commit! ledger)
                       (deref))

              db2 (->> @(fluree/stage db1 {"@context" context
                                           "insert"   [{"@id"         "ex:freddy"
                                                        "@type"       "ex:Yeti"
                                                        "schema:name" "Freddy"
                                                        "schema:age"  1002}]}
                                      {:annotation {"ex:originator" "opts" "ex:data" "ok"}})
                       (fluree/commit! ledger)
                       (deref))

              _db3 (->> @(fluree/stage db2 {"@context" context
                                            "insert"   [{"@id"         "ex:letty"
                                                         "@type"       "ex:Yeti"
                                                         "schema:name" "Leticia"
                                                         "schema:age"  38}]
                                            "opts"     {"annotation" {"ex:originator" "txn" "ex:data" "ok"}}})
                        (fluree/commit! ledger)
                        (deref))]
          (testing "annotations in commit-details"
            (is (pred-match? [{}
                              {"f:annotation" {"id" test-utils/blank-node-id? "ex:data" "ok" "ex:originator" "opts"}}
                              {"f:annotation" {"id" test-utils/blank-node-id? "ex:data" "ok" "ex:originator" "txn"}}]
                             (->> @(fluree/history ledger {:context        context
                                                           :commit-details true
                                                           :t              {:from 1 :to :latest}})
                                  (mapv (fn [c] (-> c (get "f:commit") (select-keys ["f:txn" "f:annotation"]))))))))))

      (testing "invalid annotations"
        (testing "only single annotation subject permitted"
          (let [invalid2 @(fluree/stage db0 {"@context" context
                                             "insert"   [{"@id"         "ex:betty"
                                                          "@type"       "ex:Yeti"
                                                          "schema:name" "Betty"
                                                          "schema:age"  55}]}
                                        {:annotation [{"ex:originator" "opts" "ex:multiple" true}
                                                      {"ex:originator" "opts" "ex:invalid" true}]})]
            (is (= "Commit annotation must only have a single subject." (ex-message invalid2)))))

        (testing "cannot specify id"
          (let [invalid3 @(fluree/stage db0 {"@context" context
                                             "insert"   [{"@id"         "ex:betty"
                                                          "@type"       "ex:Yeti"
                                                          "schema:name" "Betty"
                                                          "schema:age"  55}]}
                                        {:annotation [{"ex:originator" "opts" "@id" "invalid:subj"}]})]
            (is (= "Commit annotation cannot specify a subject identifier." (ex-message invalid3)))))

        (testing "annotation has no references"
          (let [invalid4 @(fluree/stage db0 {"@context" context
                                             "insert"   [{"@id"         "ex:betty"
                                                          "@type"       "ex:Yeti"
                                                          "schema:name" "Betty"
                                                          "schema:age"  55}]}
                                        {:annotation [{"ex:originator" "opts" "ex:friend" {"@id" "ex:betty"}}]})]
            (is (= "Commit annotation cannot reference other subjects." (ex-message invalid4))
                "using id-map"))
          (let [invalid4 @(fluree/stage db0 {"@context" context
                                             "insert"   [{"@id"         "ex:betty"
                                                          "@type"       "ex:Yeti"
                                                          "schema:name" "Betty"
                                                          "schema:age"  55}]}
                                        {:annotation [{"ex:originator" "opts" "ex:friend"
                                                       {"@type" "id" "@value" "ex:betty"}}]})]
            (is (= "Commit annotation cannot reference other subjects." (ex-message invalid4))
                "using value-map with type id"))
          (let [invalid1 @(fluree/stage db0 {"@context" context
                                             "insert"   [{"@id"         "ex:betty"
                                                          "@type"       "ex:Yeti"
                                                          "schema:name" "Betty"
                                                          "schema:age"  55}]}
                                        {:annotation {"ex:originator" "opts" "ex:nested" {"valid" false}}})]
            (is (= "Commit annotation cannot reference other subjects." (ex-message invalid1))
                "using implicit blank node identifier")))))))
