(ns fluree.db.query.history-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.crypto :as crypto]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration history-query-test
  (let [ts-primeval (util/current-time-iso)

        conn    (test-utils/create-conn)
        ledger  @(fluree/create conn "historytest")
        context [test-utils/default-context {:ex "http://example.org/ns/"}]

        db1 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                          "insert"   [{:id   :ex/dan
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}
                                                      {:id   :ex/cat
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}
                                                      {:id   :ex/dog
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}]})
        db2 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                          "delete"   {:id   :ex/dan
                                                      :ex/x "foo-1"
                                                      :ex/y "bar-1"}
                                          "insert"   {:id   :ex/dan
                                                      :ex/x "foo-2"
                                                      :ex/y "bar-2"}})
        ts2 (-> db2 :commit :time)
        db3 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                          "delete"   {:id   :ex/dan
                                                      :ex/x "foo-2"
                                                      :ex/y "bar-2"}
                                          "insert"   {:id   :ex/dan
                                                      :ex/x "foo-3"
                                                      :ex/y "bar-3"}})

        ts3 (-> db3 :commit :time)
        db4 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
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
        db5 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                          "delete"   {:id   :ex/dan
                                                      :ex/x "foo-3"
                                                      :ex/y "bar-3"}
                                          "insert"   {:id   :ex/dan
                                                      :ex/x "foo-cat"
                                                      :ex/y "bar-cat"}})]
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
      (let [conn    (test-utils/create-conn)
            ledger  @(fluree/create conn "historycachetest")
            context [test-utils/default-context {:ex "http://example.org/ns/"}]

            db1 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                              "insert"   [{:id   :ex/dan
                                                           :ex/x "foo-1"
                                                           :ex/y "bar-1"}]})
            db2 @(test-utils/transact ledger {"@context" ["https://ns.flur.ee" context]
                                              "delete"   {:id   :ex/dan
                                                          :ex/x "foo-1"
                                                          :ex/y "bar-1"}
                                              "insert"   {:id   :ex/dan
                                                          :ex/x "foo-2"
                                                          :ex/y "bar-2"}})]
        (testing "no t-range cache collision"
          (is (= [{:f/t       2
                   :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:context context
                                          :history [:ex/dan]
                                          :t       {:from 2}}))))))))

(deftest ^:integration commit-details-test
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "committest")
          context ["https://ns.flur.ee" test-utils/default-context {:ex "http://example.org/ns/"}]

          db1 @(test-utils/transact ledger {"@context" context
                                            "insert"   {:id   :ex/alice
                                                        :ex/x "foo-1"
                                                        :ex/y "bar-1"}})
          db2 @(test-utils/transact ledger {"@context" context
                                            "insert"   {:id   :ex/alice
                                                        :ex/x "foo-2"
                                                        :ex/y "bar-2"}})
          db3 @(test-utils/transact ledger {"@context" context
                                            "insert"   {:id   :ex/alice
                                                        :ex/x "foo-3"
                                                        :ex/y "bar-3"}})
          db4 @(test-utils/transact ledger {"@context" context
                                            "insert"   {:id   :ex/cat
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"}})
          db5 @(test-utils/transact ledger {"@context" context
                                            "insert"   {:id   :ex/alice
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"}}
                                    {:message "meow"})]
      (testing "at time t"
        (is (pred-match?
              [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address test-utils/address?
                           :f/alias   "committest"
                           :f/author  ""
                           :f/branch  "main"
                           :f/previous
                           {:id test-utils/commit-id?}
                           :f/data    {:f/address  test-utils/address?
                                       :f/assert   [{:ex/x "foo-1"
                                                     :ex/y "bar-1"
                                                     :id   :ex/alice}]
                                       :f/flakes   2
                                       :f/retract  []
                                       :f/size     pos-int?
                                       :f/t        1
                                       :f/previous {:id test-utils/db-id?}
                                       :id         test-utils/db-id?}
                           :f/time    720000
                           :f/txn     string?
                           :f/v       0
                           :id        test-utils/commit-id?}}]
              @(fluree/history ledger {:context        context
                                       :commit-details true
                                       :t              {:from 1 :to 1}})))
        (let [commit-5 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id test-utils/did?}
                                   :f/address  test-utils/address?
                                   :f/alias    "committest"
                                   :f/author   ""
                                   :f/branch   "main"
                                   :f/data     {:f/address  test-utils/address?
                                                :f/assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                :f/flakes   70
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
                                   :f/txn      string?
                                   :f/v        0
                                   :id         test-utils/commit-id?}}
              commit-4 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id test-utils/did?}
                                   :f/address  test-utils/address?
                                   :f/alias    "committest"
                                   :f/author   ""
                                   :f/branch   "main"
                                   :f/data     {:f/address  test-utils/address?
                                                :f/assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/cat}]
                                                :f/flakes   53
                                                :f/previous {:id test-utils/db-id?}
                                                :f/retract  []
                                                :f/size     pos-int?
                                                :f/t        4
                                                :id         test-utils/db-id?}
                                   :f/previous {:id test-utils/commit-id?}
                                   :f/time     720000
                                   :f/txn      string?
                                   :f/v        0
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
                              :f/author   ""
                              :f/branch   "main"
                              :f/data     {:f/address  test-utils/address?
                                           :f/assert   [{:ex/x "foo-cat"
                                                         :ex/y "bar-cat"
                                                         :id   :ex/cat}]
                                           :f/flakes   53
                                           :f/previous {:id test-utils/db-id?}
                                           :f/retract  []
                                           :f/size     pos-int?
                                           :f/t        4
                                           :id         test-utils/db-id?}
                              :f/previous {:id test-utils/commit-id?}
                              :f/time     720000
                              :f/txn      string?
                              :f/v        0
                              :id         test-utils/commit-id?}}
                 c4)))
          (is (pred-match?
                {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    "committest"
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/flakes   36
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        3
                                         :id         test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/txn      string?
                            :f/v        0
                            :id         test-utils/commit-id?}}
               c3))
          (is (pred-match?
                {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address  test-utils/address?
                            :f/alias    "committest"
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/flakes   19
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-1"
                                                       :ex/y "bar-1"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        2
                                         :id         test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/txn      string?
                            :f/v        0
                            :id         test-utils/commit-id?}}
               c2))))

      (testing "time range from"
        (is (pred-match?
              [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address  test-utils/address?
                           :f/alias    "committest"
                           :f/author   ""
                           :f/branch   "main"
                           :f/data     {:f/address  test-utils/address?
                                        :f/assert   [{:ex/x "foo-cat"
                                                      :ex/y "bar-cat"
                                                      :id   :ex/cat}]
                                        :f/flakes   53
                                        :f/previous {:id test-utils/db-id?}
                                        :f/retract  []
                                        :f/size     pos-int?
                                        :f/t        4
                                        :id         test-utils/db-id?}
                           :f/previous {:id test-utils/commit-id?}
                           :f/time     720000
                           :f/txn      string?
                           :f/v        0
                           :id         test-utils/commit-id?}}
               {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address  test-utils/address?
                           :f/alias    "committest"
                           :f/author   ""
                           :f/branch   "main"
                           :f/data     {:f/address  test-utils/address?
                                        :f/assert   [{:ex/x "foo-cat"
                                                      :ex/y "bar-cat"
                                                      :id   :ex/alice}]
                                        :f/flakes   70
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
                           :f/txn      string?
                           :f/v        0
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
                           :f/author   ""
                           :f/branch   "main"
                           :f/previous {:id test-utils/commit-id?}
                           :f/data     {:f/address  test-utils/address?
                                        :f/assert   [{:ex/x "foo-1"
                                                      :ex/y "bar-1"
                                                      :id   :ex/alice}]
                                        :f/flakes   2
                                        :f/retract  []
                                        :f/size     pos-int?
                                        :f/t        1
                                        :f/previous {:id test-utils/db-id?}
                                        :id         test-utils/db-id?}
                           :f/time     720000
                           :f/txn      string?
                           :f/v        0
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
                             :f/author   ""
                             :f/branch   "main"
                             :f/data     {:f/address  test-utils/address?
                                          :f/assert   [{:ex/x "foo-3"
                                                        :ex/y "bar-3"
                                                        :id   :ex/alice}]
                                          :f/flakes   36
                                          :f/previous {:id test-utils/db-id?}
                                          :f/retract  [{:ex/x "foo-2"
                                                        :ex/y "bar-2"
                                                        :id   :ex/alice}]
                                          :f/size     pos-int?
                                          :f/t        3
                                          :id         test-utils/db-id?}
                             :f/previous {:id test-utils/commit-id?}
                             :f/time     720000
                             :f/txn      string?
                             :f/v        0
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
                             :f/author   ""
                             :f/branch   "main"
                             :f/data     {:f/address  test-utils/address?
                                          :f/assert   [{:ex/x "foo-cat"
                                                        :ex/y "bar-cat"
                                                        :id   :ex/alice}]
                                          :f/flakes   70
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
                             :f/txn      string?
                             :f/v        0
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

(deftest loaded-mem-ledger-history-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded memory ledger"
      (let [ledger-name   "loaded-history-mem"
            conn          @(fluree/connect {:method :memory})
            ledger        @(fluree/create conn ledger-name)
            context       [test-utils/default-context {:ex "http://example.org/ns/"}]
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-1"
                                                                    :ex/y "bar-1"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "delete"   {:id   :ex/alice
                                                                    :ex/x "foo-1"
                                                                    :ex/y "bar-1"}
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-2"
                                                                    :ex/y "bar-2"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "delete"   {:id   :ex/alice
                                                                    :ex/x "foo-2"
                                                                    :ex/y "bar-2"}
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-3"
                                                                    :ex/y "bar-3"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/cat
                                                                    :ex/x "foo-cat"
                                                                    :ex/y "bar-cat"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
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
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/flakes   38
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        3
                                         :id test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/txn      string?
                            :f/v        0
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
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-cat"
                                                       :ex/y "bar-cat"
                                                       :id   :ex/alice}]
                                         :f/flakes   72
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
                            :f/txn      string?
                            :f/v        0
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
            conn        @(fluree/connect {:method   :memory
                                          :defaults {:did (did/private->did-map
                                                           test-utils/default-private-key)}})
            ledger      @(fluree/create conn ledger-name)
            context     [test-utils/default-context {:ex "http://example.org/ns/"}]

            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-1"
                                                                    :ex/y "bar-1"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-2"
                                                                    :ex/y "bar-2"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/alice
                                                                    :ex/x "foo-3"
                                                                    :ex/y "bar-3"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
                                                        "insert"   {:id   :ex/cat
                                                                    :ex/x "foo-cat"
                                                                    :ex/y "bar-cat"}})
            _             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                    context]
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
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/flakes   36
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-2"
                                                       :ex/y "bar-2"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        3
                                         :id         test-utils/db-id?}
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/txn      string?
                            :f/v        0
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
                            :f/author   ""
                            :f/branch   "main"
                            :f/data     {:f/address  test-utils/address?
                                         :f/assert   [{:ex/x "foo-cat"
                                                       :ex/y "bar-cat"
                                                       :id   :ex/alice}]
                                         :f/flakes   70
                                         :f/previous {:id test-utils/db-id?}
                                         :f/retract  [{:ex/x "foo-3"
                                                       :ex/y "bar-3"
                                                       :id   :ex/alice}]
                                         :f/size     pos-int?
                                         :f/t        5
                                         :id test-utils/db-id?}
                            :f/message  "meow"
                            :f/previous {:id test-utils/commit-id?}
                            :f/time     720000
                            :f/txn      string?
                            :f/v        0
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
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded file ledger"
      (with-tmp-dir storage-path
        (let [ledger-name "loaded-history-file"
              conn        @(fluree/connect {:method       :file
                                            :storage-path storage-path
                                            :defaults     {:did (did/private->did-map
                                                                 test-utils/default-private-key)}})
              ledger      @(fluree/create conn ledger-name)
              context     [test-utils/default-context {:ex "http://example.org/ns/"}]

              a             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                      context]
                                                          "insert"   {:id   :ex/alice
                                                                      :ex/x "foo-1"
                                                                      :ex/y "bar-1"}})
              b             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                      context]
                                                          "delete"   {:id   :ex/alice
                                                                      :ex/x "foo-1"
                                                                      :ex/y "bar-1"}
                                                          "insert"   {:id   :ex/alice
                                                                      :ex/x "foo-2"
                                                                      :ex/y "bar-2"}})
              c             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                      context]
                                                          "delete"   {:id   :ex/alice
                                                                      :ex/x "foo-2"
                                                                      :ex/y "bar-2"}
                                                          "insert"   {:id   :ex/alice
                                                                      :ex/x "foo-3"
                                                                      :ex/y "bar-3"}})
              d             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                      context]
                                                          "insert"   {:id   :ex/cat
                                                                      :ex/x "foo-cat"
                                                                      :ex/y "bar-cat"}})
              e             @(test-utils/transact ledger {"@context" ["https://ns.flur.ee"
                                                                      context]
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
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id test-utils/did?}
                              :f/address  test-utils/address?
                              :f/alias    ledger-name
                              :f/author   ""
                              :f/branch   "main"
                              :f/data     {:f/address  test-utils/address?
                                           :f/assert   [{:ex/x "foo-3"
                                                         :ex/y "bar-3"
                                                         :id   :ex/alice}]
                                           :f/flakes   40
                                           :f/previous {:id test-utils/db-id?}
                                           :f/retract  [{:ex/x "foo-2"
                                                         :ex/y "bar-2"
                                                         :id   :ex/alice}]
                                           :f/size     pos-int?
                                           :f/t        3
                                           :id         test-utils/db-id?}
                              :f/previous {:id test-utils/commit-id?}
                              :f/time     720000
                              :f/txn      string?
                              :f/v        0
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
                              :f/author   ""
                              :f/branch   "main"
                              :f/data     {:f/address  test-utils/address?
                                           :f/assert   [{:ex/x "foo-cat"
                                                         :ex/y "bar-cat"
                                                         :id   :ex/alice}]
                                           :f/flakes   76
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
                              :f/txn      string?
                              :f/v        0
                              :id         test-utils/commit-id?}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history loaded-ledger {:context        context
                                               :history        :ex/alice
                                               :commit-details true
                                               :t              {:from 3}}))))))))

(deftest ^:integration author-and-txn-id
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn         @(fluree/connect {:method :memory})
          ledger-name  "authortest"
          ledger       @(fluree/create conn ledger-name)
          context      [test-utils/default-str-context "https://ns.flur.ee" {"ex" "http://example.org/ns/"}]
          root-privkey "89e0ab9ac36fb82b172890c89e9e231224264c7c757d58cfd8fcd6f3d4442199"
          root-did     (:id (did/private->did-map root-privkey))

          db0 @(fluree/db ledger)
          db1 @(fluree/stage db0 {"@context" context
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
                                              {"@id"    root-did
                                               "f:role" {"@id" "ex:rootRole"}}]})
          db2 (->> @(fluree/stage db1 {"@context" context
                                       "insert"   {"@id"          "ex:rootPolicy"
                                                   "@type"        ["f:Policy"]
                                                   "f:targetNode" {"@id" "f:allNodes"}
                                                   "f:allow"      [{"@id"          "ex:rootAccessAllow"
                                                                    "f:targetRole" {"@id" "ex:rootRole"}
                                                                    "f:action"     [{"@id" "f:view"}
                                                                                    {"@id" "f:modify"}]}]}})
                   (fluree/commit! ledger)
                   (deref))

          db3 @(test-utils/transact ledger (crypto/create-jws
                                             (json/stringify {"@context" context "insert" {"ex:foo" 3}})
                                             root-privkey))

          db4 @(test-utils/transact ledger (crypto/create-jws
                                             (json/stringify {"@context" context "insert" {"ex:foo" 5}})
                                             root-privkey))]
      (is (= [{"f:author" "", "f:txn" "", "f:data" {"f:t" 1}}
              {"f:author" "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
               "f:txn" "fluree:memory://8bf810310a1f54a186acde9c7f05c91e0cf0facf6fb9ce5aa17987be0957bfd9",
               "f:data" {"f:t" 2}}
              {"f:author" "did:fluree:Tf8ziWxPPA511tcGtUHTLYihHSy2phNjrKb",
               "f:txn" "fluree:memory://3b4f6c72ffbea9b6c601fb30d1df4934520dfffc20139ece2aecb15b6a610804",
               "f:data" {"f:t" 3}}]
             (->> @(fluree/history ledger {:context        context
                                           :commit-details true
                                           :t              {:from 1 :to :latest}})
                  (mapv (fn [c]
                          (-> (get c "f:commit")
                              (update "f:data" select-keys ["f:t"])
                              (select-keys ["f:author" "f:txn" "f:data"]))))))))))

(deftest ^:integration txn-annotation
  (let [bnode-counter (atom 0)]
    (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")
                  fluree.db.json-ld.iri/new-blank-node-id (fn [] (str "_:fdb-" (swap! bnode-counter inc)))]
      (let [conn         @(fluree/connect {:method :memory})
            ledger-name  "annotationtest"
            ledger       @(fluree/create conn ledger-name)
            context      [test-utils/default-str-context "https://ns.flur.ee" {"ex" "http://example.org/ns/"}]

            db0 @(fluree/db ledger)
            db1 (->> @(fluree/stage db0 {"@context" context
                                         "insert" [{"@id" "ex:betty"
                                                    "@type" "ex:Yeti"
                                                    "schema:name" "Betty"
                                                    "schema:age" 55}]})
                     (fluree/commit! ledger)
                     (deref))





            db2 (->> @(fluree/stage db1 {"@context" context
                                         "insert" [{"@id" "ex:freddy"
                                                    "@type" "ex:Yeti"
                                                    "schema:name" "Freddy"
                                                    "schema:age" 1002}]}
                                    {:annotation {"ex:originator" "opts" "ex:data" "ok"}})
                     (fluree/commit! ledger)
                     (deref))

            db3 (->> @(fluree/stage db2 {"@context" context
                                         "insert"   [{"@id"         "ex:letty"
                                                      "@type"       "ex:Yeti"
                                                      "schema:name" "Leticia"
                                                      "schema:age"  38}]
                                         "opts" {"annotation" {"ex:originator" "txn" "ex:data" "ok"}}})
                     (fluree/commit! ledger)
                     (deref))]
        (testing "annotations in commit-details"
          (is (= [{"f:txn" "fluree:memory://2c28ed0d4102ff436641911c90f47f0681a9ae251640823f7f361f7e22a57fef"}
                  {"f:txn" "fluree:memory://cd26d7c28c0a5368694a6b8fbd11c573f0623313c06ae96c15ff016572972568"
                   "f:annotation" {"id" "_:fdb-3" "ex:data" "ok" "ex:originator" "opts"}}

                  {"f:txn" "fluree:memory://40ce18ee7469c565297e9b9e348788a891ab1bca8fdfda2116d7a2b6c2c82a86"
                   "f:annotation" {"id" "_:fdb-5" "ex:data" "ok" "ex:originator" "txn"}}]
                 (->> @(fluree/history ledger {:context        context
                                               :commit-details true
                                               :t              {:from 1 :to :latest}})
                      (mapv (fn [c] (-> c (get "f:commit") (select-keys ["f:txn" "f:annotation"]))))))))

        (testing "only single annotation subject permitted"
          (let [invalid1 @(fluree/stage db0 {"@context" context
                                             "insert" [{"@id" "ex:betty"
                                                        "@type" "ex:Yeti"
                                                        "schema:name" "Betty"
                                                        "schema:age" 55}]}
                                        {:annotation {"ex:originator" "opts" "ex:nested" {"invalid" "true"}}})
                invalid2 @(fluree/stage db0 {"@context" context
                                             "insert" [{"@id" "ex:betty"
                                                        "@type" "ex:Yeti"
                                                        "schema:name" "Betty"
                                                        "schema:age" 55}]}
                                        {:annotation [{"ex:originator" "opts" "ex:multiple" true}
                                                      {"ex:originator" "opts" "ex:invalid" true}]})]
            (is (= "Commit annotation must only have a single subject." (ex-message invalid1)))
            (is (= "Commit annotation must only have a single subject." (ex-message invalid2)))))

        (testing "annotation has no references"
          (let [invalid3 @(fluree/stage db0 {"@context" context
                                             "insert" [{"@id" "ex:betty"
                                                        "@type" "ex:Yeti"
                                                        "schema:name" "Betty"
                                                        "schema:age" 55}]}
                                        {:annotation [{"ex:originator" "opts" "@id" "invalid:subj"}]})]
            (is (= "Commit annotation cannot specify a subject identifier." (ex-message invalid3)))))))))
