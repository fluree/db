(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]
            [fluree.db.util.core :as util]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration history-query-test
  (let [ts-primeval (util/current-time-iso)

        conn        (test-utils/create-conn)
        ledger      @(fluree/create conn "historytest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

        db1         @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                  "insert" [{:id   :ex/dan
                                                             :ex/x "foo-1"
                                                             :ex/y "bar-1"}
                                                            {:id   :ex/cat
                                                             :ex/x "foo-1"
                                                             :ex/y "bar-1"}
                                                            {:id   :ex/dog
                                                             :ex/x "foo-1"
                                                             :ex/y "bar-1"}]})
        db2         @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                  "delete" {:id :ex/dan
                                                            :ex/x "foo-1"
                                                            :ex/y "bar-1"}
                                                  "insert" {:id   :ex/dan
                                                            :ex/x "foo-2"
                                                            :ex/y "bar-2"}})
        ts2         (-> db2 :commit :time)
        db3         @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                  "delete" {:id   :ex/dan
                                                            :ex/x "foo-2"
                                                            :ex/y "bar-2"}
                                                  "insert" {:id   :ex/dan
                                                            :ex/x "foo-3"
                                                            :ex/y "bar-3"}})

        ts3         (-> db3 :commit :time)
        db4         @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                  "delete" [{:id   :ex/cat
                                                             :ex/x "foo-1"
                                                             :ex/y "bar-1"}
                                                            {:id   :ex/dog
                                                             :ex/x "foo-1"
                                                             :ex/y "bar-1"}]
                                                  "insert" [{:id   :ex/cat
                                                             :ex/x "foo-cat"
                                                             :ex/y "bar-cat"}
                                                            {:id   :ex/dog
                                                             :ex/x "foo-dog"
                                                             :ex/y "bar-dog"}]})
        db5         @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                  "delete" {:id   :ex/dan
                                                            :ex/x "foo-3"
                                                            :ex/y "bar-3"}
                                                  "insert" {:id   :ex/dan
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
             @(fluree/history ledger {:history :ex/dan :t {:from 1}}))))
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

      (is (= [{:f/t       1
               :f/assert #{{:ex/x "foo-1" :id :ex/dog}
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
             (->> @(fluree/history ledger {:history [nil :ex/x] :t {:from 1}})
                  (mapv #(-> % (update :f/assert set) (update :f/retract set)))))))
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
               :f/assert  [{:ex/x "foo-cat" :id :ex/cat} {:ex/x "foo-dog" :id :ex/dog}]
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
             (-> @(fluree/history ledger {:history [:ex/dan :ex/x]
                                          :t       {:from ts-primeval}})
                 Throwable->map
                 :cause))))

    #_(testing "invalid query"
        (is (= "History query not properly formatted. Provided {:history []}"
               (-> @(fluree/history ledger {:history []})
                   Throwable->map
                   :cause))))

    (testing "small cache"
      (let [conn   (test-utils/create-conn)
            ledger @(fluree/create conn "historycachetest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

            db1    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                 "insert" [{:id   :ex/dan
                                                            :ex/x "foo-1"
                                                            :ex/y "bar-1"}]})
            db2    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                 "delete" {:id   :ex/dan
                                                           :ex/x "foo-1"
                                                           :ex/y "bar-1"}
                                                 "insert" {:id   :ex/dan
                                                           :ex/x "foo-2"
                                                           :ex/y "bar-2"}})]
        (testing "no t-range cache collision"
          (is (= [{:f/t       2
                   :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:history [:ex/dan] :t {:from 2}}))))))))

(deftest ^:integration commit-details-test
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "committest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

          db1    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                               "insert" {:id   :ex/alice
                                                         :ex/x "foo-1"
                                                         :ex/y "bar-1"}})
          db2    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                               "insert" {:id   :ex/alice
                                                         :ex/x "foo-2"
                                                         :ex/y "bar-2"}})
          db3    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                               "insert" {:id   :ex/alice
                                                         :ex/x "foo-3"
                                                         :ex/y "bar-3"}})
          db4    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                               "insert" {:id   :ex/cat
                                                         :ex/x "foo-cat"
                                                         :ex/y "bar-cat"}})
          db5    @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                               "insert" {:id   :ex/alice
                                                         :ex/x "foo-cat"
                                                         :ex/y "bar-cat"}}
                                       {:message "meow"})]
      (testing "at time t"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address        test-utils/address?
                          :f/alias          "committest"
                          :f/branch         "main"
                          :f/defaultContext {:id test-utils/context-id?}
                          :f/data           #:f{:address test-utils/address?
                                                :assert  [{:ex/x "foo-1"
                                                           :ex/y "bar-1"
                                                           :id   :ex/alice}]
                                                :flakes  7
                                                :retract []
                                                :size    pos-int?
                                                :t       1}
                          :f/time           720000
                          :f/v              0
                          :id               test-utils/commit-id?}}]
             @(fluree/history ledger {:commit-details true :t {:from 1 :to 1}})))
        (let [commit-5 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id test-utils/did?}
                                   :f/address        test-utils/address?
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id test-utils/context-id?}
                                   :f/data           #:f{:address  test-utils/address?
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/alice}]
                                                         :flakes   94
                                                         :previous {:id test-utils/db-id?}
                                                         :retract  [{:ex/x "foo-3"
                                                                     :ex/y "bar-3"
                                                                     :id   :ex/alice}]
                                                         :size     pos-int?
                                                         :t        5}
                                   :f/message        "meow"
                                   :f/previous       {:id test-utils/commit-id?}
                                   :f/time           720000
                                   :f/v              0
                                   :id               test-utils/commit-id?}}
              commit-4 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id test-utils/did?}
                                   :f/address        test-utils/address?
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id test-utils/context-id?}
                                   :f/data           #:f{:address  test-utils/address?
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/cat}]
                                                         :flakes   76
                                                         :previous {:id test-utils/db-id?}
                                                         :retract  []
                                                         :size     pos-int?
                                                         :t        4}
                                   :f/previous       {:id test-utils/commit-id?}
                                   :f/time           720000
                                   :f/v              0
                                   :id               test-utils/commit-id?}}]
          (is (pred-match?
               [commit-4 commit-5]
               @(fluree/history ledger {:commit-details true :t {:from 4 :to 5}})))
          (is (pred-match?
               [commit-5]
               @(fluree/history ledger {:commit-details true :t {:at :latest}})))))

      (testing "time range"
        (let [[c2 c3 c4 :as response] @(fluree/history
                                        ledger
                                        {:commit-details true
                                         :t              {:from 2 :to 4}})]
          (testing "all commits in time range are returned"
            (is (= 3 (count response)))
            (is (pred-match?
                 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id test-utils/did?}
                             :f/address        test-utils/address?
                             :f/alias          "committest"
                             :f/branch         "main"
                             :f/defaultContext {:id test-utils/context-id?}
                             :f/data           #:f{:address  test-utils/address?
                                                   :assert   [{:ex/x "foo-cat"
                                                               :ex/y "bar-cat"
                                                               :id   :ex/cat}]
                                                   :flakes   76
                                                   :previous {:id test-utils/db-id?}
                                                   :retract  []
                                                   :size     pos-int?
                                                   :t        4}
                             :f/previous       {:id test-utils/commit-id?}
                             :f/time           720000
                             :f/v              0
                             :id               test-utils/commit-id?}}
                 c4)))
          (is (pred-match?
               {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address        test-utils/address?
                           :f/alias          "committest"
                           :f/branch         "main"
                           :f/defaultContext {:id test-utils/context-id?}
                           :f/data           #:f{:address  test-utils/address?
                                                 :assert   [{:ex/x "foo-3"
                                                             :ex/y "bar-3"
                                                             :id   :ex/alice}]
                                                 :flakes   57
                                                 :previous {:id test-utils/db-id?}
                                                 :retract  [{:ex/x "foo-2"
                                                             :ex/y "bar-2"
                                                             :id   :ex/alice}]
                                                 :size     pos-int?
                                                 :t        3}
                           :f/previous       {:id test-utils/commit-id?}
                           :f/time           720000
                           :f/v              0
                           :id               test-utils/commit-id?}}
               c3))
          (is (pred-match?
               {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                           {:id test-utils/did?}
                           :f/address        test-utils/address?
                           :f/alias          "committest"
                           :f/branch         "main"
                           :f/defaultContext {:id test-utils/context-id?}
                           :f/data           #:f{:address  test-utils/address?
                                                 :assert   [{:ex/x "foo-2"
                                                             :ex/y "bar-2"
                                                             :id   :ex/alice}]
                                                 :flakes   39
                                                 :previous {:id test-utils/db-id?}
                                                 :retract  [{:ex/x "foo-1"
                                                             :ex/y "bar-1"
                                                             :id   :ex/alice}]
                                                 :size     pos-int?
                                                 :t        2}
                           :f/previous       {:id test-utils/commit-id?}
                           :f/time           720000
                           :f/v              0
                           :id               test-utils/commit-id?}}
               c2))))

      (testing "time range from"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address        test-utils/address?
                          :f/alias          "committest"
                          :f/branch         "main"
                          :f/defaultContext {:id test-utils/context-id?}
                          :f/data           #:f{:address  test-utils/address?
                                                :assert   [{:ex/x "foo-cat"
                                                            :ex/y "bar-cat"
                                                            :id   :ex/cat}]
                                                :flakes   76
                                                :previous {:id test-utils/db-id?}
                                                :retract  []
                                                :size     pos-int?
                                                :t        4}
                          :f/previous       {:id test-utils/commit-id?}
                          :f/time           720000
                          :f/v              0
                          :id               test-utils/commit-id?}}
              {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address        test-utils/address?
                          :f/alias          "committest"
                          :f/branch         "main"
                          :f/defaultContext {:id test-utils/context-id?}
                          :f/data           #:f{:address  test-utils/address?
                                                :assert   [{:ex/x "foo-cat"
                                                            :ex/y "bar-cat"
                                                            :id   :ex/alice}]
                                                :flakes   94
                                                :previous {:id test-utils/db-id?}
                                                :retract  [{:ex/x "foo-3"
                                                            :ex/y "bar-3"
                                                            :id   :ex/alice}]
                                                :size     pos-int?
                                                :t        5}
                          :f/message        "meow"
                          :f/previous       {:id test-utils/commit-id?}
                          :f/time           720000
                          :f/v              0
                          :id               test-utils/commit-id?}}]
             @(fluree/history ledger {:commit-details true :t {:from 4}}))))

      (testing "time range to"
        (is (pred-match?
             [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                          {:id test-utils/did?}
                          :f/address        test-utils/address?
                          :f/alias          "committest"
                          :f/branch         "main"
                          :f/defaultContext {:id test-utils/context-id?}
                          :f/data           #:f{:address test-utils/address?
                                                :assert  [{:ex/x "foo-1"
                                                           :ex/y "bar-1"
                                                           :id   :ex/alice}]
                                                :flakes  7
                                                :retract []
                                                :size    pos-int?
                                                :t       1}
                          :f/time           720000
                          :f/v              0
                          :id               test-utils/commit-id?}}]
             @(fluree/history ledger {:commit-details true :t {:to 1}}))))

      (testing "history commit details"
        (is (pred-match?
             [#:f{:assert  [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address        test-utils/address?
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :flakes   57
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-2"
                                                              :ex/y "bar-2"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        3}
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address        test-utils/address?
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                  :flakes   94
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        5}
                            :f/message        "meow"
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history ledger {:history        :ex/alice
                                      :commit-details true
                                      :t              {:from 3}}))))
      (testing "multiple history results"
        (let [history-with-commits @(fluree/history ledger {:history :ex/alice :commit-details true :t {:from 1 :to 5}})]
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
            conn          @(fluree/connect
                            {:method :memory
                             :defaults
                             {:context      test-utils/default-context
                              :context-type :keyword}})
            ledger        @(fluree/create conn ledger-name
                                          {:defaultContext
                                           ["" {:ex "http://example.org/ns/"}]})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-1"
                                                                  :ex/y "bar-1"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "delete" {:id   :ex/alice
                                                                  :ex/x "foo-1"
                                                                  :ex/y "bar-1"}
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-2"
                                                                  :ex/y "bar-2"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "delete" {:id   :ex/alice
                                                                  :ex/x "foo-2"
                                                                  :ex/y "bar-2"}
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-3"
                                                                  :ex/y "bar-3"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/cat
                                                                  :ex/x "foo-cat"
                                                                  :ex/y "bar-cat"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "delete" {:id   :ex/alice
                                                                  :ex/x "foo-3"
                                                                  :ex/y "bar-3"}
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-cat"
                                                                  :ex/y "bar-cat"}}
                                                {:message "meow"})
            loaded-ledger (test-utils/retry-load conn ledger-name 100)]
        (is (pred-match?
             [#:f{:assert  [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :commit  {:f/address        test-utils/address?
                            :f/alias          ledger-name
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :flakes   58
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-2"
                                                              :ex/y "bar-2"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        3}
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {:f/address        test-utils/address?
                            :f/alias          ledger-name
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                  :flakes   95
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        5}
                            :f/message        "meow"
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history loaded-ledger {:history        :ex/alice
                                             :commit-details true
                                             :t              {:from 3}})))))

    (testing "history commit details on a loaded memory ledger w/ issuer"
      (let [ledger-name   "loaded-history-mem-issuer"
            conn          @(fluree/connect
                            {:method :memory
                             :defaults
                             {:context      test-utils/default-context
                              :context-type :keyword
                              :did          (did/private->did-map
                                             test-utils/default-private-key)}})
            ledger        @(fluree/create conn ledger-name
                                          {:defaultContext
                                           ["" {:ex "http://example.org/ns/"}]})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-1"
                                                                  :ex/y "bar-1"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-2"
                                                                  :ex/y "bar-2"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/alice
                                                                  :ex/x "foo-3"
                                                                  :ex/y "bar-3"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/cat
                                                                  :ex/x "foo-cat"
                                                                  :ex/y "bar-cat"}})
            _             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                        "insert" {:id   :ex/alice
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
                            :f/address        test-utils/address?
                            :f/alias          ledger-name
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :flakes   57
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-2"
                                                              :ex/y "bar-2"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        3}
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-2"
                             :ex/y "bar-2"
                             :id   :ex/alice}]
                  :t       3}
              #:f{:assert  [{:ex/x "foo-cat"
                             :ex/y "bar-cat"
                             :id   :ex/alice}]
                  :commit  {"https://www.w3.org/2018/credentials#issuer"
                            {:id test-utils/did?}
                            :f/address        test-utils/address?
                            :f/alias          ledger-name
                            :f/branch         "main"
                            :f/defaultContext {:id test-utils/context-id?}
                            :f/data           #:f{:address  test-utils/address?
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                  :flakes   94
                                                  :previous {:id test-utils/db-id?}
                                                  :retract  [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :size     pos-int?
                                                  :t        5}
                            :f/message        "meow"
                            :f/previous       {:id test-utils/commit-id?}
                            :f/time           720000
                            :f/v              0
                            :id               test-utils/commit-id?}
                  :retract [{:ex/x "foo-3"
                             :ex/y "bar-3"
                             :id   :ex/alice}]
                  :t       5}]
             @(fluree/history loaded-ledger {:history        :ex/alice
                                             :commit-details true
                                             :t              {:from 3}})))))))

(deftest loaded-file-ledger-history-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded file ledger"
      (with-tmp-dir storage-path
        (let [ledger-name   "loaded-history-file"
              conn          @(fluree/connect
                              {:method       :file
                               :storage-path storage-path
                               :defaults
                               {:context      test-utils/default-context
                                :context-type :keyword
                                :did          (did/private->did-map
                                               test-utils/default-private-key)}})
              ledger        @(fluree/create conn ledger-name
                                            {:defaultContext
                                             ["" {:ex "http://example.org/ns/"}]})
              a             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                          "insert" {:id   :ex/alice
                                                                    :ex/x "foo-1"
                                                                    :ex/y "bar-1"}})
              b             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                          "delete" {:id   :ex/alice
                                                                    :ex/x "foo-1"
                                                                    :ex/y "bar-1"}
                                                          "insert" {:id   :ex/alice
                                                                    :ex/x "foo-2"
                                                                    :ex/y "bar-2"}})
              c             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                          "delete" {:id   :ex/alice
                                                                    :ex/x "foo-2"
                                                                    :ex/y "bar-2"}
                                                          "insert" {:id   :ex/alice
                                                                    :ex/x "foo-3"
                                                                    :ex/y "bar-3"}})
              d             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                          "insert" {:id   :ex/cat
                                                                    :ex/x "foo-cat"
                                                                    :ex/y "bar-cat"}})
              e             @(test-utils/transact ledger {"@context" "https://ns.flur.ee"
                                                          "delete" {:id   :ex/alice
                                                                    :ex/x "foo-3"
                                                                    :ex/y "bar-3"}
                                                          "insert" {:id   :ex/alice
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
                              :f/address        test-utils/address?
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id test-utils/context-id?}
                              :f/data           #:f{:address  test-utils/address?
                                                    :assert   [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :flakes   61
                                                    :previous {:id test-utils/db-id?}
                                                    :retract  [{:ex/x "foo-2"
                                                                :ex/y "bar-2"
                                                                :id   :ex/alice}]
                                                    :size     pos-int?
                                                    :t        3}
                              :f/previous       {:id test-utils/commit-id?}
                              :f/time           720000
                              :f/v              0
                              :id               test-utils/commit-id?}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id test-utils/did?}
                              :f/address        test-utils/address?
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id test-utils/context-id?}
                              :f/data           #:f{:address  test-utils/address?
                                                    :assert   [{:ex/x "foo-cat"
                                                                :ex/y "bar-cat"
                                                                :id   :ex/alice}]
                                                    :flakes   100
                                                    :previous {:id test-utils/db-id?}
                                                    :retract  [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :size     pos-int?
                                                    :t        5}
                              :f/message        "meow"
                              :f/previous       {:id test-utils/commit-id?}
                              :f/time           720000
                              :f/v              0
                              :id               test-utils/commit-id?}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history loaded-ledger {:history        :ex/alice
                                               :commit-details true
                                               :t              {:from 3}}))))))))
