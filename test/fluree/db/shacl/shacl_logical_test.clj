(ns fluree.db.shacl.shacl-logical-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest ^:integration shacl-not-test
  (testing "shacl basic not constraint works"
    (let [conn             (test-utils/create-conn)
          ledger           @(fluree/create conn "shacl/a"
                                           {:defaultContext
                                            ["" {:ex "http://example.org/ns/"}]})
          user-query       {:select {'?s [:*]}
                            :where  {:id '?s, :type :ex/User}}
          db               @(fluree/stage
                              (fluree/db ledger)
                              {:id             :ex/UserShape
                               :type           [:sh/NodeShape]
                               :sh/targetClass :ex/User
                               :sh/not         [{:sh/path     :schema/companyName
                                                 :sh/minCount 1}
                                                {:sh/path   :schema/name
                                                 :sh/equals :schema/callSign}]
                               :sh/property    [{:sh/path     :schema/callSign
                                                 :sh/minCount 1
                                                 :sh/maxCount 1
                                                 :sh/datatype :xsd/string}]})
          db-ok            @(fluree/stage
                              db
                              {:id              :ex/john,
                               :type            [:ex/User],
                               :schema/name     "John"
                               :schema/callSign "j-rock"})
          db-company-name  (try
                             @(fluree/stage
                                db
                                {:id                 :ex/john,
                                 :type               [:ex/User],
                                 :schema/companyName "WrongCo"
                                 :schema/callSign    "j-rock"})
                             (catch Exception e e))
          db-two-names     (try
                             @(fluree/stage
                                db
                                {:id                 :ex/john,
                                 :type               [:ex/User],
                                 :schema/companyName ["John", "Johnny"]
                                 :schema/callSign    "j-rock"})
                             (catch Exception e e))
          db-callsign-name (try
                             @(fluree/stage
                                db
                                {:id              :ex/john
                                 :type            [:ex/User]
                                 :schema/name     "Johnny Boy"
                                 :schema/callSign "Johnny Boy"})
                             (catch Exception e e))
          ok-results       @(fluree/query db-ok user-query)]
      (is (util/exception? db-company-name))
      (is (= "SHACL PropertyShape exception - sh:not sh:minCount of 1 requires lower count but actual count was 1."
             (ex-message db-company-name)))
      (is (util/exception? db-two-names))
      (is (= "SHACL PropertyShape exception - sh:not sh:minCount of 1 requires lower count but actual count was 2."
             (ex-message db-two-names)))
      (is (util/exception? db-callsign-name))
      (is (= "SHACL PropertyShape exception - sh:not sh:equals: [\"Johnny Boy\"] is required to be not equal to [\"Johnny Boy\"]."
             (ex-message db-callsign-name)))
      (is (= [{:id              :ex/john,
               :type            :ex/User,
               :schema/name     "John",
               :schema/callSign "j-rock"}]
             ok-results)
          (str "unexpected query result: " (pr-str ok-results)))))

  (testing "shacl not w/ value ranges works"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/a"
                                       {:defaultContext
                                        ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  {:id '?s, :type :ex/User}}
          db           @(fluree/stage
                          (fluree/db ledger)
                          {:id             :ex/UserShape
                           :type           [:sh/NodeShape]
                           :sh/targetClass :ex/User
                           :sh/not         [{:sh/path         :schema/age
                                             :sh/minInclusive 130}
                                            {:sh/path         :schema/favNums
                                             :sh/maxExclusive 9000}]
                           :sh/property    [{:sh/path     :schema/age
                                             :sh/minCount 1
                                             :sh/maxCount 1
                                             :sh/datatype :xsd/long}]})
          db-ok        @(fluree/stage
                          db
                          {:id              :ex/john,
                           :type            [:ex/User],
                           :schema/name     "John"
                           :schema/callSign "j-rock"
                           :schema/age      42
                           :schema/favNums  [9004 9008 9015 9016 9023 9042]})
          db-too-old   @(fluree/stage
                          db
                          {:id                 :ex/john,
                           :type               [:ex/User],
                           :schema/companyName "WrongCo"
                           :schema/callSign    "j-rock"
                           :schema/age         131})
          db-too-low   @(fluree/stage
                          db
                          {:id                 :ex/john,
                           :type               [:ex/User],
                           :schema/companyName ["John", "Johnny"]
                           :schema/callSign    "j-rock"
                           :schema/age         27
                           :schema/favNums     [4 8 15 16 23 42]})
          db-two-probs @(fluree/stage
                          db
                          {:id              :ex/john
                           :type            [:ex/User]
                           :schema/name     "Johnny Boy"
                           :schema/callSign "Johnny Boy"
                           :schema/age      900
                           :schema/favNums  [4 8 15 16 23 42]})
          ok-results   @(fluree/query db-ok user-query)]
      (is (util/exception? db-too-old))
      (is (= "SHACL PropertyShape exception - sh:not sh:minInclusive: value 131 must be less than 130."
             (ex-message db-too-old)))
      (is (util/exception? db-too-low))
      (is (= "SHACL PropertyShape exception - sh:not sh:maxExclusive: value 42 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 23 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 16 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 15 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 8 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 4 must be greater than or equal to 9000."
             (ex-message db-too-low)))
      (is (util/exception? db-two-probs))
      (is (str/starts-with? (ex-message db-two-probs)
                            ;; could be either problem so just match common prefix
                            "SHACL PropertyShape exception - sh:not "))
      (is (= [{:id              :ex/john,
               :type            :ex/User,
               :schema/name     "John",
               :schema/callSign "j-rock"
               :schema/age      42
               :schema/favNums  [9004 9008 9015 9016 9023 9042]}]
             ok-results)
          (str "unexpected query result: " (pr-str ok-results)))))

  (testing "shacl not w/ string constraints works"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/str"
                                     {:defaultContext
                                      ["" {:ex "http://example.org/ns/"}]})
          user-query {:select {'?s [:*]}
                      :where  {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                        (fluree/db ledger)
                        {:id             :ex/UserShape
                         :type           [:sh/NodeShape]
                         :sh/targetClass :ex/User
                         :sh/not         [{:sh/path      :ex/tag
                                           :sh/minLength 4}
                                          {:sh/path      :schema/name
                                           :sh/maxLength 10}
                                          {:sh/path    :ex/greeting
                                           :sh/pattern "hello.*"}]})
          db-ok-name @(fluree/stage
                        db
                        {:id          :ex/jean-claude
                         :type        :ex/User,
                         :schema/name "Jean-Claude"})
          db-ok-tag  @(fluree/stage
                        db
                        {:id     :ex/al,
                         :type   :ex/User,
                         :ex/tag 1})

          db-ok-greeting        @(fluree/stage
                                   db
                                   {:id          :ex/al,
                                    :type        :ex/User,
                                    :ex/greeting "HOWDY"})
          db-name-too-short     (try @(fluree/stage
                                        db
                                        {:id          :ex/john,
                                         :type        [:ex/User],
                                         :schema/name "John"})
                                     (catch Exception e e))
          db-tag-too-long       (try @(fluree/stage
                                        db
                                        {:id     :ex/john,
                                         :type   [:ex/User],
                                         :ex/tag 12345})
                                     (catch Exception e e))
          db-greeting-incorrect (try @(fluree/stage
                                        db
                                        {:id          :ex/john,
                                         :type        [:ex/User],
                                         :ex/greeting "hello!"})
                                     (catch Exception e e))]
      (is (util/exception? db-name-too-short))
      (is (= "SHACL PropertyShape exception - sh:not sh:maxLength: value John must have string length greater than 10."
             (ex-message db-name-too-short)))
      (is (util/exception? db-tag-too-long))
      (is (= "SHACL PropertyShape exception - sh:not sh:minLength: value 12345 must have string length less than 4."
             (ex-message db-tag-too-long)))
      (is (util/exception? db-greeting-incorrect))
      (is (= "SHACL PropertyShape exception - sh:not sh:pattern: value hello! must not match pattern \"hello.*\"."
             (ex-message db-greeting-incorrect)))
      (is (= [{:id          :ex/jean-claude
               :type        :ex/User,
               :schema/name "Jean-Claude"}]
             @(fluree/query db-ok-name user-query)))
      (is (= [{:id     :ex/al,
               :type   :ex/User,
               :ex/tag 1}]
             @(fluree/query db-ok-tag user-query)))
      (is (= [{:id          :ex/al,
               :type        :ex/User,
               :ex/greeting "HOWDY"}]
             @(fluree/query db-ok-greeting user-query))))))
