(ns fluree.db.shacl.shacl-basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [clojure.string :as str]))

(deftest ^:integration using-pre-defined-types-as-classes
  (testing "Class not used as class initially can still be used as one."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "class/testing" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db1       @(fluree/stage2
                       (fluree/db ledger)
                       {"@context" "https://ns.flur.ee"
                        "insert" {:id :ex/MyClass
                                  :schema/description "Just a basic object not used as a class"}})
          db2       @(fluree/stage2
                       db1
                       {:context "https://ns.flur.ee"
                        "insert" {:id :ex/myClassInstance
                                  :type :ex/MyClass
                                  :schema/description "Now a new subject uses MyClass as a Class"}})
          query-res @(fluree/query db2 '{:select {?s [:*]}
                                         :where  [[?s :id :ex/myClassInstance]]})]
      (is (= query-res
             [{:id                 :ex/myClassInstance
               :type           :ex/MyClass
               :schema/description "Now a new subject uses MyClass as a Class"}])))))


(deftest ^:integration shacl-cardinality-constraints
  (testing "shacl minimum and maximum cardinality"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/a" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :type :ex/User]]}
          db           @(fluree/stage2
                          (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id             :ex/UserShape
                            :type           [:sh/NodeShape]
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path     :schema/name
                                              :sh/minCount 1
                                              :sh/maxCount 1
                                              :sh/datatype :xsd/string}]}})
          db-ok        @(fluree/stage2
                          db
                         {"@context" "https://ns.flur.ee"
                          "insert"
                          {:id              :ex/john
                           :type            :ex/User
                           :schema/name     "John"
                           :schema/callSign "j-rock"}})
          ; no :schema/name
          db-no-names  (try
                         @(fluree/stage2
                            db
                           {"@context" "https://ns.flur.ee"
                            "insert"
                            {:id              :ex/john
                             :type            :ex/User
                             :schema/callSign "j-rock"}})
                         (catch Exception e e))
          db-two-names (try
                         @(fluree/stage2
                            db
                           {"@context" "https://ns.flur.ee"
                            "insert"
                            {:id              :ex/john
                             :type            :ex/User
                             :schema/name     ["John", "Johnny"]
                             :schema/callSign "j-rock"}})
                         (catch Exception e e))]
      (is (util/exception? db-no-names)
          "Exception, because :schema/name requires at least 1 value.")
      (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
             (ex-message db-no-names)))
      (is (util/exception? db-two-names)
          "Exception, because :schema/name can have at most 1 value.")
      (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
             (ex-message db-two-names)))
      (is (= [{:id              :ex/john,
               :type        :ex/User,
               :schema/name     "John",
               :schema/callSign "j-rock"}]
             @(fluree/query db-ok user-query))
          "basic rdf:type query response not correct"))))


(deftest ^:integration shacl-datatype-constraints
  (testing "shacl datatype errors"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :type :ex/User]]}
          db           @(fluree/stage2
                          (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id             :ex/UserShape
                            :type           :sh/NodeShape
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path     :schema/name
                                              :sh/datatype :xsd/string}]}})
          db-ok        @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name "John"}})
          ;; no :schema/name
          db-int-name  @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name 42}})
          db-bool-name @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name true}})]
      (is (util/exception? db-int-name)
          "Exception, because :schema/name is an integer and not a string.")
      (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
             (ex-message db-int-name)))
      (is (util/exception? db-bool-name)
          "Exception, because :schema/name is a boolean and not a string.")
      (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
             (ex-message db-bool-name)))
      (is (= @(fluree/query db-ok user-query)
             [{:id          :ex/john
               :type    :ex/User
               :schema/name "John"}])
          "basic rdf:type query response not correct"))))

(deftest ^:integration shacl-closed-shape
  (testing "shacl closed shape"
    (let [conn          (test-utils/create-conn)
          ledger        @(fluree/create conn "shacl/c" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query    {:select {'?s [:*]}
                         :where  [['?s :type :ex/User]]}
          db            @(fluree/stage2
                           (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id                   :ex/UserShape
                            :type                 :sh/NodeShape
                            :sh/targetClass       :ex/User
                            :sh/property          [{:sh/path     :schema/name
                                                    :sh/datatype :xsd/string}]
                            :sh/closed            true
                            :sh/ignoredProperties [:type]}})

          db-ok         @(fluree/stage2
                           db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name "John"}})
          ; no :schema/name
          db-extra-prop (try
                          @(fluree/stage
                            db
                            {:id           :ex/john
                             :type         :ex/User
                             :schema/name  "John"
                             :schema/email "john@flur.ee"})
                          (catch Exception e e))]
      (is (util/exception? db-extra-prop))
      (is (str/starts-with? (ex-message db-extra-prop)
                            "SHACL shape is closed, extra properties not allowed: [10"))

      (is (= [{:id          :ex/john
               :type    :ex/User
               :schema/name "John"}]
             @(fluree/query db-ok user-query))
          "basic type query response not correct"))))

(deftest ^:integration shacl-property-pairs
  (testing "shacl property pairs"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/pairs" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query {:select {'?s [:*]}
                      :where  [['?s :type :ex/User]]}]
      (testing "single-cardinality equals"
        (let [db           @(fluree/stage2
                              (fluree/db ledger)
                              {"@context" "https://ns.flur.ee"
                               "insert"
                               {:id             :ex/EqualNamesShape
                                :type           :sh/NodeShape
                                :sh/targetClass :ex/User
                                :sh/property    [{:sh/path   :schema/name
                                                  :sh/equals :ex/firstName}]}})
              db-ok        @(fluree/stage2
                              db
                              {"@context" "https://ns.flur.ee"
                               "insert"
                               {:id           :ex/alice
                                :type         :ex/User
                                :schema/name  "Alice"
                                :ex/firstName "Alice"}})

              db-not-equal (try
                             @(fluree/stage2
                                db
                                {"@context" "https://ns.flur.ee"
                                 "insert"
                                 {:id           :ex/john
                                  :type         :ex/User
                                  :schema/name  "John"
                                  :ex/firstName "Jack"}})
                             (catch Exception e e))]
          (is (util/exception? db-not-equal)
              "Exception, because :schema/name does not equal :ex/firstName")
          (is (= "SHACL PropertyShape exception - sh:equals: [\"John\"] not equal to [\"Jack\"]."
                 (ex-message db-not-equal)))

          (is (= [{:id           :ex/alice
                   :type     :ex/User
                   :schema/name  "Alice"
                   :ex/firstName "Alice"}]
                 @(fluree/query db-ok user-query)))))
      (testing "multi-cardinality equals"
        (let [db            @(fluree/stage2
                               (fluree/db ledger)
                               {"@context" "https://ns.flur.ee"
                                "insert"
                                {:id             :ex/EqualNamesShape
                                 :type           :sh/NodeShape
                                 :sh/targetClass :ex/User
                                 :sh/property    [{:sh/path   :ex/favNums
                                                   :sh/equals :ex/luckyNums}]}})
              db-ok         @(fluree/stage2
                               db
                              {"@context" "https://ns.flur.ee"
                               "insert"
                               {:id           :ex/alice
                                :type         :ex/User
                                :schema/name  "Alice"
                                :ex/favNums   [11 17]
                                :ex/luckyNums [11 17]}})

              db-ok2        @(fluree/stage2
                               db
                              {"@context" "https://ns.flur.ee"
                               "insert"
                               {:id           :ex/alice
                                :type         :ex/User
                                :schema/name  "Alice"
                                :ex/favNums   [11 17]
                                :ex/luckyNums [17 11]}})

              db-not-equal1 (try
                              @(fluree/stage2
                                 db
                                {"@context" "https://ns.flur.ee"
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums [13 18]}})
                              (catch Exception e e))
              db-not-equal2 (try
                              @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [11]}})
                              (catch Exception e e))
              db-not-equal3 (try
                              @(fluree/stage2
                                 db
                                {"@context" "https://ns.flur.ee"
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums [11 17 18]}})
                              (catch Exception e e))
              db-not-equal4 (try
                              @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums ["11" "17"]}})
                              (catch Exception e e))]
          (is (util/exception? db-not-equal1)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (= (ex-message db-not-equal1)
                 "SHACL PropertyShape exception - sh:equals: [11 17] not equal to [13 18]."))
          (is (util/exception? db-not-equal2)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:equals: [11 17] not equal to [11]."
                 (ex-message db-not-equal2)))
          (is (util/exception? db-not-equal3)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:equals: [11 17] not equal to [11 17 18]."
                 (ex-message db-not-equal3)))
          (is (util/exception? db-not-equal4)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:equals: [11 17] not equal to [\"11\" \"17\"]."
                 (ex-message db-not-equal4)))
          (is (= [{:id           :ex/alice
                   :type     :ex/User
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums [11 17]}]
                 @(fluree/query db-ok user-query)))
          (is (= [{:id           :ex/alice
                   :type     :ex/User
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums [11 17]}]
                 @(fluree/query db-ok2 user-query)))))
      (testing "disjoint"
        (let [db               @(fluree/stage2
                                  (fluree/db ledger)
                                  {"@context" "https://ns.flur.ee"
                                   "insert"
                                   {:id             :ex/DisjointShape
                                    :type           :sh/NodeShape
                                    :sh/targetClass :ex/User
                                    :sh/property    [{:sh/path     :ex/favNums
                                                      :sh/disjoint :ex/luckyNums}]}})
              db-ok            @(fluree/stage2
                                  db
                                  {"@context" "https://ns.flur.ee"
                                   "insert"
                                   {:id           :ex/alice
                                    :type         :ex/User
                                    :schema/name  "Alice"
                                    :ex/favNums   [11 17]
                                    :ex/luckyNums 1}})

              db-not-disjoint1 (try
                                 @(fluree/stage2
                                    db
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id           :ex/brian
                                      :type         :ex/User
                                      :schema/name  "Brian"
                                      :ex/favNums   11
                                      :ex/luckyNums 11}})
                                 (catch Exception e e))
              db-not-disjoint2 (try
                                 @(fluree/stage2
                                    db
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id           :ex/brian
                                      :type         :ex/User
                                      :schema/name  "Brian"
                                      :ex/favNums   [11 17 31]
                                      :ex/luckyNums 11}})
                                 (catch Exception e e))

              db-not-disjoint3 (try
                                 @(fluree/stage2
                                    db
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id           :ex/brian
                                      :type         :ex/User
                                      :schema/name  "Brian"
                                      :ex/favNums   [11 17 31]
                                      :ex/luckyNums [13 18 11]}})
                                 (catch Exception e e))]
          (is (util/exception? db-not-disjoint1)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:disjoint: [11] not disjoint from [11]."
                 (ex-message db-not-disjoint1)))

          (is (util/exception? db-not-disjoint2)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:disjoint: [11 17 31] not disjoint from [11]."
                 (ex-message db-not-disjoint2)))


          (is (util/exception? db-not-disjoint3)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (= "SHACL PropertyShape exception - sh:disjoint: [11 17 31] not disjoint from [11 13 18]."
                 (ex-message db-not-disjoint3)))

          (is (= [{:id           :ex/alice
                   :type     :ex/User
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums 1}]
                 @(fluree/query db-ok user-query)))))
      (testing "lessThan"
        (let [db       @(fluree/stage2
                          (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id             :ex/LessThanShape
                            :type           :sh/NodeShape
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path     :ex/p1
                                              :sh/lessThan :ex/p2}]}})
              db-ok1   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/alice
                            :type        :ex/User
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       [18 19]}})


              db-ok2   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/alice
                            :type        :ex/User
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       [18]}})

              db-fail1 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       17}})
                         (catch Exception e e))

              db-fail2 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       ["18" "19"]}})
                         (catch Exception e e))


              db-fail3 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [12 17]
                              :ex/p2       [10 18]}})
                         (catch Exception e e))

              db-fail4 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       [12 16]}})
                         (catch Exception e e))
              db-iris  (try @(fluree/stage2
                               db
                               {"@context" "https://ns.flur.ee"
                                "insert"
                                {:id          :ex/alice
                                 :type        :ex/User
                                 :schema/name "Alice"
                                 :ex/p1       :ex/brian
                                 :ex/p2       :ex/john}})
                            (catch Exception e e))]
          (is (util/exception? db-fail1)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThan: 17 not less than 17, or values are not valid for comparison."
                 (ex-message db-fail1)))


          (is (util/exception? db-fail2)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThan: 17 not less than 19, or values are not valid for comparison; sh:lessThan: 17 not less than 18, or values are not valid for comparison; sh:lessThan: 11 not less than 19, or values are not valid for comparison; sh:lessThan: 11 not less than 18, or values are not valid for comparison."
                 (ex-message db-fail2)))

          (is (util/exception? db-fail3)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThan: 17 not less than 10, or values are not valid for comparison; sh:lessThan: 12 not less than 10, or values are not valid for comparison."
                 (ex-message db-fail3)))

          (is (util/exception? db-fail4)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThan: 17 not less than 16, or values are not valid for comparison; sh:lessThan: 17 not less than 12, or values are not valid for comparison."
                 (ex-message db-fail4)))

          (is (util/exception? db-iris)
              "Exception, because :ex/p1 and :ex/p2 are iris, and not valid for comparison")
          (is (str/starts-with? (ex-message db-iris)
                                "SHACL PropertyShape exception - sh:lessThan:"))

          (is (= [{:id          :ex/alice
                   :type    :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [18 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice
                   :type    :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       18}]
                 @(fluree/query db-ok2 user-query)))))
      (testing "lessThanOrEquals"
        (let [db       @(fluree/stage2
                          (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id             :ex/LessThanOrEqualsShape
                            :type           :sh/NodeShape
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path             :ex/p1
                                              :sh/lessThanOrEquals :ex/p2}]}})
              db-ok1   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/alice
                            :type        :ex/User
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       [17 19]}})


              db-ok2   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id          :ex/alice
                            :type        :ex/User
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       17}})

              db-fail1 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       10}})
                         (catch Exception e e))

              db-fail2 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       ["17" "19"]}})
                         (catch Exception e e))

              db-fail3 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [12 17]
                              :ex/p2       [10 17]}})
                         (catch Exception e e))

              db-fail4 (try
                         @(fluree/stage2
                            db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       [12 16]}})
                         (catch Exception e e))]

          (is (util/exception? db-fail1)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThanOrEquals: 17 not less than or equal to 10, or values are not valid for comparison; sh:lessThanOrEquals: 11 not less than or equal to 10, or values are not valid for comparison."
                 (ex-message db-fail1)))


          (is (util/exception? db-fail2)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThanOrEquals: 17 not less than or equal to 19, or values are not valid for comparison; sh:lessThanOrEquals: 17 not less than or equal to 17, or values are not valid for comparison; sh:lessThanOrEquals: 11 not less than or equal to 19, or values are not valid for comparison; sh:lessThanOrEquals: 11 not less than or equal to 17, or values are not valid for comparison."
                 (ex-message db-fail2)))

          (is (util/exception? db-fail3)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThanOrEquals: 17 not less than or equal to 10, or values are not valid for comparison; sh:lessThanOrEquals: 12 not less than or equal to 10, or values are not valid for comparison."
                 (ex-message db-fail3)))

          (is (util/exception? db-fail4)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (= "SHACL PropertyShape exception - sh:lessThanOrEquals: 17 not less than or equal to 16, or values are not valid for comparison; sh:lessThanOrEquals: 17 not less than or equal to 12, or values are not valid for comparison."
                 (ex-message db-fail4)))
          (is (= [{:id          :ex/alice
                   :type    :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [17 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice
                   :type    :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       17}]
                 @(fluree/query db-ok2 user-query))))))))

(deftest ^:integration shacl-value-range
  (testing "shacl value range constraints"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/value-range" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query {:select {'?s [:*]}
                      :where  [['?s :type :ex/User]]}]
      (testing "exclusive constraints"
        (let [db          @(fluree/stage2
                             (fluree/db ledger)
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id             :ex/ExclusiveNumRangeShape
                              :type           :sh/NodeShape
                              :sh/targetClass :ex/User
                              :sh/property    [{:sh/path         :schema/age
                                                :sh/minExclusive 1
                                                :sh/maxExclusive 100}]}})
              db-ok       @(fluree/stage2
                             db
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id         :ex/john
                              :type       :ex/User
                              :schema/age 2}})
              db-too-low  (try @(fluree/stage2
                                  db
                                  {"@context" "https://ns.flur.ee"
                                   "insert"
                                   {:id         :ex/john
                                    :type       :ex/User
                                    :schema/age 1}})
                               (catch Exception e e))
              db-too-high (try @(fluree/stage2
                                  db
                                  {"@context" "https://ns.flur.ee"
                                   "insert"
                                   {:id         :ex/john
                                    :type       :ex/User
                                    :schema/age 100}})
                               (catch Exception e e))]
          (is (util/exception? db-too-low)
              "Exception, because :schema/age is below the minimum")
          (is (= "SHACL PropertyShape exception - sh:minExclusive: value 1 is either non-numeric or lower than exclusive minimum of 1."
                 (ex-message db-too-low)))

          (is (util/exception? db-too-high)
              "Exception, because :schema/age is above the maximum")
          (is (= "SHACL PropertyShape exception - sh:maxExclusive: value 100 is either non-numeric or higher than exclusive maximum of 100."
                 (ex-message db-too-high)))

          (is (= @(fluree/query db-ok user-query)
                 [{:id         :ex/john
                   :type   :ex/User
                   :schema/age 2}]))))
      (testing "inclusive constraints"
        (let [db          @(fluree/stage2
                             (fluree/db ledger)
                             {"@context" "https://ns.flur.ee"
                              "insert"
                              {:id             :ex/InclusiveNumRangeShape
                               :type           :sh/NodeShape
                               :sh/targetClass :ex/User
                               :sh/property    [{:sh/path         :schema/age
                                                 :sh/minInclusive 1
                                                 :sh/maxInclusive 100}]}})
              db-ok       @(fluree/stage2
                             db
                             {"@context" "https://ns.flur.ee"
                              "insert"
                              {:id         :ex/brian
                               :type       :ex/User
                               :schema/age 1}})
              db-ok2      @(fluree/stage2
                             db-ok
                             {"@context" "https://ns.flur.ee"
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 100}})
              db-too-low  @(fluree/stage2
                             db
                             {"@context" "https://ns.flur.ee"
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 0}})
              db-too-high @(fluree/stage2
                             db
                             {"@context" "https://ns.flur.ee"
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 101}})]
          (is (util/exception? db-too-low)
              "Exception, because :schema/age is below the minimum")
          (is (= "SHACL PropertyShape exception - sh:minInclusive: value 0 is either non-numeric or lower than minimum of 1."
                 (ex-message db-too-low)))

          (is (util/exception? db-too-high)
              "Exception, because :schema/age is above the maximum")
          (is (= "SHACL PropertyShape exception - sh:maxInclusive: value 101 is either non-numeric or higher than maximum of 100."
                 (ex-message db-too-high)))
          (is (= @(fluree/query db-ok2 user-query)
                 [{:id         :ex/alice
                   :type   :ex/User
                   :schema/age 100}
                  {:id         :ex/brian
                   :type   :ex/User
                   :schema/age 1}]))))
      (testing "non-numeric values"
        (let [db         @(fluree/stage2
                            (fluree/db ledger)
                            {"@context" "https://ns.flur.ee"
                             "insert"
                             {:id             :ex/NumRangeShape
                              :type           :sh/NodeShape
                              :sh/targetClass :ex/User
                              :sh/property    [{:sh/path         :schema/age
                                                :sh/minExclusive 0}]}})
              db-subj-id (try @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id         :ex/alice
                                   :type       :ex/User
                                   :schema/age :ex/brian}})
                              (catch Exception e e))
              db-string  (try @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id         :ex/alice
                                   :type       :ex/User
                                   :schema/age "10"}})
                              (catch Exception e e))]
          (is (util/exception? db-subj-id)
              "Exception, because :schema/age is not a number")
          (is (= "SHACL PropertyShape exception - sh:minExclusive: value 10 is either non-numeric or lower than exclusive minimum of 0."
                 (ex-message db-string)))

          (is (util/exception? db-string)
              "Exception, because :schema/age is not a number")
          (is (= "SHACL PropertyShape exception - sh:minExclusive: value 10 is either non-numeric or lower than exclusive minimum of 0."
                 (ex-message db-string))))))))

(deftest ^:integration shacl-string-length-constraints
  (testing "shacl string length constraint errors"
    (let [conn                (test-utils/create-conn)
          ledger              @(fluree/create conn "shacl/str"
                                              {:defaultContext
                                               ["" {:ex "http://example.org/ns/"}]})
          user-query          {:select {'?s [:*]}
                               :where  [['?s :type :ex/User]]}
          db                  @(fluree/stage2
                                 (fluree/db ledger)
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id             :ex/UserShape
                                   :type           :sh/NodeShape
                                   :sh/targetClass :ex/User
                                   :sh/property    [{:sh/path      :schema/name
                                                     :sh/minLength 4
                                                     :sh/maxLength 10}]}})
          db-ok-str           @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id          :ex/john
                                   :type        :ex/User
                                   :schema/name "John"}})

          db-ok-non-str       @(fluree/stage2
                                 db
                                 {"@context" "https://ns.flur.ee"
                                  "insert"
                                  {:id          :ex/john
                                   :type        :ex/User
                                   :schema/name 12345}})

          db-too-short-str    (try
                                @(fluree/stage2
                                   db
                                   {"@context" "https://ns.flur.ee"
                                    "insert"
                                    {:id          :ex/al
                                     :type        :ex/User
                                     :schema/name "Al"}})
                                (catch Exception e e))
          db-too-long-str     (try
                                @(fluree/stage2
                                   db
                                   {"@context" "https://ns.flur.ee"
                                    "insert"
                                    {:id          :ex/jean-claude
                                     :type        :ex/User
                                     :schema/name "Jean-Claude"}})
                                (catch Exception e e))
          db-too-long-non-str (try
                                @(fluree/stage2
                                   db
                                   {"@context" "https://ns.flur.ee"
                                    "insert"
                                    {:id          :ex/john
                                     :type        :ex/User
                                     :schema/name 12345678910}})
                                (catch Exception e e))
          db-ref-value        (try
                                @(fluree/stage2
                                   db
                                   {"@context" "https://ns.flur.ee"
                                    "insert"
                                    {:id          :ex/john
                                     :type        :ex/User
                                     :schema/name :ex/ref}})
                                (catch Exception e e))]
      (is (util/exception? db-too-short-str)
          "Exception, because :schema/name is shorter than minimum string length")
      (is (= "SHACL PropertyShape exception - sh:minLength: value Al has string length smaller than minimum: 4 or it is not a literal value."
             (ex-message db-too-short-str)))
      (is (util/exception? db-too-long-str)
          "Exception, because :schema/name is longer than maximum string length")
      (is (= "SHACL PropertyShape exception - sh:maxLength: value Jean-Claude has string length larger than 10 or it is not a literal value."
             (ex-message db-too-long-str)))
      (is (util/exception? db-too-long-non-str)
          "Exception, because :schema/name is longer than maximum string length")
      (is (= "SHACL PropertyShape exception - sh:maxLength: value 12345678910 has string length larger than 10 or it is not a literal value."
             (ex-message db-too-long-non-str)))
      (is (util/exception? db-ref-value)
          "Exception, because :schema/name is not a literal value")
      (is (str/starts-with? (ex-message db-ref-value)
                            "SHACL PropertyShape exception - sh:maxLength: value "))
      (is (= [{:id          :ex/john
               :type    :ex/User
               :schema/name "John"}]
             @(fluree/query db-ok-str user-query)))
      (is (= [{:id          :ex/john
               :type        :ex/User
               :schema/name 12345}]
             @(fluree/query db-ok-non-str user-query))))))

(deftest ^:integration shacl-string-pattern-constraints
  (testing "shacl string regex constraint errors"
    (let [conn                   (test-utils/create-conn)
          ledger                 @(fluree/create conn "shacl/str"
                                                 {:defaultContext
                                                  ["" {:ex "http://example.org/ns/"}]})
          user-query             {:select {'?s [:*]}
                                  :where  [['?s :type :ex/User]]}
          db                     @(fluree/stage2
                                    (fluree/db ledger)
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id             :ex/UserShape
                                      :type           [:sh/NodeShape]
                                      :sh/targetClass :ex/User
                                      :sh/property    [{:sh/path    :ex/greeting
                                                        :sh/pattern "hello   (.*?)world"
                                                        :sh/flags   ["x" "s"]}
                                                       {:sh/path    :ex/birthYear
                                                        :sh/pattern "(19|20)[0-9][0-9]"}]}})
          db-ok-greeting         @(fluree/stage2
                                    db
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id          :ex/brian
                                      :type        :ex/User
                                      :ex/greeting "hello\nworld!"}})

          db-ok-birthyear        @(fluree/stage2
                                    db
                                    {"@context" "https://ns.flur.ee"
                                     "insert"
                                     {:id           :ex/john
                                      :type         :ex/User
                                      :ex/birthYear 1984}})
          db-wrong-case-greeting (try
                                   @(fluree/stage2
                                      db
                                      {"@context" "https://ns.flur.ee"
                                       "insert"
                                       {:id          :ex/alice
                                        :type        :ex/User
                                        :ex/greeting "HELLO\nWORLD!"}})
                                   (catch Exception e e))
          db-wrong-birth-year    (try
                                   @(fluree/stage2
                                      db
                                      {"@context" "https://ns.flur.ee"
                                       "insert"
                                       {:id           :ex/alice
                                        :type         :ex/User
                                        :ex/birthYear 1776}})
                                   (catch Exception e e))
          db-ref-value           (try
                                   @(fluree/stage2
                                      db
                                      {"@context" "https://ns.flur.ee"
                                       "insert"
                                       {:id           :ex/john
                                        :type         :ex/User
                                        :ex/birthYear :ex/ref}})
                                   (catch Exception e e))]
      (is (util/exception? db-wrong-case-greeting)
          "Exception, because :ex/greeting does not match pattern")
      (is (= "SHACL PropertyShape exception - sh:pattern: value HELLO
WORLD! does not match pattern \"hello   (.*?)world\" with provided sh:flags: [\"s\" \"x\"] or it is not a literal value."
             (ex-message db-wrong-case-greeting)))
      (is (= "SHACL PropertyShape exception - sh:pattern: value HELLO
WORLD! does not match pattern \"hello   (.*?)world\" with provided sh:flags: [\"s\" \"x\"] or it is not a literal value."
             (ex-message db-wrong-case-greeting)))
      (is (util/exception? db-wrong-birth-year)
          "Exception, because :ex/birthYear does not match pattern")
      (is (= "SHACL PropertyShape exception - sh:pattern: value 1776 does not match pattern \"(19|20)[0-9][0-9]\" or it is not a literal value."
             (ex-message db-wrong-birth-year)))
      (is (util/exception? db-ref-value)
          "Exception, because :schema/name is not a literal value")
      (is (str/starts-with? (ex-message db-ref-value)
                            "SHACL PropertyShape exception - sh:pattern: value "))
      (is (= [{:id          :ex/brian
               :type    :ex/User
               :ex/greeting "hello\nworld!"}]
             @(fluree/query db-ok-greeting user-query)))
      (is (= [{:id           :ex/john
               :type     :ex/User
               :ex/birthYear 1984}]
             @(fluree/query db-ok-birthyear user-query))))))

(deftest ^:integration shacl-multiple-properties-test
  (testing "multiple properties works"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :type :ex/User]]}
          db           @(fluree/stage2
                          (fluree/db ledger)
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id             :ex/UserShape
                            :type           :sh/NodeShape
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path     :schema/name
                                              :sh/datatype :xsd/string
                                              :sh/minCount 1
                                              :sh/maxCount 1}
                                             {:sh/path         :schema/age
                                              :sh/minCount     1
                                              :sh/maxCount     1
                                              :sh/minInclusive 0
                                              :sh/maxInclusive 130}
                                             {:sh/path     :schema/email
                                              :sh/datatype :xsd/string}]}})
          db-ok        @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/name  "John"
                            :schema/age   40
                            :schema/email "john@example.org"}})
          db-no-name   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/age   40
                            :schema/email "john@example.org"}})
          db-two-names @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/name  ["John" "Billy"]
                            :schema/age   40
                            :schema/email "john@example.org"}})
          db-too-old   @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/name  "John"
                            :schema/age   140
                            :schema/email "john@example.org"}})
          db-two-ages  @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/name  "John"
                            :schema/age   [40 21]
                            :schema/email "john@example.org"}})
          db-num-email @(fluree/stage2
                          db
                          {"@context" "https://ns.flur.ee"
                           "insert"
                           {:id           :ex/john
                            :type         :ex/User
                            :schema/name  "John"
                            :schema/age   40
                            :schema/email 42}})]
      (is (util/exception? db-no-name))
      (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
             (ex-message db-no-name)))
      (is (util/exception? db-two-names))
      (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
             (ex-message db-two-names)))
      (is (util/exception? db-too-old))
      (is (= "SHACL PropertyShape exception - sh:maxInclusive: value 140 is either non-numeric or higher than maximum of 130."
             (ex-message db-too-old)))
      (is (util/exception? db-two-ages))
      (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
             (ex-message db-two-ages)))
      (is (util/exception? db-num-email))
      (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
             (ex-message db-num-email)))
      (is (= [{:id           :ex/john
               :type     :ex/User
               :schema/age   40
               :schema/email "john@example.org"
               :schema/name  "John"}]
             @(fluree/query db-ok user-query))))))

(deftest ^:integration property-paths
  (let [conn   @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "propertypathstest" {:defaultContext [test-utils/default-str-context {"ex" "http://example.com/"}]})
        db0    (fluree/db ledger)]
    (testing "inverse path"
      (let [;; a valid Parent is anybody who is the object of a parent predicate
            db1          @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                              "insert" {"@type"          "sh:NodeShape"
                                                        "id"             "ex:ParentShape"
                                                        "sh:targetClass" {"@id" "ex:Parent"}
                                                        "sh:property"    [{"sh:path"     {"sh:inversePath" {"id" "ex:parent"}}
                                                                           "sh:minCount" 1}]}})
            valid-parent @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                              "insert" {"id"          "ex:Luke"
                                                        "schema:name" "Luke"
                                                        "ex:parent"   {"id"          "ex:Anakin"
                                                                       "type"        "ex:Parent"
                                                                       "schema:name" "Anakin"}}})
            invalid-pal  @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                              "insert" {"id"          "ex:bad-parent"
                                                        "type"        "ex:Parent"
                                                        "schema:name" "Darth Vader"}})]
        (is (= [{"id"          "ex:Luke",
                 "schema:name" "Luke",
                 "ex:parent"   {"id"          "ex:Anakin"
                                "type" "ex:Parent"
                                "schema:name" "Anakin"}}]
               @(fluree/query valid-parent {"select" {"?s" ["*" {"ex:parent" ["*"]}]}
                                            "where"  [["?s" "id" "ex:Luke"]]})))

        (is (util/exception? invalid-pal))

        (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
               (ex-message invalid-pal)))))
    (testing "sequence paths"
      (let [;; a valid Pal is anybody who has a pal with a name
            db1         @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                             "insert" {"@type"          "sh:NodeShape"
                                                       ;; "sh:targetNode" {"@id" "ex:good-pal"}
                                                       "sh:targetClass" {"@id" "ex:Pal"}
                                                       "sh:property"    [{"sh:path"     {"@list" [{"id" "ex:pal"} {"id" "schema:name"}]}
                                                                          "sh:minCount" 1}]}})
            valid-pal   @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                             "insert" {"id"          "ex:good-pal"
                                                       "type"        "ex:Pal"
                                                       "schema:name" "J.D."
                                                       "ex:pal"      [{"schema:name" "Turk"}
                                                                      {"schema:name" "Rowdy"}]}})
            invalid-pal @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                             "insert" {"id"          "ex:bad-pal"
                                                       "type"        "ex:Pal"
                                                       "schema:name" "Darth Vader"
                                                       "ex:pal"      {"ex:evil" "has no name"}}})]
        (is (= [{"id"          "ex:good-pal",
                 "type" "ex:Pal"
                 "schema:name" "J.D.",
                 "ex:pal"      [{"schema:name" "Turk"}
                                {"schema:name" "Rowdy"}]}]
               @(fluree/query valid-pal {"select" {"?s" ["*" {"ex:pal" ["schema:name"]}]}
                                         "where"  [["?s" "id" "ex:good-pal"]]})))
        (is (util/exception? invalid-pal))
        (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
               (ex-message invalid-pal)))))
    (testing "inverse sequence path"
      (let [;; a valid Princess is anybody who is the child of someone's queen
            db1              @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                                  "insert" {"@type"          "sh:NodeShape"
                                                            "id"             "ex:PrincessShape"
                                                            "sh:targetClass" {"@id" "ex:Princess"}
                                                            "sh:property"    [{"sh:path"     {"@list" [{"sh:inversePath" {"id" "ex:child"}}
                                                                                                       {"sh:inversePath" {"id" "ex:queen"}}]}
                                                                               "sh:minCount" 1}]}})
            valid-princess   @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                                  "insert" {"id"          "ex:Pleb"
                                                            "schema:name" "Pleb"
                                                            "ex:queen"    {"id"          "ex:Buttercup"
                                                                           "schema:name" "Buttercup"
                                                                           "ex:child"    {"id"          "ex:Mork"
                                                                                          "type"        "ex:Princess"
                                                                                          "schema:name" "Mork"}}}})
            invalid-princess @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                                  "insert" {"id"          "ex:Pleb"
                                                            "schema:name" "Pleb"
                                                            "ex:child"    {"id"          "ex:Gerb"
                                                                           "type"        "ex:Princess"
                                                                           "schema:name" "Gerb"}}})]
        (is (= [{"id" "ex:Mork", "type" "ex:Princess", "schema:name" "Mork"}]
               @(fluree/query valid-princess {"select" {"?s" ["*"]}
                                              "where"  [["?s" "id" "ex:Mork"]]})))

        (is (util/exception? invalid-princess))
        (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
               (ex-message invalid-princess)))))))

(deftest ^:integration shacl-class-test
  (let [conn   @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "classtest" {:defaultContext test-utils/default-str-context})
        db0    (fluree/db ledger)
        db1    @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                    "insert" [{"@type" "sh:NodeShape"
                                               "sh:targetClass" {"@id" "https://example.com/Country"}
                                               "sh:property"
                                               [{"sh:path"     {"@id" "https://example.com/name"}
                                                 "sh:datatype" {"@id" "xsd:string"}
                                                 "sh:minCount" 1
                                                 "sh:maxCount" 1}]}
                                              {"@type" "sh:NodeShape"
                                               "sh:targetClass" {"@id" "https://example.com/Actor"}
                                               "sh:property"
                                               [{"sh:path"        {"@id" "https://example.com/country"}
                                                 "sh:class"       {"@id" "https://example.com/Country"}
                                                 "sh:maxCount"    1
                                                 "sh:description" "Birth country"}
                                                {"sh:path"     {"@id" "https://example.com/name"}
                                                 "sh:minCount" 1
                                                 "sh:maxCount" 1
                                                 "sh:datatype" {"@id" "xsd:string"}}]}]})
        ;; valid inline type
        db2    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                    "insert" {"@id"                           "https://example.com/Actor/65731"
                                              "https://example.com/country"   {"@id"                      "https://example.com/Country/AU"
                                                                               "@type"                    "https://example.com/Country"
                                                                               "https://example.com/name" "Oz"}
                                              "https://example.com/gender"    "Male"
                                              "https://example.com/character" ["Jake Sully" "Marcus Wright"]
                                              "https://example.com/movie"     [{"@id" "https://example.com/Movie/19995"}
                                                                               {"@id" "https://example.com/Movie/534"}]
                                              "@type"                         "https://example.com/Actor"
                                              "https://example.com/name"      "Sam Worthington"}})
        ;; valid node ref
        db3    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                    "insert" [{"@id"                      "https://example.com/Country/US"
                                               "@type"                    "https://example.com/Country"
                                               "https://example.com/name" "United States of America"}
                                              {"@id"                         "https://example.com/Actor/4242"
                                               "https://example.com/country" {"@id" "https://example.com/Country/US"}
                                               "https://example.com/gender"  "Female"
                                               "@type"                       "https://example.com/Actor"
                                               "https://example.com/name"    "Rindsey Rohan"}]})
        ;; invalid inline type
        db4    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                    "insert" {"@id"                         "https://example.com/Actor/1001"
                                              "https://example.com/country" {"@id"                      "https://example.com/Country/Absurdistan"
                                                                             "@type"                    "https://example.com/FakeCountry"
                                                                             "https://example.com/name" "Absurdistan"}
                                              "https://example.com/gender"  "Male"
                                              "@type"                       "https://example.com/Actor"
                                              "https://example.com/name"    "Not Real"}})
        ;; invalid node ref type
        db5    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                    "insert" [{"@id"                      "https://example.com/Country/Absurdistan"
                                               "@type"                    "https://example.com/FakeCountry"
                                               "https://example.com/name" "Absurdistan"}
                                              {"@id"                         "https://example.com/Actor/8675309"
                                               "https://example.com/country" {"@id" "https://example.com/Country/Absurdistan"}
                                               "https://example.com/gender"  "Female"
                                               "@type"                       "https://example.com/Actor"
                                               "https://example.com/name"    "Jenny Tutone"}]})]
    (is (not (util/exception? db2)))
    (is (not (util/exception? db3)))
    (is (util/exception? db4))
    (is (str/starts-with? (ex-message db4)
                          "SHACL PropertyShape exception - sh:class: class(es) "))
    (is (util/exception? db5))
    (is (str/starts-with? (ex-message db5)
                          "SHACL PropertyShape exception - sh:class: class(es) "))))

(deftest ^:integration shacl-in-test
  (testing "value nodes"
    (let [conn   @(fluree/connect {:method :memory
                                   :defaults
                                   {:context test-utils/default-str-context}})
          ledger @(fluree/create conn "shacl-in-test"
                                 {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
          db0    (fluree/db ledger)
          db1    @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                      "insert" [{"type"           ["sh:NodeShape"]
                                                 "sh:targetClass" {"id" "ex:Pony"}
                                                 "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                    "sh:in"   '("cyan" "magenta")}]}]})
          db2    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                      "insert" {"id"       "ex:YellowPony"
                                                "type"     "ex:Pony"
                                                "ex:color" "yellow"}})]
      (is (util/exception? db2))
      (is (= "SHACL PropertyShape exception - sh:in: value must be one of [\"cyan\" \"magenta\"]."
             (ex-message db2)))))
  (testing "node refs"
    (let [conn   @(fluree/connect {:method :memory
                                   :defaults
                                   {:context test-utils/default-str-context}})
          ledger @(fluree/create conn "shacl-in-test")
          db0    (fluree/db ledger)
          db1    @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                      "insert" [{"type"           ["sh:NodeShape"]
                                                 "sh:targetClass" {"id" "ex:Pony"}
                                                 "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                    "sh:in"   '({"id" "ex:Pink"}
                                                                                {"id" "ex:Purple"})}]}]})
          db2    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                      "insert" [{"id"   "ex:Pink"
                                                 "type" "ex:color"}
                                                {"id"   "ex:Purple"
                                                 "type" "ex:color"}
                                                {"id"   "ex:Green"
                                                 "type" "ex:color"}
                                                {"id"       "ex:RainbowPony"
                                                 "type"     "ex:Pony"
                                                 "ex:color" [{"id" "ex:Pink"}
                                                             {"id" "ex:Green"}]}]})
          db3    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                      "insert" [{"id"       "ex:PastelPony"
                                                 "type"     "ex:Pony"
                                                 "ex:color" [{"id" "ex:Pink"}
                                                             {"id" "ex:Purple"}]}]})]
      (is (util/exception? db2))
      (is (str/starts-with? (ex-message db2)
                            "SHACL PropertyShape exception - sh:in: value must be one of "))

      (is (not (util/exception? db3)))
      (is (= [{"id"       "ex:PastelPony"
               "type" "ex:Pony"
               "ex:color" [{"id" "ex:Pink"} {"id" "ex:Purple"}]}]
             @(fluree/query db3 '{"select" {"?p" ["*"]}
                                  "where"  [["?p" "type" "ex:Pony"]]})))))
  (testing "mixed values and refs"
    (let [conn   @(fluree/connect {:method :memory
                                   :defaults
                                   {:context test-utils/default-str-context}})
          ledger @(fluree/create conn "shacl-in-test")
          db0    (fluree/db ledger)
          db1    @(fluree/stage2 db0 {"@context" "https://ns.flur.ee"
                                      "insert" [{"type"           ["sh:NodeShape"]
                                                 "sh:targetClass" {"id" "ex:Pony"}
                                                 "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                    "sh:in"   '({"id" "ex:Pink"}
                                                                                {"id" "ex:Purple"}
                                                                                "green")}]}]})
          db2    @(fluree/stage2 db1 {"@context" "https://ns.flur.ee"
                                      "insert" {"id"       "ex:RainbowPony"
                                                "type"     "ex:Pony"
                                                "ex:color" [{"id" "ex:Pink"}
                                                            {"id" "ex:Green"}]}})]
      (is (util/exception? db2))
      (is (str/starts-with? (ex-message db2)
                            "SHACL PropertyShape exception - sh:in: value must be one of ")))))

(deftest ^:integration shacl-targetobjectsof-test
  (testing "subject and object of constrained predicate in the same txn"
    (testing "datatype constraint"
      (let [conn               @(fluree/connect {:method :memory
                                                 :defaults
                                                 {:context test-utils/default-str-context}})
            ledger             @(fluree/create conn "shacl-target-objects-of-test"
                                               {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1                @(fluree/stage (fluree/db ledger)
                                              [{"@id"                "ex:friendShape"
                                                "type"               ["sh:NodeShape"]
                                                "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                       "sh:datatype" {"@id" "xsd:string"}}]}])
            db-bad-friend-name @(fluree/stage db1
                                              [{"id"        "ex:Alice"
                                                "ex:name"   "Alice"
                                                "type"      "ex:User"
                                                "ex:friend" {"@id" "ex:Bob"}}
                                               {"id"      "ex:Bob"
                                                "ex:name" 123
                                                "type"    "ex:User"}])]
        (is (util/exception? db-bad-friend-name))
        (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
               (ex-message db-bad-friend-name)))))
    (testing "maxCount"
      (let [conn          @(fluree/connect {:method :memory
                                            :defaults
                                            {:context test-utils/default-str-context}})
            ledger        @(fluree/create conn "shacl-target-objects-of-test"
                                          {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1           @(fluree/stage (fluree/db ledger)
                                         [{"@id"                "ex:friendShape"
                                           "type"               ["sh:NodeShape"]
                                           "sh:targetObjectsOf" {"@id" "ex:friend"}
                                           "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                  "sh:maxCount" 1}]}])
            db-excess-ssn @(fluree/stage db1
                                         [{"id"        "ex:Alice"
                                           "ex:name"   "Alice"
                                           "type"      "ex:User"
                                           "ex:friend" {"@id" "ex:Bob"}}
                                          {"id"     "ex:Bob"
                                           "ex:ssn" ["111-11-1111"
                                                     "222-22-2222"]
                                           "type"   "ex:User"}])]
        (is (util/exception? db-excess-ssn))
        (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
               (ex-message db-excess-ssn)))))
    (testing "required properties"
      (let [conn          @(fluree/connect {:method :memory
                                            :defaults
                                            {:context test-utils/default-str-context}})
            ledger        @(fluree/create conn "shacl-target-objects-of-test"
                                          {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1           @(fluree/stage (fluree/db ledger)
                                         [{"@id"                "ex:friendShape"
                                           "type"               ["sh:NodeShape"]
                                           "sh:targetObjectsOf" {"@id" "ex:friend"}
                                           "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                  "sh:minCount" 1}]}])
            db-just-alice @(fluree/stage db1
                                         [{"id"        "ex:Alice"
                                           "ex:name"   "Alice"
                                           "type"      "ex:User"
                                           "ex:friend" {"@id" "ex:Bob"}}])]
        (is (util/exception? db-just-alice))
        (is (= "SHACL PropertyShape exception - sh:minCount of 1 higher than actual count of 0."
               (ex-message db-just-alice)))))
    (testing "combined with `sh:targetClass`"
      (let [conn          @(fluree/connect {:method :memory
                                            :defaults
                                            {:context test-utils/default-str-context}})
            ledger        @(fluree/create conn "shacl-target-objects-of-test"
                                          {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1           @(fluree/stage (fluree/db ledger)
                                         [{"@id"            "ex:UserShape"
                                           "type"           ["sh:NodeShape"]
                                           "sh:targetClass" {"@id" "ex:User"}
                                           "sh:property"    [{"sh:path"     {"@id" "ex:ssn"}
                                                              "sh:maxCount" 1}]}
                                          {"@id"                "ex:friendShape"
                                           "type"               ["sh:NodeShape"]
                                           "sh:targetObjectsOf" {"@id" "ex:friend"}
                                           "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                  "sh:maxCount" 1}]}])
            db-bad-friend @(fluree/stage db1 [{"id"        "ex:Alice"
                                               "ex:name"   "Alice"
                                               "type"      "ex:User"
                                               "ex:friend" {"@id" "ex:Bob"}}
                                              {"id"      "ex:Bob"
                                               "ex:name" ["Bob" "Robert"]
                                               "ex:ssn"  "111-11-1111"
                                               "type"    "ex:User"}])]
        (is (util/exception? db-bad-friend))
        (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
               (ex-message db-bad-friend))))))
  (testing "separate txns"
    (testing "maxCount"
      (let [conn                   @(fluree/connect {:method :memory
                                                     :defaults
                                                     {:context test-utils/default-str-context}})
            ledger                 @(fluree/create conn "shacl-target-objects-of-test"
                                                   {:defaultContext ["" {"ex" "http://example.com/ns/"}]})

            db1                    @(fluree/stage (fluree/db ledger)
                                                  [{"@id"                "ex:friendShape"
                                                    "type"               ["sh:NodeShape"]
                                                    "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                    "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                           "sh:maxCount" 1}]}])
            db2                    @(fluree/stage db1 [{"id"     "ex:Bob"
                                                        "ex:ssn" ["111-11-1111" "222-22-2222"]
                                                        "type"   "ex:User"}])
            db-db-forbidden-friend @(fluree/stage db2
                                                  {"id"        "ex:Alice"
                                                   "type"      "ex:User"
                                                   "ex:friend" {"@id" "ex:Bob"}})]
        (is (util/exception? db-db-forbidden-friend))
        (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
               (ex-message db-db-forbidden-friend))))
      (let [conn          @(fluree/connect {:method :memory
                                            :defaults
                                            {:context test-utils/default-str-context}})
            ledger        @(fluree/create conn "shacl-target-objects-of-test"
                                          {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1           @(fluree/stage (fluree/db ledger)
                                         [{"@id"                "ex:friendShape"
                                           "type"               ["sh:NodeShape"]
                                           "sh:targetObjectsOf" {"@id" "ex:friend"}
                                           "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                  "sh:maxCount" 1}]}])
            db2           @(fluree/stage db1
                                         [{"id"        "ex:Alice"
                                           "ex:name"   "Alice"
                                           "type"      "ex:User"
                                           "ex:friend" {"@id" "ex:Bob"}}
                                          {"id"      "ex:Bob"
                                           "ex:name" "Bob"
                                           "type"    "ex:User"}])
            db-excess-ssn @(fluree/stage db2
                                         {"id"     "ex:Bob"
                                          "ex:ssn" ["111-11-1111"
                                                    "222-22-2222"]})]
        (is (util/exception? db-excess-ssn))
        (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
               (ex-message db-excess-ssn)))))
    (testing "datatype"
      (let [conn @(fluree/connect {:method :memory
                                   :defaults
                                   {:context test-utils/default-str-context}})
            ledger @(fluree/create conn "shacl-target-objects-of-test"
                                   {:defaultContext ["" {"ex" "http://example.com/ns/"}]})
            db1 @(fluree/stage (fluree/db ledger)
                               [{"@id" "ex:friendShape"
                                 "type" ["sh:NodeShape"]
                                 "sh:targetObjectsOf" {"@id" "ex:friend"}
                                 "sh:property" [{"sh:path" {"@id" "ex:name"}
                                                 "sh:datatype" {"@id" "xsd:string"}}]}])
            db2 @(fluree/stage db1 [{"id" "ex:Bob"
                                     "ex:name" 123
                                     "type" "ex:User"}])
            db-forbidden-friend @(fluree/stage db2
                                               {"id" "ex:Alice"
                                                "type" "ex:User"
                                                "ex:friend" {"@id" "ex:Bob"}})]
        (is (util/exception? db-forbidden-friend))
        (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
               (ex-message db-forbidden-friend)))))))

(deftest ^:integration shape-based-constraints
  (testing "sh:node"
    (let [conn           @(fluree/connect {:method :memory})
          ledger         @(fluree/create conn "shape-constaints" {:defaultContext [test-utils/default-str-context
                                                                                   {"ex" "http://example.com/"}]})
          db0            (fluree/db ledger)

          db1            @(fluree/stage db0 [{"id"          "ex:AddressShape"
                                              "type"        "sh:NodeShape"
                                              "sh:property" [{"sh:path"     {"id" "ex:postalCode"}
                                                              "sh:maxCount" 1}]}
                                             {"id"             "ex:PersonShape"
                                              "type"           "sh:NodeShape"
                                              "sh:targetClass" {"id" "ex:Person"}
                                              "sh:property"    [{"sh:path"     {"id" "ex:address"}
                                                                 "sh:node"     {"id" "ex:AddressShape"}
                                                                 "sh:minCount" 1}]}])
          valid-person   @(fluree/stage db1 [{"id"         "ex:Bob"
                                              "type"       "ex:Person"
                                              "ex:address" {"ex:postalCode" "12345"}}])
          invalid-person @(fluree/stage db1 [{"id"         "ex:Reto"
                                              "type"       "ex:Person"
                                              "ex:address" {"ex:postalCode" ["12345" "45678"]}}])]
      (is (= [{"id"         "ex:Bob",
               "type" "ex:Person",
               "ex:address" {"id" "_:f211106232532997", "ex:postalCode" "12345"}}]
             @(fluree/query valid-person {"select" {"?s" ["*" {"ex:address" ["*"]}]}
                                          "where"  [["?s" "id" "ex:Bob"]]})))
      (is (util/exception? invalid-person))
      (is (= "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."
             (ex-message invalid-person)))))

  (testing "sh:qualifiedValueShape property shape"
    (let [conn        @(fluree/connect {:method :memory})
          ledger      @(fluree/create conn "shape-constaints" {:defaultContext [test-utils/default-str-context
                                                                                {"ex" "http://example.com/"}]})
          db0         (fluree/db ledger)
          db1         @(fluree/stage db0 [{"id"             "ex:KidShape"
                                           "type"           "sh:NodeShape"
                                           "sh:targetClass" {"id" "ex:Kid"}
                                           "sh:property"    [{"sh:path"                {"id" "ex:parent"}
                                                              "sh:minCount"            2
                                                              "sh:maxCount"            2
                                                              "sh:qualifiedValueShape" {"sh:path"    {"id" "ex:gender"}
                                                                                        ;; "sh:hasValue" "ex:female"
                                                                                        "sh:pattern" "female"}
                                                              "sh:qualifiedMinCount"   1}]}
                                          {"id"        "ex:Bob"
                                           "ex:gender" "male"}
                                          {"id"        "ex:Jane"
                                           "ex:gender" "female"}])
          valid-kid   @(fluree/stage db1 [{"id"        "ex:ValidKid"
                                           "type"      "ex:Kid"
                                           "ex:parent" [{"id" "ex:Bob"} {"id" "ex:Jane"}]}])
          invalid-kid @(fluree/stage db1 [{"id"        "ex:InvalidKid"
                                           "type"      "ex:Kid"
                                           "ex:parent" [{"id" "ex:Bob"}
                                                        {"id"        "ex:Zorba"
                                                         "ex:gender" "alien"}]}])]
      (is (= [{"id"        "ex:ValidKid"
               "type" "ex:Kid"
               "ex:parent" [{"id" "ex:Bob"}
                            {"id" "ex:Jane"}]}]
             @(fluree/query valid-kid {"select" {"?s" ["*"]}
                                       "where"  [["?s" "id" "ex:ValidKid"]]})))
      (is (util/exception? invalid-kid))
      (is (= "SHACL PropertyShape exception - path [[1002 :predicate]] conformed to sh:qualifiedValueShape fewer than sh:qualifiedMinCount times."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShape node shape"
    (let [conn   @(fluree/connect {:method :memory})
          ledger @(fluree/create conn "shape-constaints" {:defaultContext [test-utils/default-str-context
                                                                           {"ex" "http://example.com/"}]})
          db0    (fluree/db ledger)

          db1    @(fluree/stage db0 [{"id" "ex:KidShape"
                                      "type" "sh:NodeShape"
                                      "sh:targetClass" {"id" "ex:Kid"}
                                      "sh:property"
                                      [{"sh:path" {"id" "ex:parent"}
                                        "sh:minCount" 2
                                        "sh:maxCount" 2
                                        "sh:qualifiedValueShape" {"id" "ex:ParentShape"
                                                                  "type" "sh:NodeShape"
                                                                  "sh:targetClass" {"id" "ex:Parent"}
                                                                  "sh:property" {"sh:path" {"id" "ex:gender"}
                                                                                 "sh:pattern" "female"}}
                                        "sh:qualifiedMinCount" 1}]}
                                     {"id" "ex:Mom"
                                      "type" "ex:Parent"
                                      "ex:gender" "female"}
                                     {"id" "ex:Dad"
                                      "type" "ex:Parent"
                                      "ex:gender" "male"}])
          valid-kid @(fluree/stage db1 [{"id" "ex:ValidKid"
                                         "type" "ex:Kid"
                                         "ex:parent" [{"id" "ex:Mom"} {"id" "ex:Dad"}]}])
          invalid-kid @(fluree/stage db1 [{"id" "ex:InvalidKid"
                                           "type" "ex:Kid"
                                           "ex:parent" [{"id" "ex:Bob"}
                                                        {"id" "ex:Zorba"
                                                         "type" "ex:Parent"
                                                         "ex:gender" "alien"}]}])]

      (is (= [{"id" "ex:ValidKid"
               "type" "ex:Kid"
               "ex:parent" [{"id" "ex:Mom"}
                            {"id" "ex:Dad"}]}]
             @(fluree/query valid-kid {"select" {"?s" ["*"]}
                                       "where" [["?s" "id" "ex:ValidKid"]]})))
      (is (util/exception? invalid-kid))
      (is (= "SHACL PropertyShape exception - sh:pattern: value alien does not match pattern \"female\" or it is not a literal value."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShapesDisjoint"
    (let [conn         @(fluree/connect {:method :memory})
          ledger       @(fluree/create conn "shape-constaints" {:defaultContext [test-utils/default-str-context
                                                                                 {"ex" "http://example.com/"}]})
          db0          (fluree/db ledger)

          db1          @(fluree/stage db0 [{"id"      "ex:Digit"
                                            "ex:name" "Toe"}
                                           {"id"             "ex:HandShape"
                                            "type"           "sh:NodeShape"
                                            "sh:targetClass" {"id" "ex:Hand"}
                                            "sh:property"    [{"sh:path"     {"id" "ex:digit"}
                                                               "sh:maxCount" 5}
                                                              {"sh:path"                         {"id" "ex:digit"}
                                                               "sh:qualifiedValueShape"          {"sh:path"    {"id" "ex:name"}
                                                                                                  "sh:pattern" "Thumb"}
                                                               "sh:qualifiedMinCount"            1
                                                               "sh:qualifiedMaxCount"            1
                                                               "sh:qualifiedValueShapesDisjoint" true}
                                                              {"sh:path"                         {"id" "ex:digit"}
                                                               "sh:qualifiedValueShape"          {"sh:path"    {"id" "ex:name"}
                                                                                                  "sh:pattern" "Finger"}
                                                               "sh:qualifiedMinCount"            4
                                                               "sh:qualifiedMaxCount"            4
                                                               "sh:qualifiedValueShapesDisjoint" true}]}])

          valid-hand   @(fluree/stage db1 [{"id"       "ex:ValidHand"
                                            "type"     "ex:Hand"
                                            "ex:digit" [{"ex:name" "Thumb"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" "Finger"}]}])
          invalid-hand @(fluree/stage db1 [{"id"       "ex:InvalidHand"
                                            "type"     "ex:Hand"
                                            "ex:digit" [{"ex:name" "Thumb"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" "Finger"}
                                                        {"ex:name" ["Finger" "Thumb"]}]}])]
      (is (= [{"id"       "ex:ValidHand",
               "type" "ex:Hand",
               "ex:digit"
               [{"ex:name" "Thumb"}
                {"ex:name" "Finger"}
                {"ex:name" "Finger"}
                {"ex:name" "Finger"}
                {"ex:name" "Finger"}]}]
             @(fluree/query valid-hand {"select" {"?s" ["*" {"ex:digit" ["ex:name"]}]}
                                        "where"  [["?s" "id" "ex:ValidHand"]]})))
      (is (util/exception? invalid-hand))
      (is (= "SHACL PropertyShape exception - path [[1003 :predicate]] conformed to sh:qualifiedValueShape fewer than sh:qualifiedMinCount times."
             (ex-message invalid-hand))))))

(deftest ^:integration post-processing-validation
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "post-processing" {:defaultContext [test-utils/default-str-context
                                                                        {"ex" "http://example.com/"}]})
        db0 (fluree/db ledger)]
    (testing "shacl-objects-of-test"
      (let [db1 @(fluree/stage db0
                               [{"@id" "ex:friendShape"
                                 "type" ["sh:NodeShape"]
                                 "sh:targetObjectsOf" {"@id" "ex:friend"}
                                 "sh:property" [{"sh:path" {"@id" "ex:name"}
                                                 "sh:datatype" {"@id" "xsd:string"}}]}])
            db2 @(fluree/stage db1 [{"id" "ex:Bob"
                                     "ex:name" 123
                                     "type" "ex:User"}])
            db-forbidden-friend @(fluree/stage db2
                                               {"id" "ex:Alice"
                                                "type" "ex:User"
                                                "ex:friend" {"@id" "ex:Bob"}})]
        (is (util/exception? db-forbidden-friend))
        (is (= "SHACL PropertyShape exception - sh:datatype: every datatype must be 1."
               (ex-message db-forbidden-friend)))))

    (testing "shape constraints"
      (let [db1 @(fluree/stage db0 [{"id" "ex:CoolShape"
                                     "type" "sh:NodeShape"
                                     "sh:property" [{"sh:path" {"id" "ex:isCool"}
                                                     "sh:hasValue" true
                                                     "sh:minCount" 1}]}
                                    {"id" "ex:PersonShape"
                                     "type" "sh:NodeShape"
                                     "sh:targetClass" {"id" "ex:Person"}
                                     "sh:property" [{"sh:path" {"id" "ex:cool"}
                                                     "sh:node" {"id" "ex:CoolShape"}
                                                     "sh:minCount" 1}]}])
            valid-person @(fluree/stage db1 [{"id" "ex:Bob"
                                              "type" "ex:Person"
                                              "ex:cool" {"ex:isCool" true}}])
            invalid-person @(fluree/stage db1 [{"id" "ex:Reto"
                                                "type" "ex:Person"
                                                "ex:cool" {"ex:isCool" false}}])]
        (is (= [{"id" "ex:Bob",
                 "type" "ex:Person",
                 "ex:cool" {"id" "_:f211106232532997", "ex:isCool" true}}]
               @(fluree/query valid-person {"select" {"?s" ["*" {"ex:cool" ["*"]}]}
                                            "where" [["?s" "id" "ex:Bob"]]})))
        (is (util/exception? invalid-person))
        (is (= "SHACL PropertyShape exception - sh:hasValue: at least one value must be true."
               (ex-message invalid-person)))))
    (testing "extended path constraints"
      (let [db1 @(fluree/stage db0 [{"id" "ex:PersonShape"
                                     "type" "sh:NodeShape"
                                     "sh:targetClass" {"id" "ex:Person"}
                                     "sh:property" [{"sh:path" [{"id" "ex:cool"} {"id" "ex:dude"}]
                                                     "sh:nodeKind" {"id" "sh:BlankNode"}
                                                     "sh:minCount" 1}]}])
            valid-person @(fluree/stage db1 [{"id" "ex:Bob"
                                              "type" "ex:Person"
                                              "ex:cool" {"ex:dude" {"ex:isBlank" true}}}])
            invalid-person @(fluree/stage db1 [{"id" "ex:Reto"
                                                "type" "ex:Person"
                                                "ex:cool" {"ex:dude" {"id" "ex:Dude"
                                                                      "ex:isBlank" false}}}])]
        (is (= [{"id" "ex:Bob",
                 "type" "ex:Person",
                 "ex:cool" {"id" "_:f211106232532995",
                            "ex:dude" {"id" "_:f211106232532996", "ex:isBlank" true}}}]
               @(fluree/query valid-person {"select" {"?s" ["*" {"ex:cool" ["*" {"ex:dude" ["*"]}]}]}
                                            "where" [["?s" "id" "ex:Bob"]]})))
        (is (util/exception? invalid-person))
        (is (= "SHACL PropertyShape exception - sh:nodekind: every value must be a blank node identifier."
               (ex-message invalid-person)))))))
