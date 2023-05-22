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
          db1       @(fluree/stage
                      (fluree/db ledger)
                      {:id                 :ex/MyClass,
                       :schema/description "Just a basic object not used as a class"})
          db2       @(fluree/stage
                      db1
                      {:id                 :ex/myClassInstance,
                       :type               [:ex/MyClass]
                       :schema/description "Now a new subject uses MyClass as a Class"})
          query-res @(fluree/query db2 '{:select {?s [:*]},
                                         :where  [[?s :id :ex/myClassInstance]]})]
      (is (= query-res
             [{:id                 :ex/myClassInstance,
               :rdf/type           [:ex/MyClass],
               :schema/description "Now a new subject uses MyClass as a Class"}])))))


(deftest ^:integration shacl-cardinality-constraints
  (testing "shacl minimum and maximum cardinality"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/a" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :rdf/type :ex/User]]}
          db           @(fluree/stage
                         (fluree/db ledger)
                         {:id             :ex/UserShape,
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :schema/name
                                            :sh/minCount 1
                                            :sh/maxCount 1
                                            :sh/datatype :xsd/string}]})
          db-ok        @(fluree/stage
                         db
                         {:id              :ex/john,
                          :type            [:ex/User],
                          :schema/name     "John"
                          :schema/callSign "j-rock"})
          ; no :schema/name
          db-no-names  (try
                         @(fluree/stage
                           db
                           {:id              :ex/john,
                            :type            [:ex/User],
                            :schema/callSign "j-rock"})
                         (catch Exception e e))
          db-two-names (try
                         @(fluree/stage
                           db
                           {:id              :ex/john,
                            :type            [:ex/User],
                            :schema/name     ["John", "Johnny"]
                            :schema/callSign "j-rock"})
                         (catch Exception e e))]
      (is (util/exception? db-no-names)
          "Exception, because :schema/name requires at least 1 value.")
      (is (str/starts-with? (ex-message db-no-names)
                            "Required properties not present:"))
      (is (util/exception? db-two-names)
          "Exception, because :schema/name can have at most 1 value.")
      (is (= (ex-message db-two-names)
             "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2."))
      (is (= @(fluree/query db-ok user-query)
             [{:id              :ex/john,
               :rdf/type        [:ex/User],
               :schema/name     "John",
               :schema/callSign "j-rock"}])
          "basic rdf:type query response not correct"))))


(deftest ^:integration shacl-datatype-constraints
  (testing "shacl datatype errors"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :rdf/type :ex/User]]}
          db           @(fluree/stage
                         (fluree/db ledger)
                         {:id             :ex/UserShape,
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :schema/name
                                            :sh/datatype :xsd/string}]})
          db-ok        @(fluree/stage
                         db
                         {:id          :ex/john,
                          :type        [:ex/User],
                          :schema/name "John"})
          ; no :schema/name
          db-int-name  (try
                         @(fluree/stage
                           db
                           {:id          :ex/john,
                            :type        [:ex/User],
                            :schema/name 42})
                         (catch Exception e e))
          db-bool-name (try
                         @(fluree/stage
                           db
                           {:id          :ex/john,
                            :type        [:ex/User],
                            :schema/name true})
                         (catch Exception e e))]
      (is (util/exception? db-int-name)
          "Exception, because :schema/name is an integer and not a string.")
      (is (str/starts-with? (ex-message db-int-name)
                            "Required data type"))
      (is (util/exception? db-bool-name)
          "Exception, because :schema/name is a boolean and not a string.")
      (is (str/starts-with? (ex-message db-bool-name)
                            "Required data type"))
      (is (= @(fluree/query db-ok user-query)
             [{:id          :ex/john,
               :rdf/type    [:ex/User],
               :schema/name "John"}])
          "basic rdf:type query response not correct"))))


(deftest ^:integration shacl-closed-shape
  (testing "shacl closed shape"
    (let [conn          (test-utils/create-conn)
          ledger        @(fluree/create conn "shacl/c" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query    {:select {'?s [:*]}
                         :where  [['?s :rdf/type :ex/User]]}
          db            @(fluree/stage
                          (fluree/db ledger)
                          {:id                   :ex/UserShape,
                           :type                 [:sh/NodeShape],
                           :sh/targetClass       :ex/User
                           :sh/property          [{:sh/path     :schema/name
                                                   :sh/datatype :xsd/string}]
                           :sh/ignoredProperties [:rdf/type]
                           :sh/closed            true})
          db-ok         @(fluree/stage
                          db
                          {:id          :ex/john,
                           :type        [:ex/User],
                           :schema/name "John"})
          ; no :schema/name
          db-extra-prop (try
                          @(fluree/stage
                            db
                            {:id           :ex/john,
                             :type         [:ex/User],
                             :schema/name  "John"
                             :schema/email "john@flur.ee"})
                          (catch Exception e e))]
      (is (util/exception? db-extra-prop))
      (is (str/starts-with? (ex-message db-extra-prop)
                            "SHACL shape is closed"))

      (is (= @(fluree/query db-ok user-query)
             [{:id          :ex/john,
               :rdf/type    [:ex/User],
               :schema/name "John"}])
          "basic rdf:type query response not correct"))))

(deftest ^:integration shacl-property-pairs
  (testing "shacl property pairs"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/pairs" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query {:select {'?s [:*]}
                      :where  [['?s :rdf/type :ex/User]]}]
      (testing "single-cardinality equals"
        (let [db           @(fluree/stage
                             (fluree/db ledger)
                             {:id             :ex/EqualNamesShape
                              :type           [:sh/NodeShape],
                              :sh/targetClass :ex/User
                              :sh/property    [{:sh/path   :schema/name
                                                :sh/equals :ex/firstName}]})
              db-ok        @(fluree/stage
                             db
                             {:id           :ex/alice,
                              :type         [:ex/User],
                              :schema/name  "Alice"
                              :ex/firstName "Alice"})

              db-not-equal (try
                             @(fluree/stage
                               db
                               {:id           :ex/john,
                                :type         [:ex/User],
                                :schema/name  "John"
                                :ex/firstName "Jack"})
                             (catch Exception e e))]
          (is (util/exception? db-not-equal)
              "Exception, because :schema/name does not equal :ex/firstName")
          (is (str/starts-with? (ex-message db-not-equal)
                                "SHACL PropertyShape exception - sh:equals"))

          (is (= [{:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice"
                   :ex/firstName "Alice"}]
                 @(fluree/query db-ok user-query)))))
      (testing "multi-cardinality equals"
        (let [db            @(fluree/stage
                              (fluree/db ledger)
                              {:id             :ex/EqualNamesShape
                               :type           [:sh/NodeShape],
                               :sh/targetClass :ex/User
                               :sh/property    [{:sh/path   :ex/favNums
                                                 :sh/equals :ex/luckyNums}]})
              db-ok         @(fluree/stage
                              db
                              {:id           :ex/alice,
                               :type         [:ex/User],
                               :schema/name  "Alice"
                               :ex/favNums   [11 17]
                               :ex/luckyNums [11 17]})

              db-ok2        @(fluree/stage
                              db
                              {:id           :ex/alice,
                               :type         [:ex/User],
                               :schema/name  "Alice"
                               :ex/favNums   [11 17]
                               :ex/luckyNums [17 11]})

              db-not-equal1 (try
                              @(fluree/stage
                                db
                                {:id           :ex/brian
                                 :type         [:ex/User],
                                 :schema/name  "Brian"
                                 :ex/favNums   [11 17]
                                 :ex/luckyNums [13 18]})
                              (catch Exception e e))
              db-not-equal2 (try
                              @(fluree/stage
                                db
                                {:id           :ex/brian
                                 :type         [:ex/User],
                                 :schema/name  "Brian"
                                 :ex/favNums   [11 17]
                                 :ex/luckyNums [11]})
                              (catch Exception e e))
              db-not-equal3 (try
                              @(fluree/stage
                                db
                                {:id           :ex/brian
                                 :type         [:ex/User],
                                 :schema/name  "Brian"
                                 :ex/favNums   [11 17]
                                 :ex/luckyNums [11 17 18]})
                              (catch Exception e e))
              db-not-equal4 (try
                              @(fluree/stage
                                db
                                {:id           :ex/brian
                                 :type         [:ex/User],
                                 :schema/name  "Brian"
                                 :ex/favNums   [11 17]
                                 :ex/luckyNums ["11" "17"]})
                              (catch Exception e e))]
          (is (util/exception? db-not-equal1)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-equal1)
                                "SHACL PropertyShape exception - sh:equals"))
          (is (util/exception? db-not-equal2)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-equal2)
                                "SHACL PropertyShape exception - sh:equals"))
          (is (util/exception? db-not-equal3)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-equal3)
                                "SHACL PropertyShape exception - sh:equals"))
          (is (util/exception? db-not-equal4)
              "Exception, because :ex/favNums does not equal :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-equal4)
                                "SHACL PropertyShape exception - sh:equals"))
          (is (= [{:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums [11 17]}]
                 @(fluree/query db-ok user-query)))
          (is (= [{:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums [11 17]}]
                 @(fluree/query db-ok2 user-query)))))
      (testing "disjoint"
        (let [db               @(fluree/stage
                                 (fluree/db ledger)
                                 {:id             :ex/DisjointShape
                                  :type           [:sh/NodeShape],
                                  :sh/targetClass :ex/User
                                  :sh/property    [{:sh/path     :ex/favNums
                                                    :sh/disjoint :ex/luckyNums}]})
              db-ok            @(fluree/stage
                                 db
                                 {:id           :ex/alice,
                                  :type         [:ex/User],
                                  :schema/name  "Alice"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums 1})

              db-not-disjoint1 (try
                                 @(fluree/stage
                                   db
                                   {:id           :ex/brian
                                    :type         [:ex/User],
                                    :schema/name  "Brian"
                                    :ex/favNums   11
                                    :ex/luckyNums 11})
                                 (catch Exception e e))
              db-not-disjoint2 (try
                                 @(fluree/stage
                                   db
                                   {:id           :ex/brian
                                    :type         [:ex/User],
                                    :schema/name  "Brian"
                                    :ex/favNums   [11 17 31]
                                    :ex/luckyNums 11})
                                 (catch Exception e e))

              db-not-disjoint3 (try
                                 @(fluree/stage
                                   db
                                   {:id           :ex/brian
                                    :type         [:ex/User],
                                    :schema/name  "Brian"
                                    :ex/favNums   [11 17 31]
                                    :ex/luckyNums [13 18 11]})
                                 (catch Exception e e))]
          (is (util/exception? db-not-disjoint1)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-disjoint1)
                                "SHACL PropertyShape exception - sh:disjoint"))

          (is (util/exception? db-not-disjoint2)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-disjoint2)
                                "SHACL PropertyShape exception - sh:disjoint"))


          (is (util/exception? db-not-disjoint3)
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (str/starts-with? (ex-message db-not-disjoint3)
                                "SHACL PropertyShape exception - sh:disjoint"))

          (is (= [{:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums 1}]
                 @(fluree/query db-ok user-query)))))
      (testing "lessThan"
        (let [db       @(fluree/stage
                         (fluree/db ledger)
                         {:id             :ex/LessThanShape
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :ex/p1
                                            :sh/lessThan :ex/p2}]})
              db-ok1   @(fluree/stage
                         db
                         {:id          :ex/alice,
                          :type        [:ex/User],
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [18 19]})


              db-ok2   @(fluree/stage
                         db
                         {:id          :ex/alice,
                          :type        [:ex/User],
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [18]})

              db-fail1 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       17})
                         (catch Exception e e))

              db-fail2 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       ["18" "19"]})
                         (catch Exception e e))


              db-fail3 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [12 17]
                            :ex/p2       [10 18]})
                         (catch Exception e e))

              db-fail4 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       [12 16]})
                         (catch Exception e e))
              db-iris  (try @(fluree/stage
                              db
                              {:id          :ex/alice,
                               :type        [:ex/User],
                               :schema/name "Alice"
                               :ex/p1       :ex/brian
                               :ex/p2       :ex/john})
                            (catch Exception e e))]
          (is (util/exception? db-fail1)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (str/starts-with? (ex-message db-fail1)
                                "SHACL PropertyShape exception - sh:lessThan"))


          (is (util/exception? db-fail2)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (str/starts-with? (ex-message db-fail2)
                                "SHACL PropertyShape exception - sh:lessThan"))

          (is (util/exception? db-fail3)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (str/starts-with? (ex-message db-fail3)
                                "SHACL PropertyShape exception - sh:lessThan"))

          (is (util/exception? db-fail4)
              "Exception, because :ex/p1 is not less than :ex/p2")
          (is (str/starts-with? (ex-message db-fail4)
                                "SHACL PropertyShape exception - sh:lessThan"))

          (is (util/exception? db-iris)
              "Exception, because :ex/p1 and :ex/p2 are iris, and not valid for comparison")
          (is (str/starts-with? (ex-message db-iris)
                                "SHACL PropertyShape exception - sh:lessThan"))

          (is (= [{:id          :ex/alice,
                   :rdf/type    [:ex/User],
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [18 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice,
                   :rdf/type    [:ex/User],
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       18}]
                 @(fluree/query db-ok2 user-query)))))
      (testing "lessThanOrEquals"
        (let [db       @(fluree/stage
                         (fluree/db ledger)
                         {:id             :ex/LessThanOrEqualsShape
                          :type           [:sh/NodeShape],
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path             :ex/p1
                                            :sh/lessThanOrEquals :ex/p2}]})
              db-ok1   @(fluree/stage
                         db
                         {:id          :ex/alice,
                          :type        [:ex/User],
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [17 19]})


              db-ok2   @(fluree/stage
                         db
                         {:id          :ex/alice,
                          :type        [:ex/User],
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       17})

              db-fail1 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       10})
                         (catch Exception e e))

              db-fail2 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       ["17" "19"]})
                         (catch Exception e e))

              db-fail3 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [12 17]
                            :ex/p2       [10 17]})
                         (catch Exception e e))

              db-fail4 (try
                         @(fluree/stage
                           db
                           {:id          :ex/alice,
                            :type        [:ex/User],
                            :schema/name "Alice"
                            :ex/p1       [11 17]
                            :ex/p2       [12 16]})
                         (catch Exception e e))]

          (is (util/exception? db-fail1)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (str/starts-with? (ex-message db-fail1)
                                "SHACL PropertyShape exception - sh:lessThanOrEquals"))


          (is (util/exception? db-fail2)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (str/starts-with? (ex-message db-fail2)
                                "SHACL PropertyShape exception - sh:lessThanOrEquals"))

          (is (util/exception? db-fail3)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (str/starts-with? (ex-message db-fail3)
                                "SHACL PropertyShape exception - sh:lessThanOrEquals"))

          (is (util/exception? db-fail4)
              "Exception, because :ex/p1 is not less than or equal to :ex/p2")
          (is (str/starts-with? (ex-message db-fail4)
                                "SHACL PropertyShape exception - sh:lessThanOrEquals"))
          (is (= [{:id          :ex/alice,
                   :rdf/type    [:ex/User],
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [17 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice,
                   :rdf/type    [:ex/User],
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       17}]
                 @(fluree/query db-ok2 user-query))))))))

(deftest ^:integration shacl-value-range
  (testing "shacl value range constraints"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/value-range" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query {:select {'?s [:*]}
                      :where  [['?s :rdf/type :ex/User]]}]
      (testing "exclusive constraints"
        (let [db          @(fluree/stage
                            (fluree/db ledger)
                            {:id             :ex/ExclusiveNumRangeShape
                             :type           [:sh/NodeShape],
                             :sh/targetClass :ex/User
                             :sh/property    [{:sh/path         :schema/age
                                               :sh/minExclusive 1
                                               :sh/maxExclusive 100}]})
              db-ok       @(fluree/stage
                            db
                            {:id         :ex/john,
                             :type       [:ex/User],
                             :schema/age 2})
              db-too-low  (try @(fluree/stage
                                 db
                                 {:id         :ex/john,
                                  :type       [:ex/User],
                                  :schema/age 1})
                               (catch Exception e e))
              db-too-high (try @(fluree/stage
                                 db
                                 {:id         :ex/john,
                                  :type       [:ex/User],
                                  :schema/age 100})
                               (catch Exception e e))]
          (is (util/exception? db-too-low)
              "Exception, because :schema/age is below the minimum")
          (is (str/starts-with? (ex-message db-too-low)
                                "SHACL PropertyShape exception - sh:minExclusive: value 1"))

          (is (util/exception? db-too-high)
              "Exception, because :schema/age is above the maximum")
          (is (str/starts-with? (ex-message db-too-high)
                                "SHACL PropertyShape exception - sh:maxExclusive: value 100"))

          (is (= @(fluree/query db-ok user-query)
                 [{:id         :ex/john,
                   :rdf/type   [:ex/User],
                   :schema/age 2}]))))
      (testing "inclusive constraints"
        (let [db          @(fluree/stage
                            (fluree/db ledger)
                            {:id             :ex/InclusiveNumRangeShape
                             :type           [:sh/NodeShape],
                             :sh/targetClass :ex/User
                             :sh/property    [{:sh/path         :schema/age
                                               :sh/minInclusive 1
                                               :sh/maxInclusive 100}]})
              db-ok       @(fluree/stage
                            db
                            {:id         :ex/brian
                             :type       [:ex/User],
                             :schema/age 1})
              db-ok2      @(fluree/stage
                            db-ok
                            {:id         :ex/alice,
                             :type       [:ex/User],
                             :schema/age 100})
              db-too-low  @(fluree/stage
                            db
                            {:id         :ex/alice,
                             :type       [:ex/User],
                             :schema/age 0})
              db-too-high @(fluree/stage
                            db
                            {:id         :ex/alice,
                             :type       [:ex/User],
                             :schema/age 101})]
          (is (util/exception? db-too-low)
              "Exception, because :schema/age is below the minimum")
          (is (str/starts-with? (ex-message db-too-low)
                                "SHACL PropertyShape exception - sh:minInclusive: value 0"))

          (is (util/exception? db-too-high)
              "Exception, because :schema/age is above the maximum")
          (is (str/starts-with? (ex-message db-too-high)
                                "SHACL PropertyShape exception - sh:maxInclusive: value 101"))
          (is (= @(fluree/query db-ok2 user-query)
                 [{:id         :ex/alice
                   :rdf/type   [:ex/User]
                   :schema/age 100}
                  {:id         :ex/brian
                   :rdf/type   [:ex/User]
                   :schema/age 1}]))))
      (testing "non-numeric values"
        (let [db         @(fluree/stage
                           (fluree/db ledger)
                           {:id             :ex/NumRangeShape
                            :type           [:sh/NodeShape],
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path         :schema/age
                                              :sh/minExclusive 0}]})
              db-subj-id (try @(fluree/stage
                                db
                                {:id         :ex/alice,
                                 :type       [:ex/User],
                                 :schema/age :ex/brian})
                              (catch Exception e e))
              db-string  (try @(fluree/stage
                                db
                                {:id         :ex/alice,
                                 :type       [:ex/User],
                                 :schema/age "10"})
                              (catch Exception e e))]
          (is (util/exception? db-subj-id)
              "Exception, because :schema/age is not a number")
          (is (str/starts-with? (ex-message db-string)
                                "SHACL PropertyShape exception - sh:minExclusive"))

          (is (util/exception? db-string)
              "Exception, because :schema/age is not a number")
          (is (str/starts-with? (ex-message db-string)
                                "SHACL PropertyShape exception - sh:minExclusive: value 10")))))))

(deftest shacl-multiple-properties-test
  (testing "multiple properties works"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          user-query   {:select {'?s [:*]}
                        :where  [['?s :rdf/type :ex/User]]}
          db           @(fluree/stage
                         (fluree/db ledger)
                         {:id             :ex/UserShape
                          :type           [:sh/NodeShape]
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
                                            :sh/datatype :xsd/string}]})
          db-ok        @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/name  "John"
                          :schema/age   40
                          :schema/email "john@example.org"})
          db-no-name   @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/age   40
                          :schema/email "john@example.org"})
          db-two-names @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/name  ["John" "Billy"]
                          :schema/age   40
                          :schema/email "john@example.org"})
          db-too-old   @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/name  "John"
                          :schema/age   140
                          :schema/email "john@example.org"})
          db-two-ages  @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/name  "John"
                          :schema/age   [40 21]
                          :schema/email "john@example.org"})
          db-num-email @(fluree/stage
                         db
                         {:id           :ex/john
                          :type         [:ex/User]
                          :schema/name  "John"
                          :schema/age   40
                          :schema/email 42})]
      (is (util/exception? db-no-name))
      (is (str/starts-with? (ex-message db-no-name)
                            "Required properties not present:"))
      (is (util/exception? db-two-names))
      (is (str/starts-with? (ex-message db-two-names)
                            "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2"))
      (is (util/exception? db-too-old))
      (is (str/starts-with? (ex-message db-too-old)
                            "SHACL PropertyShape exception - sh:maxInclusive: value 140 is either non-numeric or higher than maximum of 130"))
      (is (util/exception? db-two-ages))
      (is (str/starts-with? (ex-message db-two-ages)
                            "SHACL PropertyShape exception - sh:maxCount of 1 lower than actual count of 2"))
      (is (util/exception? db-num-email))
      (is (str/starts-with? (ex-message db-num-email)
                            "Required data type"))
      (is (= [{:id           :ex/john
               :rdf/type     [:ex/User]
               :schema/age   40
               :schema/email "john@example.org"
               :schema/name  "John"}]
             @(fluree/query db-ok user-query))))))

