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
          ledger    @(fluree/create conn "class/testing")
          db1       @(fluree/stage
                       (fluree/db ledger)
                       {:context            {:ex "http://example.org/ns/"}
                        :id                 :ex/MyClass,
                        :schema/description "Just a basic object not used as a class"})
          db2       @(fluree/stage
                       db1
                       {:context            {:ex "http://example.org/ns/"}
                        :id                 :ex/myClassInstance,
                        :type               [:ex/MyClass]
                        :schema/description "Now a new subject uses MyClass as a Class"})
          query-res @(fluree/query db2 '{:context {:ex "http://example.org/ns/"},
                                         :select {?s [:*]},
                                         :where [[?s :id :ex/myClassInstance]]})]
      (is (= query-res
             [{:id                 :ex/myClassInstance,
               :rdf/type           [:ex/MyClass],
               :schema/description "Now a new subject uses MyClass as a Class"}])))))


(deftest ^:integration shacl-cardinality-constraints
  (testing "shacl minimum and maximum cardinality"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/a")
          user-query   {:context {:ex "http://example.org/ns/"}
                        :select  {'?s [:*]}
                        :where   [['?s :rdf/type :ex/User]]}
          db           @(fluree/stage
                          (fluree/db ledger)
                          {:context        {:ex "http://example.org/ns/"}
                           :id             :ex/UserShape,
                           :type           [:sh/NodeShape],
                           :sh/targetClass :ex/User
                           :sh/property    [{:sh/path     :schema/name
                                             :sh/minCount 1
                                             :sh/maxCount 1
                                             :sh/datatype :xsd/string}]})
          db-ok        @(fluree/stage
                          db
                          {:context         {:ex "http://example.org/ns/"}
                           :id              :ex/john,
                           :type            [:ex/User],
                           :schema/name     "John"
                           :schema/callSign "j-rock"})
          ; no :schema/name
          db-no-names  (try
                         @(fluree/stage
                            db
                            {:context         {:ex "http://example.org/ns/"}
                             :id              :ex/john,
                             :type            [:ex/User],
                             :schema/callSign "j-rock"})
                         (catch Exception e e))
          db-two-names (try
                         @(fluree/stage
                            db
                            {:context         {:ex "http://example.org/ns/"}
                             :id              :ex/john,
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


(deftest ^:integration shacl-datatype-constraings
  (testing "shacl datatype errors"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b")
          user-query   {:context {:ex "http://example.org/ns/"}
                        :select  {'?s [:*]}
                        :where   [['?s :rdf/type :ex/User]]}
          db           @(fluree/stage
                          (fluree/db ledger)
                          {:context        {:ex "http://example.org/ns/"}
                           :id             :ex/UserShape,
                           :type           [:sh/NodeShape],
                           :sh/targetClass :ex/User
                           :sh/property    [{:sh/path     :schema/name
                                             :sh/datatype :xsd/string}]})
          db-ok        @(fluree/stage
                          db
                          {:context     {:ex "http://example.org/ns/"}
                           :id          :ex/john,
                           :type        [:ex/User],
                           :schema/name "John"})
          ; no :schema/name
          db-int-name  (try
                         @(fluree/stage
                            db
                            {:context     {:ex "http://example.org/ns/"}
                             :id          :ex/john,
                             :type        [:ex/User],
                             :schema/name 42})
                         (catch Exception e e))
          db-bool-name (try
                         @(fluree/stage
                            db
                            {:context     {:ex "http://example.org/ns/"}
                             :id          :ex/john,
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
          ledger        @(fluree/create conn "shacl/c")
          user-query    {:context {:ex "http://example.org/ns/"}
                         :select  {'?s [:*]}
                         :where   [['?s :rdf/type :ex/User]]}
          db            @(fluree/stage
                           (fluree/db ledger)
                           {:context              {:ex "http://example.org/ns/"}
                            :id                   :ex/UserShape,
                            :type                 [:sh/NodeShape],
                            :sh/targetClass       :ex/User
                            :sh/property          [{:sh/path     :schema/name
                                                    :sh/datatype :xsd/string}]
                            :sh/ignoredProperties [:rdf/type]
                            :sh/closed            true})
          db-ok         @(fluree/stage
                           db
                           {:context     {:ex "http://example.org/ns/"}
                            :id          :ex/john,
                            :type        [:ex/User],
                            :schema/name "John"})
          ; no :schema/name
          db-extra-prop (try
                          @(fluree/stage
                             db
                             {:context      {:ex "http://example.org/ns/"}
                              :id           :ex/john,
                              :type         [:ex/User],
                              :schema/name  "John"
                              :schema/email "john@flur.ee"})
                          (catch Exception e e))]
      (is (util/exception? db-extra-prop)
          "Exception, because :schema/name is an integer and not a string.")
      (is (str/starts-with? (ex-message db-extra-prop)
                            "SHACL shape is closed"))

      (is (= @(fluree/query db-ok user-query)
             [{:id          :ex/john,
               :rdf/type    [:ex/User],
               :schema/name "John"}])
          "basic rdf:type query response not correct"))))

(deftest ^:integration shacl-property-pairs
  (testing "shacl property pairs"
    (let [conn          (test-utils/create-conn)
          ledger        @(fluree/create conn "shacl/pairs")
          user-query    {:context {:ex "http://example.org/ns/"}
                         :select  {'?s [:*]}
                         :where   [['?s :rdf/type :ex/User]]} ]
      (testing "single-cardinality equals"
        (let [db            @(fluree/stage
                              (fluree/db ledger)
                              {:context              {:ex "http://example.org/ns/"}
                               :id                   :ex/EqualNames
                               :type                 [:sh/NodeShape],
                               :sh/targetClass       :ex/User
                               :sh/property          [{:sh/path     :schema/name
                                                       :sh/equals   :ex/firstName}]})
              db-ok         @(fluree/stage
                              db
                              {:context     {:ex "http://example.org/ns/"}
                               :id          :ex/alice,
                               :type        [:ex/User],
                               :schema/name "Alice"
                               :ex/firstName "Alice"})

              db-not-equal (try
                             @(fluree/stage
                               db
                               {:context      {:ex "http://example.org/ns/"}
                                :id           :ex/john,
                                :type         [:ex/User],
                                :schema/name  "John"
                                :ex/firstName "Jack"})
                             (catch Exception e e))]
          (is (util/exception? db-not-equal)
              "Exception, because :schema/name does not equal :ex/firstName")
          (is (str/starts-with? (ex-message db-not-equal)
                                "SHACL PropertyShape exception - sh:equals"))

          (is (= [{:id          :ex/alice,
                   :rdf/type    [:ex/User],
                   :schema/name "Alice"
                   :ex/firstName "Alice"}]
                 @(fluree/query db-ok user-query))
              "basic rdf:type query response not correct")))
      (testing "multi-cardinality equals"
          (let [db            @(fluree/stage
                                (fluree/db ledger)
                                {:context              {:ex "http://example.org/ns/"}
                                 :id                   :ex/EqualNames
                                 :type                 [:sh/NodeShape],
                                 :sh/targetClass       :ex/User
                                 :sh/property          [{:sh/path     :ex/favNums
                                                         :sh/equals   :ex/luckyNums}]})
                db-ok         @(fluree/stage
                                db
                                {:context     {:ex "http://example.org/ns/"}
                                 :id          :ex/alice,
                                 :type        [:ex/User],
                                 :schema/name "Alice"
                                 :ex/favNums   [11 17]
                                 :ex/luckyNums [11 17]})

                db-not-equal1 (try
                                @(fluree/stage
                                  db
                                  {:context     {:ex "http://example.org/ns/"}
                                   :id          :ex/brian
                                   :type        [:ex/User],
                                   :schema/name "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [13 18]})
                                (catch Exception e e))
                db-not-equal2 (try
                                @(fluree/stage
                                  db
                                  {:context     {:ex "http://example.org/ns/"}
                                   :id          :ex/brian
                                   :type        [:ex/User],
                                   :schema/name "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [11]})
                                (catch Exception e e))
                db-not-equal3 (try
                                @(fluree/stage
                                  db
                                  {:context     {:ex "http://example.org/ns/"}
                                   :id          :ex/brian
                                   :type        [:ex/User],
                                   :schema/name "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [11 17 18]})
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
            (is (= [{:id          :ex/alice,
                     :rdf/type        [:ex/User],
                     :schema/name "Alice"
                     :ex/favNums   [11 17]
                     :ex/luckyNums [11 17]}]
                   @(fluree/query db-ok user-query))
                "basic rdf:type query response not correct"))))))
