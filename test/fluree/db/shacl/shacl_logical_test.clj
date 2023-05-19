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
                            :where  [['?s :rdf/type :ex/User]]}
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
      (is (str/starts-with? (ex-message db-company-name)
                            "SHACL PropertyShape exception - sh:not sh:minCount of 1 requires lower count but actual count was 1"))
      (is (util/exception? db-two-names))
      (is (str/starts-with? (ex-message db-two-names)
                            "SHACL PropertyShape exception - sh:not sh:minCount of 1 requires lower count but actual count was 2"))
      (is (util/exception? db-callsign-name))
      (is (str/starts-with? (ex-message db-callsign-name)
                            "SHACL PropertyShape exception - sh:not sh:equals: [\"Johnny Boy\"] is required to be not equal to [\"Johnny Boy\"]"))
      (is (= [{:id              :ex/john,
               :rdf/type        [:ex/User],
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
                        :where  [['?s :rdf/type :ex/User]]}
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
          db-too-old   (try
                         @(fluree/stage
                           db
                           {:id                 :ex/john,
                            :type               [:ex/User],
                            :schema/companyName "WrongCo"
                            :schema/callSign    "j-rock"
                            :schema/age         131})
                         (catch Exception e e))
          db-too-low   (try
                         @(fluree/stage
                           db
                           {:id                 :ex/john,
                            :type               [:ex/User],
                            :schema/companyName ["John", "Johnny"]
                            :schema/callSign    "j-rock"
                            :schema/age         27
                            :schema/favNums     [4 8 15 16 23 42]})
                         (catch Exception e e))
          db-two-probs (try
                         @(fluree/stage
                           db
                           {:id              :ex/john
                            :type            [:ex/User]
                            :schema/name     "Johnny Boy"
                            :schema/callSign "Johnny Boy"
                            :schema/age      900
                            :schema/favNums  [4 8 15 16 23 42]})
                         (catch Exception e e))
          ok-results   @(fluree/query db-ok user-query)]
      (is (util/exception? db-too-old))
      (is (str/starts-with? (ex-message db-too-old)
                            "SHACL PropertyShape exception - sh:not sh:minInclusive: value 131 must be less than 130"))
      (is (util/exception? db-too-low))
      (is (str/starts-with? (ex-message db-too-low)
                            "SHACL PropertyShape exception - sh:not sh:maxExclusive: value 42 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 23 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 16 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 15 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 8 must be greater than or equal to 9000; sh:not sh:maxExclusive: value 4 must be greater than or equal to 9000"))
      (is (util/exception? db-two-probs))
      (is (str/starts-with? (ex-message db-two-probs)
                            ;; could be either problem so just match common prefix
                            "SHACL PropertyShape exception - sh:not "))
      (is (= [{:id              :ex/john,
               :rdf/type        [:ex/User],
               :schema/name     "John",
               :schema/callSign "j-rock"
               :schema/age      42
               :schema/favNums  [9004 9008 9015 9016 9023 9042]}]
             ok-results)
          (str "unexpected query result: " (pr-str ok-results))))))
