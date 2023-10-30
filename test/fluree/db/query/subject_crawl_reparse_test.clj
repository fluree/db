(ns fluree.db.query.subject-crawl-reparse-test
  (:require
   [clojure.test :refer :all]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.query.fql.parse :as parse]
   [fluree.db.query.subject-crawl.reparse :as reparse]
   [fluree.db.dbproto :as dbproto]))

(deftest test-reparse-as-ssc
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/parse" {:defaultContext ["" {:ex "http://example.org/ns/"
                                                                        :owl "http://www.w3.org/2002/07/owl#"
                                                                        :vocab1 "http://vocab1.example.org"
                                                                        :vocab2 "http://vocab2.example.org"}]})
        db     @(fluree/stage
                 (fluree/db ledger)
                 [{:id           :vocab1/credential
                   :type         :rdf/Property}
                  {:id           :vocab2/degree
                   :type         :rdf/Property
                   :owl/equivalentProperty :vocab1/credential}
                  {:id           :ex/brian,
                   :type         :ex/User,
                   :schema/name  "Brian"
                   :schema/email "brian@example.org"
                   :schema/age   50
                   :ex/favColor  "Green"
                   :ex/favNums   7}
                  {:id           :ex/alice,
                   :type         :ex/User,
                   :schema/name  "Alice"
                   :schema/email "alice@example.org"
                   :vocab1/credential "MS"
                   :schema/age   50
                   :ex/favColor  "Blue"
                   :ex/favNums   [42, 76, 9]}
                  {:id           :ex/cam,
                   :type         :ex/User,
                   :schema/name  "Cam"
                   :schema/email "cam@example.org"
                   :vocab2/degree "BA"
                   :schema/age   34
                   :ex/favNums   [5, 10]
                   :ex/friend    [:ex/brian :ex/alice]}])
        context  (dbproto/-context db)
        ssc-q1-parsed (parse/parse-analytical-query {:select {"?s" ["*"]}
                                                     :where  {:id "?s", :schema/name "Alice"}}
                                                     context)
        ssc-q2-parsed (parse/parse-analytical-query {:select {"?s" ["*"]}
                                                     :where  {:id "?s"
                                                              :schema/age 50
                                                              :ex/favColor "Blue"}}
                                                     context)
        not-ssc-parsed (parse/parse-analytical-query {:select  ['?name '?age '?email]
                                                      :where  {:schema/name "Cam"
                                                               :ex/friend {:schema/name '?name
                                                                           :schema/age '?age
                                                                           :schema/email '?email}
 }}
                                                      context)
        order-group-parsed (parse/parse-analytical-query {:select   ['?name '?favNums]
                                                          :where    {:schema/name '?name
                                                                     :ex/favNums '?favNums}
                                                           :group-by '?name
                                                           :order-by '?name}
                                                          context)
        vars-query-parsed (parse/parse-analytical-query {:select {"?s" ["*"]}
                                                         :where  {:id "?s", :schema/name '?name}
                                                         :values ['?name ["Alice"]]}
                                                         context)
        s+p+o-parsed (parse/parse-analytical-query {:select {"?s" [:*]}
                                                    :where  {:id "?s", "?p" "?o"}}
                                                   context)
        s+p+o2-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                     :where {:id '?s
                                                             :schema/age 50
                                                             '?p '?o}}
                                                    context)
        s+p+o3-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                     :where {:id '?s
                                                             '?p '?o
                                                             :schema/age 50}}
                                                    context)
        equivalent-property-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                                  :where {:id '?s
                                                                          :schema/name '?name
                                                                          :vocab1/credential '?credential}}
                                                                 context)]
    (testing "simple-subject-crawl?"
      (is (= true
             (reparse/simple-subject-crawl? ssc-q1-parsed db)))
      (is (= true
             (reparse/simple-subject-crawl? ssc-q2-parsed db)))
      (is (not (reparse/simple-subject-crawl? vars-query-parsed db)))
      (is (not (reparse/simple-subject-crawl? not-ssc-parsed db)))
      (is (not (reparse/simple-subject-crawl? order-group-parsed db)))
      (is (not (reparse/simple-subject-crawl? s+p+o-parsed db)))
      (is (not (reparse/simple-subject-crawl? s+p+o2-parsed db)))
      (is (not (reparse/simple-subject-crawl? s+p+o3-parsed db)))
      (is (not (reparse/simple-subject-crawl? equivalent-property-parsed db))))
    (testing "reparse"
      (let [ssc-q1-reparsed (reparse/re-parse-as-simple-subj-crawl ssc-q1-parsed db)
            {:keys [where context]} ssc-q1-reparsed
            [pattern] where
            {:keys [s p o]} pattern]
        (is (not (nil? context)))
        (is (= {:variable '?s}
               s))
        (is (number? (:value p)))
        (let [{:keys [value datatype]} o]
          (is (= "Alice"
                 value))
          (is datatype)))
      (let [ssc-q2-reparsed (reparse/re-parse-as-simple-subj-crawl ssc-q2-parsed db)
            {:keys [where context]} ssc-q2-reparsed
            [pattern _s-filter] where
            {:keys [s p o]} pattern]
        (is (not (nil? context)))
        (is (= {:variable '?s}
               s))
        (is (number? (:value p)))
        (let [{:keys [value datatype]} o]
          (is (= 50
                 value))
          (is datatype)))
      (is (nil?
           (reparse/re-parse-as-simple-subj-crawl not-ssc-parsed db))))))
