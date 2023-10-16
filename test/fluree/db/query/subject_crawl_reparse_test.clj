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
        ssc-q1-parsed (parse/parse-analytical-query* {:select {"?s" ["*"]}
                                                      :where  [["?s" :schema/name "Alice"]]}
                                                     context)
        ssc-q2-parsed (parse/parse-analytical-query* {:select {"?s" ["*"]}
                                                      :where  [["?s" :schema/age 50]
                                                               ["?s" :ex/favColor "Blue"]]}
                                                     context)
        not-ssc-parsed (parse/parse-analytical-query* {:select  ['?name '?age '?email]
                                                       :where   [['?s :schema/name "Cam"]
                                                                 ['?s :ex/friend '?f]
                                                                 ['?f :schema/name '?name]
                                                                 ['?f :schema/age '?age]
                                                                 ['?f :schema/email '?email]]}
                                                      context)
        order-group-parsed (parse/parse-analytical-query* {:select   ['?name '?favNums]
                                                           :where    [['?s :schema/name '?name]
                                                                      ['?s :ex/favNums '?favNums]]
                                                           :group-by '?name
                                                           :order-by '?name}
                                                          context)
        vars-query-parsed (parse/parse-analytical-query* {:select {"?s" ["*"]}
                                                          :where  [["?s" :schema/name '?name]]
                                                          :vars {'?name "Alice"}}
                                                         context)
        s+p+o-parsed (parse/parse-analytical-query {:select {"?s" [:*]}
                                                    :where  [["?s" "?p" "?o"]]}
                                                   context
                                                   db)
        s+p+o2-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                     :where [['?s :schema/age 50]
                                                             ['?s '?p '?o]]}
                                                    context
                                                    db)
        s+p+o3-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                     :where [['?s '?p '?o]
                                                             ['?s :schema/age 50]]}
                                                    context
                                                    db)
        equivalent-property-parsed (parse/parse-analytical-query {:select {'?s ["*"]}
                                                                  :where [['?s :schema/name '?name]
                                                                          ['?s :vocab1/credential '?credential]]}
                                                                 context
                                                                 db)]
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
