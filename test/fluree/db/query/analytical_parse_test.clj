(ns fluree.db.query.analytical-parse-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.query.analytical-parse :as ap]
    [fluree.db.util.log :as log]))


(deftest analytical-parse 
 (let [conn   (test-utils/create-conn)
       ledger @(fluree/create conn "query/parse")
       db     @(fluree/stage
                ledger
                [{:context      {:ex "http://example.org/ns/"}
                  :id           :ex/brian,
                  :type         :ex/User,
                  :schema/name  "Brian"
                  :schema/email "brian@example.org"
                  :schema/age   50
                  :ex/favNums   7}
                 {:context      {:ex "http://example.org/ns/"}
                  :id           :ex/alice,
                  :type         :ex/User,
                  :schema/name  "Alice"
                  :schema/email "alice@example.org"
                  :schema/age   50
                  :ex/favColor  "Green"
                  :ex/favNums   [42, 76, 9]}
                 {:context      {:ex "http://example.org/ns/"}
                  :id           :ex/cam,
                  :type         :ex/User,
                  :schema/name  "Cam"
                  :schema/email "cam@example.org"
                  :schema/age   34
                  :ex/favNums   [5, 10]
                  :ex/friend    [:ex/brian :ex/alice]}])] 
   (let [parsed (ap/parse db {:context {:ex "http://example.org/ns/"}
                              :select  ['?age {'?f [:*]}]
                              :where   [['?s :schema/name "Cam"]
                                        ['?s :ex/friend '?f]
                                        ['?f :schema/age '?age]]})
         {:keys [select where]} parsed
         {select-spec :spec}     select
         [where1 where2 where3] where]
     (is (= :legacy (:strategy parsed)))
     (testing "in-var/out-var correspondences" 
       (is (= []
              (:in-vars where1)))
       (is (= (:out-vars where1)
              (:in-vars where2)))
       (is (= (:out-vars where2)
              (:in-vars where3)))
       (is (= (set (:out-vars where3)) 
              (set (map :variable select-spec)))))
     (testing "first where-clause"
         (is (= {:s {:variable '?s}
                 :p {:value 1015}
                 :o {:value "Cam"}} 
                (select-keys where1 [:s :p :o])))
         (is (= {:flake-in []
                 :flake-out ['?s nil nil]
                 :all {'?s :s}
                 :others []}
                (:vars where1))))
     (testing "second where-clause"
         (is (= {:s {:variable '?s :in-n 0}  
                 :p {:value 1020} 
                 :o {:variable '?f}}
                (select-keys where2 [:s :p :o])))
         (is (= {:flake-in ['?s]
                 :flake-out ['?s nil '?f]
                 :all {'?s :s '?f :o}
                 :others []}
                (:vars where2))))
     (testing "third where-clause"
         (is (= {:s {:variable '?f :in-n 0}  
                 :p {:value 1017} 
                 :o {:variable '?age}}
                (select-keys where3 [:s :p :o])))
         (is (= {:flake-in ['?f]
                 :flake-out ['?f nil '?age]
                 :all {'?s :s '?f :s '?age :o}
                 :others ['?s]}
                (:vars where3)))))))

(deftest subject->object-joins
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/parse")
        db     @(fluree/stage
                 ledger
                 [{:context      {:ex "http://example.org/ns/"}
                   :id           :ex/brian,
                   :type         :ex/User,
                   :schema/name  "Brian"
                   :schema/email "brian@example.org"
                   :schema/age   50
                   :ex/favNums   7}
                  {:context      {:ex "http://example.org/ns/"}
                   :id           :ex/alice,
                   :type         :ex/User,
                   :schema/name  "Alice"
                   :schema/email "alice@example.org"
                   :schema/age   50
                   :ex/favColor  "Green"
                   :ex/favNums   [42, 76, 9]}
                  {:context      {:ex "http://example.org/ns/"}
                   :id           :ex/cam,
                   :type         :ex/User,
                   :schema/name  "Cam"
                   :schema/email "cam@example.org"
                   :schema/age   34
                   :ex/favNums   [5, 10]
                   :ex/friend    [:ex/brian :ex/alice]}])]
    (let [parsed (ap/parse db  {:context {:ex "http://example.org/ns/"}
                                :select {'?s ["*", {:ex/friend ["*"]}]}
                                :where [['?s :ex/friend '?o]
                                        ['?o :schema/name "Alice"]]})
          {:keys [select where]} parsed
          [where1 where2] where
          {select-spec :spec} select]
      (testing "flake-x-forms are non-nil "
        (is (:flake-x-form where1))
        (is (:flake-x-form where2)))
      (testing "join-vars"
        (is (= [] (:join-vars where1)))
        (is (= ['?o] (:join-vars where2))))
      (testing "in-var/out-var correspondences" 
        (is (= []
               (:in-vars where1)))
        (is (= (:out-vars where1)
               (:in-vars where2)))
        (is (= (set (:out-vars where2)) 
               (set (map :variable select-spec))))))))
