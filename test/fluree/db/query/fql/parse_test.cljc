(ns fluree.db.query.fql.parse-test
  (:require
   #?@(:clj  [[clojure.test :refer :all]]
       :cljs [[cljs.test :refer-macros [deftest is testing]]])
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.query.fql.parse :as parse]))

(deftest test-parse-query
  (let  [conn   (test-utils/create-conn)
         ledger @(fluree/create conn "query/parse" {:context {:ex "http://example.org/ns/"}})
         db     @(fluree/stage
                  ledger
                  [{:id           :ex/brian,
                    :type         :ex/User,
                    :schema/name  "Brian"
                    :schema/email "brian@example.org"
                    :schema/age   50
                    :ex/favNums   7}
                   {:id           :ex/alice,
                    :type         :ex/User,
                    :ex/favColor  "Green"
                    :schema/name  "Alice"
                    :schema/email "alice@example.org"
                    :schema/age   50
                    :ex/favNums   [42, 76, 9]}
                   {:id           :ex/cam,
                    :type         :ex/User,
                    :schema/name  "Cam"
                    :ex/email "cam@example.org"
                    :schema/age   34
                    :ex/favNums   [5, 10]
                    :ex/friend    [:ex/brian :ex/alice]}])]
    (testing "parse-analytical-query"
      (let [ssc {:select {"?s" ["*"]}
                 :where  [["?s" :schema/name "Alice"]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query ssc db)]
        (is (= {:var '?s
                :selection ["*"]
                :depth 0
                :spec {:depth 0 :wildcard? true}}
               ;;select is a record, turn into map for testing
               (into {} select)))
        (is (= {:fluree.db.query.exec.where/patterns	    
	        [[{:fluree.db.query.exec.where/var '?s}
	          {:fluree.db.query.exec.where/val 1003}
	          {:fluree.db.query.exec.where/val "Alice"}]],
	        :fluree.db.query.exec.where/filters {}}
               where)))
      (let [ssc-vars {:select {"?s" ["*"]}
                      :where  [["?s" :schema/name '?name]]
                      :vars {'?name "Alice"} }
            {:keys [select where vars] :as parsed} (parse/parse-analytical-query ssc-vars db)]
        (is (= {'?name	  
                {:fluree.db.query.exec.where/var '?name,
                 :fluree.db.query.exec.where/val "Alice"}}
               vars))
        (is (= {:var '?s
                :selection ["*"]
                :depth 0
                :spec {:depth 0 :wildcard? true}}
               ;;select is a record, turn into map for testing
               (into {} select)))
        (is (= {:fluree.db.query.exec.where/patterns	    
                [[{:fluree.db.query.exec.where/var '?s}
                  {:fluree.db.query.exec.where/val 1003}
                  {:fluree.db.query.exec.where/var '?name}]],
                :fluree.db.query.exec.where/filters {}}
               where)))
      (let [query  {:context {:ex "http://example.org/ns/"}
                    :select  ['?name '?age '?email]
                    :where   [['?s :schema/name "Cam"]
                              ['?s :ex/friend '?f]
                              ['?f :schema/name '?name]
                              ['?f :schema/age '?age]
                              ['?f :ex/email '?email]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query query db)]
        (is (= [{:var '?name}
                {:var '?age}
                {:var '?email}] 
               (mapv #(into {} %) select)))
        (is (= {:fluree.db.query.exec.where/patterns	  
                [[{:fluree.db.query.exec.where/var '?s}
                  {:fluree.db.query.exec.where/val 1003}
                  {:fluree.db.query.exec.where/val "Cam"}]
                 [{:fluree.db.query.exec.where/var '?s}
                  {:fluree.db.query.exec.where/val 1009}
                  {:fluree.db.query.exec.where/var '?f}]
                 [{:fluree.db.query.exec.where/var '?f}
                  {:fluree.db.query.exec.where/val 1003}
                  {:fluree.db.query.exec.where/var '?name}]
                 [{:fluree.db.query.exec.where/var '?f}
                  {:fluree.db.query.exec.where/val 1005}
                  {:fluree.db.query.exec.where/var '?age}]
                 [{:fluree.db.query.exec.where/var '?f}
                  {:fluree.db.query.exec.where/val 1008}
                  {:fluree.db.query.exec.where/var '?email}]],
                :fluree.db.query.exec.where/filters {}}
               where)))
      (testing "class, optional"
        (let [optional-q {:select ['?name '?favColor]
                          :where  [['?s :rdf/type :ex/User]
                                   ['?s :schema/name '?name]
                                   {:optional ['?s :ex/favColor '?favColor]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query optional-q db)]
          (is (= [{:var '?name} {:var '?favColor}]
                 (mapv #(into {} %) select)))
          (is (= {:fluree.db.query.exec.where/patterns	  
                  [[:class
                    [{:fluree.db.query.exec.where/var '?s}
                     {:fluree.db.query.exec.where/val 200}
                     {:fluree.db.query.exec.where/val 1002}]]
                   [{:fluree.db.query.exec.where/var '?s}
                    {:fluree.db.query.exec.where/val 1003}
                    {:fluree.db.query.exec.where/var '?name}]
                   [:optional
                    {:fluree.db.query.exec.where/patterns
                     [[{:fluree.db.query.exec.where/var '?s}
                       {:fluree.db.query.exec.where/val 1007}
                       {:fluree.db.query.exec.where/var '?favColor}]],
                     :fluree.db.query.exec.where/filters {}}]],
                  :fluree.db.query.exec.where/filters {}}
                 where))))
      (testing "class, union"
        (let [union-q {:select ['?s '?email1 '?email2]
                       :where  [['?s :rdf/type :ex/User]
                                {:union [[['?s :ex/email '?email1]]
                                         [['?s :schema/email '?email2]]]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query union-q db)]
          (is (= [{:var '?s} {:var '?email1} {:var '?email2}]
                 (mapv #(into {} %) select)))
          (is (= {:fluree.db.query.exec.where/patterns	  
                  [[:class
                    [{:fluree.db.query.exec.where/var '?s}
                     {:fluree.db.query.exec.where/val 200}
                     {:fluree.db.query.exec.where/val 1002}]]
                   [:union
                    [{:fluree.db.query.exec.where/patterns
                      [[{:fluree.db.query.exec.where/var '?s}
                        {:fluree.db.query.exec.where/val 1008}
                        {:fluree.db.query.exec.where/var '?email1}]],
                      :fluree.db.query.exec.where/filters {}}
                     {:fluree.db.query.exec.where/patterns
                      [[{:fluree.db.query.exec.where/var '?s}
                        {:fluree.db.query.exec.where/val 1004}
                        {:fluree.db.query.exec.where/var '?email2}]],
                      :fluree.db.query.exec.where/filters {}}]]],
                  :fluree.db.query.exec.where/filters {}}
                 where))))
      (testing "class, filters"
        (let [filter-q {:select ['?name '?age]
                        :where  [['?s :rdf/type :ex/User]
                                 ['?s :schema/age '?age]
                                 ['?s :schema/name '?name]
                                 {:filter ["(> ?age 45)", "(< ?age 50)"]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query filter-q db)]
          (is (= [{:var '?name} {:var '?age}]
                 (mapv #(into {} %) select)))
          (let [{:fluree.db.query.exec.where/keys [patterns filters]} where]
            (is (= [[:class
                     [{:fluree.db.query.exec.where/var '?s}
                      {:fluree.db.query.exec.where/val 200}
                      {:fluree.db.query.exec.where/val 1002}]]
                    [{:fluree.db.query.exec.where/var '?s}
                     {:fluree.db.query.exec.where/val 1005}
                     {:fluree.db.query.exec.where/var '?age}]
                    [{:fluree.db.query.exec.where/var '?s}
                     {:fluree.db.query.exec.where/val 1003}
                     {:fluree.db.query.exec.where/var '?name}]]
                   patterns))
            (is (= '?age
                   (-> filters keys first)))
            (let [filter-details (get filters '?age)
                  [f1 f2] filter-details]
              (def f filter-details)
              (is (= {:fluree.db.query.exec.where/var '?age
                      :fluree.db.query.exec.where/params ['?age]}
                     (select-keys f1 [:fluree.db.query.exec.where/var
                                      :fluree.db.query.exec.where/params])))
              (is (:fluree.db.query.exec.where/fn f1))
              (is (:fluree.db.query.exec.where/fn f2))))))
      (testing "group-by, order-by"
        (let [query {:select   ['?name '?favNums]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/favNums '?favNums]]
                     :group-by '?name
                     :order-by '?name}
              {:keys [select where group-by order-by] :as parsed} (parse/parse-analytical-query query db)]
          (is (= ['?name] 
                 group-by))
          (is (=  [['?name :asc]]
                 order-by)))))))

;;TODO fulltext, recursion
