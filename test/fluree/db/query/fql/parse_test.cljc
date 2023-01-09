(ns fluree.db.query.fql.parse-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.query.fql.parse :as parse]))

(deftest test-parse-query
  (let  [conn   (test-utils/create-conn)
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
                    :ex/favNums   [42, 76, 9]}
                   {:context      {:ex "http://example.org/ns/"}
                    :id           :ex/cam,
                    :type         :ex/User,
                    :schema/name  "Cam"
                    :schema/email "cam@example.org"
                    :schema/age   34
                    :ex/favNums   [5, 10]
                    :ex/friend    [:ex/brian :ex/alice]}])]
    (let [ssc {:select {"?s" ["*"]}
               :where  [["?s" :schema/name "Alice"]]}
          {:keys [select where] :as parsed} (parse/parse ssc db)]
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
    (let [query  {:context {:ex "http://example.org/ns/"}
                  :select  ['?name '?age '?email]
                  :where   [['?s :schema/name "Cam"]
                            ['?s :ex/friend '?f]
                            ['?f :schema/name '?name]
                            ['?f :schema/age '?age]
                            ['?f :schema/email '?email]]}
          {:keys [select where] :as parsed} (parse/parse query db)]
      (is (= [{:var '?name}
              {:var '?age}
              {:var '?email}] 
             (mapv #(into {} %) select)))
      (is (= {:fluree.db.query.exec.where/patterns	    
              [[{:fluree.db.query.exec.where/var '?s}
                {:fluree.db.query.exec.where/val 1003}
                {:fluree.db.query.exec.where/val "Cam"}]
               [{:fluree.db.query.exec.where/var '?s}
                {:fluree.db.query.exec.where/val 1007}
                {:fluree.db.query.exec.where/var '?f}]
               [{:fluree.db.query.exec.where/var '?f}
                {:fluree.db.query.exec.where/val 1003}
                {:fluree.db.query.exec.where/var '?name}]
               [{:fluree.db.query.exec.where/var '?f}
                {:fluree.db.query.exec.where/val 1005}
                {:fluree.db.query.exec.where/var '?age}]
               [{:fluree.db.query.exec.where/var '?f}
                {:fluree.db.query.exec.where/val 1004}
                {:fluree.db.query.exec.where/var '?email}]],
              :fluree.db.query.exec.where/filters {}}
             where)))))
