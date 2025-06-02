(ns fluree.db.explain-test
  (:require  [clojure.test :refer [deftest is testing]]
             [fluree.db.api :as fluree]))

(defn gen-subj
  [i]
  {"@id" (str "ex:" i)
   "ex:foo" "foo"
   "ex:i" i})

(deftest explain-test
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "test-explain")
        db0    (fluree/db ledger)

        db1    @(fluree/stage db0 {"insert"
                                   (mapv (fn [i]
                                           (cond-> (gen-subj i)
                                             (odd? i) (assoc "ex:ref1"
                                                             (cond-> (gen-subj (+ 1000 i))
                                                               (#{0} (mod i 3))
                                                               (assoc "ex:ref2" (gen-subj (+ 2000 i))
                                                                      "ex:ref3" {"@id" (str "ex:" i)})))
                                             (even? i) (assoc
                                                        "ex:bar" "bar"
                                                        "ex:num" [i (inc i) (dec i)])))
                                         (range 1 100 3))})]
    (testing "query with triple and non-triple patterns"
      (is (= {[:filter "(> 50 ?num)"] {:in 96 :out 48},
              [:optional {"@id" "?s", "ex:ref1" "?ref1"}] {:in 48 :out 48},
              [:union {"@id" "?s", "ex:foo" "?str"} {"@id" "?s", "ex:bar" "?str"}] {:in 48 :out 96},
              {"@id" "?s", "ex:bar" "?str"} {:in 48 :out 48},
              {"@id" "?s", "ex:foo" "?str"} {:in 48 :out 48},
              {"@id" "?s", "ex:num" "?num"} {:in 1 :out 48},
              {"@id" "?s", "ex:ref1" "?ref1"} {:in 48 :out 0}}
             (:explain @(fluree/explain db1 {"where" [{"@id" "?s" "ex:num" "?num"}
                                                      ["optional"
                                                       {"@id" "?s" "ex:ref1" "?ref1"}]
                                                      ["union"
                                                       {"@id" "?s" "ex:foo" "?str"}
                                                       {"@id" "?s" "ex:bar" "?str"}]
                                                      ["filter" "(> 50 ?num)"]]
                                             "select" ["?s" "?ref1" "?str"]})))))))
