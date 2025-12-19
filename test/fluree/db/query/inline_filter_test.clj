(ns fluree.db.query.inline-filter-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as opt]))

(defn mk-filter
  "Build a :filter pattern with metadata describing vars and dependency order.
  Forms are dummies; these tests only exercise optimizer structure, not compile."
  [vars order]
  (let [f (with-meta (constantly true)
            {:forms            ['(constantly true)]
             :vars             (set vars)
             :dependency-order (vec order)})]
    (where/->pattern :filter f)))

(defn build-filter-descriptor
  [vars order]
  (-> (mk-filter vars order)
      (opt/filter-info)))

(deftest union-pushes-when-all-branches-bind
  (testing "filters push into all union branches when all vars are bound"
    (let [branch-a-binds-a-and-b [(vector (where/unmatched-var '?a)
                                          (where/unmatched-var '?p)
                                          (where/unmatched-var '?o))
                                  (vector (where/unmatched-var '?s)
                                          (where/unmatched-var '?p2)
                                          (where/unmatched-var '?b))]
          branch-b-binds-a-and-b [(vector (where/unmatched-var '?a)
                                          (where/unmatched-var '?p)
                                          (where/unmatched-var '?o2))
                                  (vector (where/unmatched-var '?s2)
                                          (where/unmatched-var '?p2)
                                          (where/unmatched-var '?b))]
          union-pattern     (where/->pattern :union [branch-a-binds-a-and-b branch-b-binds-a-and-b])
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern union-pattern [filter-descriptor] #{})
          updated-branches (where/pattern-data pattern)]
      (is (empty? remaining-filters)
          "no filters remain at top level after push")
      (is (= :union (where/pattern-type pattern))
          "pattern remains a union after push")
      (is (every? (fn [cl]
                    (some #(= :filter (where/pattern-type %)) cl))
                  updated-branches)
          "each union branch receives a pushed filter"))))

(deftest union-keeps-when-not-all-bind
  (testing "filter stays top-level when a branch is missing a bound var"
    (let [branch-binds-a-and-b   [(vector (where/unmatched-var '?a)
                                          (where/unmatched-var '?p)
                                          (where/unmatched-var '?b))]
          branch-binds-a-only    [(vector (where/unmatched-var '?a)
                                          (where/unmatched-var '?p)
                                          (where/unmatched-var '?o))]
          union-pattern          (where/->pattern :union [branch-binds-a-and-b branch-binds-a-only])
          filter-descriptor      (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern union-pattern [filter-descriptor] #{})]
      (is (= 1 (count remaining-filters))
          "one filter remains because not all branches bind ?b")
      (is (= union-pattern pattern)
          "union pattern unchanged when push is unsafe"))))

(deftest graph-skips-virtual
  (testing "virtual graphs never receive pushed filters"
    (let [inner-clause     [(vector (where/unmatched-var '?a)
                                    (where/unmatched-var '?p)
                                    (where/unmatched-var '?b))]
          virtual-graph    (where/->pattern :graph ["##vector-index" inner-clause])
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern virtual-graph [filter-descriptor] #{})]
      (is (= 1 (count remaining-filters))
          "filter remains because graph is virtual")
      (is (= virtual-graph pattern)
          "graph pattern unchanged for virtual graphs"))))

(deftest graph-pushes-when-bound
  (testing "regular graphs receive pushed filters when inner binds all vars"
    (let [inner-clause     [(vector (where/unmatched-var '?a)
                                    (where/unmatched-var '?p)
                                    (where/unmatched-var '?b))]
          graph-pattern    (where/->pattern :graph ["g" inner-clause])
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern graph-pattern [filter-descriptor] #{})
          [_ updated-inner] (where/pattern-data pattern)]
      (is (empty? remaining-filters)
          "no filters remain at top level after push")
      (is (some #(= :filter (where/pattern-type %)) updated-inner)
          "inner clause contains the pushed filter"))))

(deftest optional-keeps-filters
  (testing "optionals never receive pushed filters"
    (let [inner-clause       [(vector (where/unmatched-var '?a)
                                      (where/unmatched-var '?p)
                                      (where/unmatched-var '?b))]
          optional-pattern   (where/->pattern :optional inner-clause)
          filter-descriptor  (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern optional-pattern [filter-descriptor] #{})]
      (is (= 1 (count remaining-filters))
          "filter remains at top level for optionals")
      (is (= optional-pattern pattern)
          "optional pattern unchanged"))))

(defn pattern-contains-filter?
  [pattern]
  (case (where/pattern-type pattern)
    :filter true
    :union (->> (where/pattern-data pattern)
                (some (fn [cl]
                        (some pattern-contains-filter? cl))))
    :graph (let [[_ inner] (where/pattern-data pattern)]
             (some pattern-contains-filter? inner))
    (:optional :exists :not-exists :minus)
    (->> (where/pattern-data pattern)
         (some pattern-contains-filter?))
    false))

(defn clause-contains-filter?
  [clause]
  (some pattern-contains-filter? clause))

(deftest nested-union-pushes-recursively
  (testing "filters propagate through nested unions when all paths bind"
    (let [inner-branch-1 [(vector (where/unmatched-var '?a)
                                  (where/unmatched-var '?p)
                                  (where/unmatched-var '?o))
                          (vector (where/unmatched-var '?s)
                                  (where/unmatched-var '?p2)
                                  (where/unmatched-var '?b))]
          inner-branch-2 [(vector (where/unmatched-var '?a)
                                  (where/unmatched-var '?p)
                                  (where/unmatched-var '?o2))
                          (vector (where/unmatched-var '?s2)
                                  (where/unmatched-var '?p2)
                                  (where/unmatched-var '?b))]
          inner-union        (where/->pattern :union [inner-branch-1 inner-branch-2])
          outer-branch-1     [inner-union]
          outer-branch-2     [inner-union]
          outer-union        (where/->pattern :union [outer-branch-1 outer-branch-2])
          filter-descriptor  (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [patterns filters]}
          (opt/push-filters [outer-union] [filter-descriptor] #{})
          updated-union      (first patterns)
          updated-branches   (where/pattern-data updated-union)]
      (is (empty? filters)
          "no filters remain at top level after recursive push")
      (is (= :union (where/pattern-type updated-union))
          "remains a union pattern")
      (is (every? clause-contains-filter? updated-branches)
          "each outer branch contains a filter somewhere within nested unions"))))

(deftest exists-pushes-when-bound
  (testing ":exists receives pushed filters when inner binds all vars"
    (let [inner-clause      [(vector (where/unmatched-var '?a)
                                     (where/unmatched-var '?p)
                                     (where/unmatched-var '?b))]
          exists-pattern    (where/->pattern :exists inner-clause)
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern exists-pattern [filter-descriptor] #{})
          updated-inner     (where/pattern-data pattern)]
      (is (empty? remaining-filters)
          "no filters remain at top level after push into exists")
      (is (some #(= :filter (where/pattern-type %)) updated-inner)
          "inner clause contains the pushed filter"))))

(deftest not-exists-pushes-when-bound
  (testing ":not-exists receives pushed filters when inner binds all vars"
    (let [inner-clause      [(vector (where/unmatched-var '?a)
                                     (where/unmatched-var '?p)
                                     (where/unmatched-var '?b))]
          not-exists-pattern (where/->pattern :not-exists inner-clause)
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern not-exists-pattern [filter-descriptor] #{})
          updated-inner     (where/pattern-data pattern)]
      (is (empty? remaining-filters)
          "no filters remain after push into not-exists")
      (is (some #(= :filter (where/pattern-type %)) updated-inner)
          "inner clause contains the pushed filter"))))

(deftest minus-keeps-when-not-all-bind
  (testing ":minus keeps filter top-level when inner does not bind all vars"
    (let [inner-clause      [(vector (where/unmatched-var '?a)
                                     (where/unmatched-var '?p)
                                     (where/unmatched-var '?o))] ;; only binds ?a
          minus-pattern     (where/->pattern :minus inner-clause)
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern minus-pattern [filter-descriptor] #{})]
      (is (= 1 (count remaining-filters))
          "filter remains top-level when :minus inner doesn't bind all vars")
      (is (= minus-pattern pattern)
          ":minus pattern unchanged when push is unsafe"))))

(deftest minus-pushes-when-bound
  (testing ":minus receives pushed filters when inner binds all vars"
    (let [inner-clause      [(vector (where/unmatched-var '?a)
                                     (where/unmatched-var '?p)
                                     (where/unmatched-var '?b))]
          minus-pattern     (where/->pattern :minus inner-clause)
          filter-descriptor (build-filter-descriptor ['?a '?b] ['?a '?b])
          {:keys [pattern remaining-filters]}
          (opt/push-into-pattern minus-pattern [filter-descriptor] #{})
          updated-inner     (where/pattern-data pattern)]
      (is (empty? remaining-filters)
          "no filters remain after push into :minus")
      (is (some #(= :filter (where/pattern-type %)) updated-inner)
          ":minus inner clause contains the pushed filter"))))
