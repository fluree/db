(ns fluree.db.query.fql-parse-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.query.exec.where :as where]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.query.fql.parse :as parse]))

(defn de-recordify-select
  "Select statements are parsed into records.
  This fn turns them into raw maps/vectors for ease of testing "
  [select]
  (if (sequential? select)
    (mapv #(into {} %) select)
    (into {} select)))

(deftest test-parse-query
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/parse"
                               {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
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
                   {:id          :ex/cam,
                    :type        :ex/User,
                    :schema/name "Cam"
                    :ex/email    "cam@example.org"
                    :schema/age  34
                    :ex/favNums  [5, 10]
                    :ex/friend   [:ex/brian :ex/alice]}])]
    (testing "parse-analytical-query"
      (let [ssc {:select {"?s" ["*"]}
                 :where  [["?s" :schema/name "Alice"]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query* ssc db)
            {::where/keys [patterns]} where]
        (is (= {:var       '?s
                :selection ["*"]
                :depth     0
                :spec      {:depth 0 :wildcard? true}}
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val 1002 ::where/datatype 8}
                 {::where/val "Alice" ::where/datatype 1}]]
               patterns)))

      (let [values-query '{:select {"?s" ["*"]}
                           :where  [["?s" :schema/name ?name]]
                           :values [?name ["Alice"]]}

            {:keys [select where values] :as parsed}
            (parse/parse-analytical-query* values-query db)

            {::where/keys [patterns]} where]
        (is (= '[{?name
                  {::where/var ?name
                   ::where/val "Alice"
                   ::where/datatype 1}}]
               values))
        (is (= {:var       '?s
                :selection ["*"]
                :depth     0
                :spec      {:depth 0 :wildcard? true}}
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val      1002
                  ::where/datatype 8}
                 {::where/var '?name}]]
               patterns)))
      (let [query {:select  ['?name '?age '?email]
                   :where   [['?s :schema/name "Cam"]
                             ['?s :ex/friend '?f]
                             ['?f :schema/name '?name]
                             ['?f :schema/age '?age]
                             ['?f :ex/email '?email]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query query db)
            {::where/keys [patterns]} where]
        (is (= [{:var '?name}
                {:var '?age}
                {:var '?email}]
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val      1002
                  ::where/datatype 8}
                 {::where/val      "Cam"
                  ::where/datatype 1}]
                [{::where/var '?s}
                 {::where/val      1008
                  ::where/datatype 8}
                 {::where/var '?f}]
                [{::where/var '?f}
                 {::where/val      1002
                  ::where/datatype 8}
                 {::where/var '?name}]
                [{::where/var '?f}
                 {::where/val      1004
                  ::where/datatype 8}
                 {::where/var '?age}]
                [{::where/var '?f}
                 {::where/val      1007
                  ::where/datatype 8}
                 {::where/var '?email}]]
               patterns)))
      (testing "not a `:class` pattern if obj is a var"
        (let [query {:context {:ex "http://example.org/ns/"}
                     :select  ['?class]
                     :where   [[:ex/cam :type '?class]]}
              {:keys [where]} (parse/parse-analytical-query query db)
              {::where/keys [patterns]} where]
          (is (= :tuple
                (where/pattern-type (first patterns))))))
      (testing "class, optional"
        (let [optional-q {:select ['?name '?favColor]
                          :where  [['?s :type :ex/User]
                                   ['?s :schema/name '?name]
                                   {:optional ['?s :ex/favColor '?favColor]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query optional-q db)
              {::where/keys [patterns]} where]
          (is (= [{:var '?name} {:var '?favColor}]
                 (mapv #(into {} %) select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 8}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [{::where/var '?s}
                   {::where/val      1002
                    ::where/datatype 8}
                   {::where/var '?name}]
                  [:optional
                   {::where/patterns
                    [[{::where/var '?s}
                      {::where/val      1006
                       ::where/datatype 8}
                      {::where/var '?favColor}]]}]]
                 patterns))))
      (testing "class, union"
        (let [union-q {:select ['?s '?email1 '?email2]
                       :where  [['?s :type :ex/User]
                                {:union [[['?s :ex/email '?email1]]
                                         [['?s :schema/email '?email2]]]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query union-q db)
              {::where/keys [patterns]} where]
          (is (= [{:var '?s} {:var '?email1} {:var '?email2}]
                 (de-recordify-select select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 8}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [:union
                   [{::where/patterns
                     [[{::where/var '?s}
                       {::where/val      1007
                        ::where/datatype 8}
                       {::where/var '?email1}]]}
                    {::where/patterns
                     [[{::where/var '?s}
                       {::where/val      1003
                        ::where/datatype 8}
                       {::where/var '?email2}]]}]]]
                 patterns))))
      (testing "class, filters"
        (let [filter-q {:select ['?name '?age]
                        :where  [['?s :type :ex/User]
                                 ['?s :schema/age '?age]
                                 ['?s :schema/name '?name]
                                 {:filter ["(> ?age 45)", "(< ?age 50)"]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query filter-q db)
              {::where/keys [patterns filters]} where]
          (is (= [{:var '?name} {:var '?age}]
                 (de-recordify-select select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 8}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [{::where/var '?s}
                   {::where/val      1004
                    ::where/datatype 8}
                   {::where/var '?age}]
                  [{::where/var '?s}
                   {::where/val      1002
                    ::where/datatype 8}
                   {::where/var '?name}]]
                 patterns))))
      (testing "group-by, order-by"
        (let [query {:select   ['?name '?favNums]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/favNums '?favNums]]
                     :group-by '?name
                     :order-by '?name}
              {:keys [select where group-by order-by] :as parsed} (parse/parse-analytical-query query db)]
          (is (= ['?name]
                 group-by))
          (is (= [['?name :asc]]
                 order-by)))))))





(deftest test-validation-errs
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/parse"
                               {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
                  [{:id          :ex/brian,
                    :type        :ex/User,
                    :schema/name "Brian"}
                   {:id          :ex/alice,
                    :type        :ex/User,
                    :ex/email    "alice@foo.com"
                    :schema/name "Alice"}
                   {:id           :ex/cam,
                    :type         :ex/User,
                    :schema/email "cam@bar.com"
                    :schema/name  "Cam"}])]
    (testing "missing select"
      (let [missing-select     '{:where  [[?s ?p ?o ]]}
            missing-select-err (try @(fluree/query db missing-select)
                                     (catch Exception e e))]
        (is (= {:status 400 :error :db/invalid-query}
               (ex-data missing-select-err)))
        (is (= "Missing select"
               (ex-message missing-select-err)))))
    (testing "multiple select"
      (let [multiple-select     '{:select [?s]
                                  :selectOne [?s ?p]
                                  :where  [[?s ?p ?o ]]}
            multiple-select-err (try @(fluree/query db multiple-select)
                                     (catch Exception e e))]
        (is (= {:status 400 :error :db/invalid-query}
               (ex-data multiple-select-err)))
        (is (= "Invalid select statement."
               (ex-message multiple-select-err)))))
    (testing "invalid var select"
      (let [invalid-var-select     '{:select [+]
                             :where  [[?s ?p ?o ]]}
            invalid-var-select-err (try @(fluree/query db invalid-var-select)
                                (catch Exception e e))]
        (is (= {:status 400 :error :db/invalid-query}
               (ex-data invalid-var-select-err)))
        (is (= "Invalid select statement. Every selection must be a string or map. Provided: [+]"
               (ex-message invalid-var-select-err)))))
      (testing "more than 1 key in where map"
        (let [multi-key-where-map     '{:select ['?name '?email]
                                  :where  [['?s :type :ex/User]
                                           ['?s :schema/age '?age]
                                           ['?s :schema/name '?name]
                                           {:union  [[['?s :ex/email '?email]]
                                                     [['?s :schema/email '?email]]]
                                            :filter ["(> ?age 30)"]}]}
              multi-key-where-map-err (try @(fluree/query db multi-key-where-map)
                                     (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data multi-key-where-map-err)))
          (is (= "Where clause maps can only have one key/val, provided: {:union [[['?s :ex/email '?email]]
                                              [['?s :schema/email '?email]]]
                                      :filter [\"(> ?age 30)\"]}"
                 (ex-message multi-key-where-map-err)))))
      ;;TODO missing a good error/message somewhere.
      (testing "unrecognized op"
        (let [unrecognized-where-op     '{:select ['?name '?age]
                                          :where  [['?s :type :ex/User]
                                                   ['?s :schema/age '?age]
                                                   ['?s :schema/name '?name]
                                                   {:foo "(> ?age 45)"}]}
              unrecognized-where-op-err (try @(fluree/query db unrecognized-where-op)
                                            (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data unrecognized-where-op-err)))
          (is (= "Invalid where clause, unsupported where clause operation: :foo"
                 (ex-message unrecognized-where-op-err)))))
      ;;TODO just top-level error
      (testing "nonsequential where"
        (let [non-sequential-where     '{:select [?s ?o]
                                         :where  ?s}
              non-sequential-where-err (try @(fluree/query db non-sequential-where)
                                            (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data non-sequential-where-err)))
          (is (= "Invalid where clause, must be a vector of tuples and/or maps. Provided: ?s"
                 (ex-message non-sequential-where-err)))))
      ;;TODO just top-level error, only returning '?s
      (testing "unwrapped where"
        (let [unwrapped-where     '{:select [?s ?o]
                                    :where  [?s ?p ?o]}
              unwrapped-where-err (try @(fluree/query db unwrapped-where)
                                       (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data unwrapped-where-err)))
          (is (= "Invalid where clause, must be a vector of tuples and/or maps. Provided:"
                 (ex-message unwrapped-where-err)))))
      (testing "invalid group-by"
        (let [invalid-group-by     '{:select   [?s]
                                     :where    [[?s ?p ?o]]
                                     :group-by {}}
              invalid-group-by-err (try @(fluree/query db invalid-group-by)
                                        (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data invalid-group-by-err)))
          (is (= "Invalid groupBy clause, must be a variable or a vector of variables. Provided: {}"
                 (ex-message invalid-group-by-err)))))
      ;;TODO not getting the asc/desc error
      (testing "invalid order-by"
        (let [invalid-order-by-op     '{:select  ['?name '?favNums]
                                        :where   [['?s :schema/name '?name]
                                                  ['?s :schema/age '?age]
                                                  ['?s :ex/favNums '?favNums]]
                                        :orderBy [(foo  ?favNums)]}
              invalid-order-by-op-err (try @(fluree/query db invalid-order-by-op)
                                           (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data invalid-order-by-op-err)))
          (is (= "Invalid orderBy clause, must be variable or two-tuple formatted ['ASC' or 'DESC', var]. Provided: foo "
                 (ex-message invalid-order-by-op-err)))))
      (testing "invalid bind"
        (let [invalid-bind     '{:select [?firstLetterOfName ?name ?canVote]
                                 :where  [[?s :schema/age ?age]
                                          [?s :schema/name ?name]
                                          {:bind [?canVote           (>= ?age 18)]}]}
              invalid-bind-err (try @(fluree/query db invalid-bind)
                                    (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data invalid-bind-err)))
          (is (= "Invalid where clause, 'bind' must be a map with binding vars as keys and binding scalars, or aggregates, as values. Provided: [?canVote (>= ?age 18)]"
                 (ex-message invalid-bind-err)))))
      (testing "filter not wrapped"
        (let [filter-type-err     '{:select ['?name '?age]
                                    :where  [['?s :type :ex/User]
                                             ['?s :schema/age '?age]
                                             ['?s :schema/name '?name]
                                             {:filter "(> ?age 45)"}]}
              filter-type-err-err (try @(fluree/query db filter-type-err)
                                       (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data filter-type-err-err)))
          (is (= "Filter must be enclosed in square brackets. Provided: (> ?age 45)"
                 (ex-message filter-type-err-err)))))
      (testing "filter bad type"
        (let [filter-type-err     '{:select ['?name '?age]
                                    :where  [['?s :type :ex/User]
                                             ['?s :schema/age '?age]
                                             ['?s :schema/name '?name]
                                             {:filter :foo}]}
              filter-type-err-err (try @(fluree/query db filter-type-err)
                                       (catch Exception e e))]
          (is (= {:status 400 :error :db/invalid-query}
                 (ex-data filter-type-err-err)))
          (is (= "Filter must be a vector/array. Provided: :foo"
                 (ex-message filter-type-err-err)))))))

;; (comment

;; {:explained
;;  ({:path [0 2 :where 0 0 0 :where-map 0 0],
;;    :in [:where 3],
;;    :schema
;;    [:map-of
;;     {:max 1,
;;      :error/message "Where clause maps can only have one key/val."}
;;     :fluree.db.validation/where-op
;;     :any],
;;    :value
;;    {:union [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
;;     :filter ["(> ?age 30)"]},
;;    :type :malli.core/limits}
;;   {:path [0 2 :where 0 0 0 :tuple 0 :triple 0],
;;    :in [:where 3],
;;    :schema
;;    [:catn
;;     [:subject
;;      [:orn
;;       [:var :fluree.db.validation/var]
;;       [:val :fluree.db.validation/subject]]]
;;     [:predicate
;;      [:orn
;;       [:var :fluree.db.validation/var]
;;       [:iri :fluree.db.validation/iri]]]
;;     [:object
;;      [:orn
;;       [:var :fluree.db.validation/var]
;;       [:ident [:fn "[fluree.db.util.core/pred-ident?]"]]
;;       [:iri-map :fluree.db.validation/iri-map]
;;       [:val :any]]]],
;;    :value
;;    {:union [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
;;     :filter ["(> ?age 30)"]},
;;    :type :malli.core/invalid-type}
;;   {:path [0 2 :where 0 0 0 :tuple 0 :remote],
;;    :in [:where 3],
;;    :schema [:sequential {:max 4} :any],
;;    :value
;;    {:union [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
;;     :filter ["(> ?age 30)"]},
;;    :type :malli.core/invalid-type}),
;;  :resolved
;;  [[:where]
;;   "Invalid where clause, must be a vector of tuples and/or maps."
;;   #:error{:message
;;           "Invalid where clause, must be a vector of tuples and/or maps."}]}


;; (do

;;     (require '[malli.core :as m])
;;     (require ' [fluree.db.validation :as v])
;;     (require '[malli.error :as me])
;;     (require '[malli.util :as mu])
;;     (require '[fluree.db.query.fql.syntax :as stx]))
;;   (do
;;     (def conn   (test-utils/create-conn))
;;     (def  ledger @(fluree/create conn "query/parse"
;;                                  {:defaultContext ["" {:ex "http://example.org/ns/"}]}))
;;     (def  db     @(fluree/stage
;;                     (fluree/db ledger)
;;                     [{:id           :ex/brian,
;;                       :type         :ex/User,
;;                       :schema/name  "Brian"}
;;                      {:id           :ex/alice,
;;                       :type         :ex/User,
;;                       :schema/name  "Alice"}
;;                      {:id          :ex/cam,
;;                       :type        :ex/User,
;;                       :schema/name "Cam"}])))
;;   ;;unnested where that doesn't work
;; (let [bad-type '{:select [?s]

;;                        :where [[?s ?p ?o]]
;;                  :group-by {}}]
;;   (ex-message @(fluree/query db bad-type)))

;; (let [bad-type '{:select [?s]
;;                        :where [?s ?p ?o]
;;   }]
;;   (ex-message @(fluree/query db bad-type)))



;; {:path [0 2 :group-by 0 :clause 0 0],
;;   :in [:group-by],
;;   :schema :symbol,
;;   :value {}}
;;  {:path [0 2 :group-by 0 :collection],
;;   :in [:group-by],
;;   :schema [:sequential :fluree.db.query.fql.syntax/var],
;;   :value {},
;;   :type :malli.core/invalid-type}

;;  )
