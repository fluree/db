(ns fluree.db.query.optional-query-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [fluree.db.test-utils :as test-utils]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.log :as log]))

(deftest ^:integration optional-queries
  (testing "Testing various 'optional' query clauses."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/optional" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage
                    (fluree/db ledger)
                    {"@context" "https://ns.flur.ee"
                     "insert"
                     [{:id          :ex/brian,
                       :type        :ex/User,
                       :schema/name "Brian"
                       :ex/friend   [:ex/alice]}
                      {:id           :ex/alice,
                       :type         :ex/User,
                       :ex/favColor  "Green"
                       :schema/email "alice@flur.ee"
                       :schema/name  "Alice"}
                      {:id           :ex/cam,
                       :type         :ex/User,
                       :schema/name  "Cam"
                       :schema/email "cam@flur.ee"
                       :ex/friend    [:ex/brian :ex/alice]}]})]

      ;; basic single optional statement
      (is (= #{["Cam" nil]
               ["Alice" "Green"]
               ["Brian" nil]}
             (set @(fluree/query db '{:select [?name ?favColor]
                                      :where  [{:id          ?s
                                                :type        :ex/User
                                                :schema/name ?name}
                                               [:optional {:id ?s, :ex/favColor ?favColor}]]})))
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      (is (= #{["Cam" nil]
               ["Alice" "Green"]
               ["Brian" nil]}
             (set @(fluree/query db '{:select [?name ?favColor]
                                      :where  [{:id          ?s
                                                :type        :ex/User
                                                :schema/name ?name}
                                               ["optional" {:id ?s, :ex/favColor ?favColor}]]})))
          "Cam, Alice and Brian should all return, but only Alice has a favColor, even with string 'optional' key")

      ;; including another pass-through variable - note Brian doesn't have an email
      (is (= #{["Cam" nil "cam@flur.ee"]
               ["Alice" "Green" "alice@flur.ee"]}
             (set @(fluree/query db '{:select [?name ?favColor ?email]
                                      :where  [{:id           ?s
                                                :type         :ex/User
                                                :schema/name  ?name
                                                :schema/email ?email}
                                               [:optional {:id ?s, :ex/favColor ?favColor}]]}))))

      ;; including another pass-through variable, but with 'optional' sandwiched
      (is (= #{["Cam" nil "cam@flur.ee"]
               ["Alice" "Green" "alice@flur.ee"]}
             (set @(fluree/query db '{:select [?name ?favColor ?email]
                                      :where  [{:id          ?s,
                                                :type        :ex/User
                                                :schema/name ?name}
                                               [:optional {:id ?s, :ex/favColor ?favColor}]
                                               {:id           ?s
                                                :schema/email ?email}]}))))

      ;; query with two optionals!
      (is (= #{["Cam" nil "cam@flur.ee"]
               ["Alice" "Green" "alice@flur.ee"]
               ["Brian" nil nil]}
             (set @(fluree/query db '{:select [?name ?favColor ?email]
                                      :where  [{:id          ?s
                                                :type        :ex/User
                                                :schema/name ?name}
                                               [:optional {:id ?s, :ex/favColor ?favColor}]
                                               [:optional {:id ?s, :schema/email ?email}]]}))))

      ;; query with two optionals in the same vector
      (is (= #{["Cam" nil "cam@flur.ee"]
               ["Alice" "Green" "alice@flur.ee"]
               ["Brian" nil nil]}
             (set @(fluree/query db '{:select [?name ?favColor ?email]
                                      :where  [{:id          ?s
                                                :type        :ex/User
                                                :schema/name ?name}
                                               [:optional
                                                {:id ?s, :ex/favColor ?favColor}
                                                {:id ?s, :schema/email ?email}]]}))))

      ;; optional with unnecessary embedded vector statement
      (is (= #{["Cam" nil]
               ["Alice" "Green"]
               ["Brian" nil]}
             (set @(fluree/query db '{:select [?name ?favColor]
                                      :where  [{:id ?s
                                                :type :ex/User
                                                :schema/name ?name}
                                               [:optional {:id ?s, :ex/favColor ?favColor}]]})))
          "Cam, Alice and Brian should all return, but only Alica has a favColor")

      ;; Multiple optional clauses should work as a left outer join between them
      (is (= #{["Cam" nil nil]
               ["Alice" "Green" "alice@flur.ee"]
               ["Brian" nil nil]}
             (set @(fluree/query db '{:select [?name ?favColor ?email]
                                      :where  [{:id ?s
                                                :type :ex/User
                                                :schema/name ?name}
                                               [:optional {:id ?s,
                                                           :ex/favColor ?favColor
                                                           :schema/email ?email}]]})))
          "Multiple optional clauses should work as a left outer join between them"))))
