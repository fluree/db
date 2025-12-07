(ns fluree.db.policy.target-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.util :as util]))

(def burt
  {:id    "did:key:z6MkjMMe1dHZ5Q8DxoFQpXCjhZvqswMva3WvLGUKAAabsBxr"
   :private "4048a60cbc9eb74814808f2b67897eea4b1f01d46c166b7c4d2be7902602a66a",
   :public "48c74639952b16d838cd2853f92c18e6c4bc9c34279599c40b2162798887dc5f"})
(def arnold
  {:id "did:key:z6MkotdZEDchBwi6wVUmeDSQ97wBXnxabmorRrXPKpJFtwvc"
   :private "a1093d046aa4982aae8668bcc276d0d14f7c7c80787e24046db774f3cc686dfb",
   :public "8c39066e2a21d17752380c7e5a70d0670b193dc4118e5fc976c45363f2d5c6f5"})
(def charles
  {:id    "did:key:z6MkqKMP8Hf71HrEgNsZp7EaPyxZZQRrgGaDxWA3bCLyzTfP"
   :private "6f097986028c319cc3a8386aa90b620f97f0e774d03e8bec2ab23a969c2e010f",
   :public "a169f1aeb13649e317b714078b7a429bbe5059ea43a7a781c159dcb375b78962"})

(def wishlist-create {"@context"     {"a" "http://a.co/"
                                      "f" "https://ns.flur.ee/ledger#"}
                      "@id"          "a:wishlistCreatePolicy"
                      "f:action"     {"@id" "f:modify"}
                      "f:required"   true
                      "f:exMessage"  "User can only create a wishlist linked to their own identity."
                      "f:onProperty" [{"@id" "a:wishlist"}]
                      "f:query"
                      {"@type"  "@json"
                       "@value" {"@context" {"a" "http://a.co/"}
                                 "where"    [["filter" "(= ?$this ?$identity)"]]}}})

(def wishlist-modify {"@context"     {"a" "http://a.co/"
                                      "f" "https://ns.flur.ee/ledger#"}
                      "@id"          "a:wishlistModifyPolicy"
                      "f:action"     {"@id" "f:modify"}
                      "f:required"   true
                      "f:exMessage"  "User can only modify own wishlist properties."
                      "f:onProperty" [{"@id" "a:name"} {"@id" "a:summary"} {"@id" "a:item"}]
                      "f:query"
                      {"@type"  "@json"
                       "@value" {"@context" {"a" "http://a.co/"}
                                 "where"    [{"@id" "?$identity" "a:wishlist" "?$this"}]}}})

(def wishlist-view {"@context"     {"a" "http://a.co/"
                                    "f" "https://ns.flur.ee/ledger#"}
                    "@id"          "a:wishlistViewPolicy"
                    "f:action"     {"@id" "f:view"}
                    "f:required"   true
                    "f:onProperty" [{"@id" "a:wishlist"}]
                    "f:query"
                    {"@type"  "@json"
                     "@value" {"@context" {"a" "http://a.co/"}
                               "where"    [["filter" "(= ?$this ?$identity)"]]}}})

(def item-create {"@context"     {"a" "http://a.co/"
                                  "f" "https://ns.flur.ee/ledger#"}
                  "@id"          "a:wishlistItemCreatePolicy"
                  "f:action"     {"@id" "f:modify"}
                  "f:required"   true
                  "f:exMessage"  "User can only create an item on their own wishlist."
                  "f:onProperty" [{"@id" "a:item"}]
                  "f:query"
                  {"@type"  "@json"
                   "@value" {"@context" {"a" "http://a.co/"}
                             "where"    [{"@id" "?$identity" "a:wishlist" "?$this"}]}}})

(def item-modify {"@context"     {"a" "http://a.co/"
                                  "f" "https://ns.flur.ee/ledger#"}
                  "@id"          "a:wishlistItemModifyPolicy"
                  "f:action"     {"@id" "f:modify"}
                  "f:required"   true
                  "f:exMessage"  "User can modify all but available on item."
                  "f:onProperty" [{"@id" "a:title"}
                                  {"@id" "a:description"}
                                  {"@id" "a:rank"}]
                  "f:query"
                  {"@type"  "@json"
                   "@value" {"@context" {"a" "http://a.co/"}
                             "where"    [{"@id" "?$identity" "a:wishlist" "?wishlist"}
                                         {"@id" "?wishlist" "a:item" "?$this"}]}}})

(def item-view {"@context" {"a" "http://a.co/"
                            "f" "https://ns.flur.ee/ledger#"}
                "@id"      "a:wishlistItemViewPolicy"

                "f:onProperty" [{"@id" "a:title"}
                                {"@id" "a:description"}
                                {"@id" "a:rank"}]
                "f:query"
                {"@type"  "@json"
                 "@value" {"@context" {"a" "http://a.co/"}
                           "where"    [["filter" "(= 1 1)"]]}}})

(def available {"@context" {"a" "http://a.co/"
                            "f" "https://ns.flur.ee/ledger#"}
                "@id" "a:availableModifyPolicy"
                "f:required" true
                "f:exMessage" "User cannot modify available status on their own items."
                "f:onProperty" [{"@id" "a:available"}]
                "f:query"
                {"@type" "@json"
                 "@value" {"@context" {"a" "http://a.co/"}
                           "where" [{"@id" "?owner" "a:wishlist" "?wishlist"}
                                    {"@id" "?wishlist" "a:item" "?$this"}
                                    ["filter" "(not= ?owner ?$identity)"]]}}})
(deftest wishlist-scenario
  (let [conn   @(fluree/connect-memory)
        db0    @(fluree/create conn "policy/target")
        db1    @(fluree/update db0 {"@context" {"a" "http://a.co/"}
                                    "insert"
                                    [{"@id"    (:id arnold)
                                      "a:name" "Arnold"}
                                     {"@id"    (:id burt)
                                      "a:name" "Burt"}
                                     {"@id"    (:id charles)
                                      "a:name" "Chuck"}]})]
    (testing "wishlist"
      (testing "not linked to user"
        (let [policy-db    @(fluree/wrap-policy db1 {"@graph" [wishlist-create wishlist-modify wishlist-view
                                                               item-create item-modify item-view available]}
                                                ["?$identity" [{"@value" (:id charles) "@type" "@id"}]])
              unauthorized @(fluree/update policy-db {"@context" {"a" "http://a.co/"}
                                                      "insert"
                                                      {"@id" (:id burt)
                                                       "a:wishlist"
                                                       {"@id"       "a:burt-wish1"
                                                        "a:name"    "Burt's Birthday"
                                                        "a:summary" "My birthday wishlist"}}
                                                      "opts"     {"meta" true}})]
          (is (= "User can only create a wishlist linked to their own identity."
                 (ex-message unauthorized)))
          (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 1, :allowed 0},
                  "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                  "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                  "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                 (:policy (ex-data unauthorized))))
          (is (= 3
                 (:fuel (ex-data unauthorized))))))
      (testing "linked to user"
        (let [policy-db  @(fluree/wrap-policy db1 {"@graph" [wishlist-create wishlist-modify wishlist-view
                                                             item-create item-modify item-view available]}
                                              ["?$identity" [{"@value" (:id burt) "@type" "@id"}]])
              txn-result @(fluree/update policy-db {"@context" {"a" "http://a.co/"}
                                                    "insert"
                                                    {"@id" (:id burt)
                                                     "a:wishlist"
                                                     {"@id"       "a:burt-wish1"
                                                      "a:name"    "Burt's Birthday"
                                                      "a:summary" "My birthday wishlist"}}
                                                    "opts"     {"meta" true}})
              authorized (:db txn-result)
              result     @(fluree/query authorized {"@context" {"a" "http://a.co/"}
                                                    "where"    [{"@id" (:id burt) "a:wishlist" "?wishlist"}]
                                                    "select"   "?wishlist"
                                                    "opts"     {"meta" true}})]
          (is (nil? (ex-data authorized)))
          (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 1, :allowed 1},
                  "http://a.co/wishlistModifyPolicy"     {:executed 2, :allowed 2},
                  "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                  "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                 (:policy txn-result)))
          (is (= 5
                 (:fuel txn-result)))
          (is (= ["a:burt-wish1"]
                 (:result result)))
          (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                  "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                  "http://a.co/wishlistViewPolicy"       {:executed 1, :allowed 1},
                  "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                  "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                  "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                 (:policy result)))
          (is (= 1
                 (:fuel result))))))
    (testing "wishlist item"
      (let [db2 @(fluree/update db1 {"@context" {"a" "http://a.co/"}
                                     "insert"
                                     {"@id" (:id burt)
                                      "a:wishlist"
                                      {"@id"       "a:burt-wish1"
                                       "a:name"    "Burt's Birthday"
                                       "a:summary" "My birthday wishlist"}}})]
        (testing "not linked to owner"
          (let [policy-db    @(fluree/wrap-policy db2 {"@graph" [wishlist-create wishlist-view
                                                                 item-create item-modify item-view available]}
                                                  ["?$identity" [{"@value" (:id charles) "@type" "@id"}]])
                unauthorized @(fluree/update policy-db {"insert"
                                                        {"@context" {"a" "http://a.co/"}
                                                         "@id"      "a:burt-wish1"
                                                         "a:item"   {"@id"           "a:burt-wish1-1"
                                                                     "a:title"       "helicopter"
                                                                     "a:description" "flying car, basically"
                                                                     "a:rank"        1
                                                                     "a:available"   true}}
                                                        "opts" {"meta" true}})]
            (is (= "User can only create an item on their own wishlist."
                   (ex-message unauthorized)))
            (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                    "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemCreatePolicy" {:executed 1, :allowed 0},
                    "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                    "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                   (:policy (ex-data unauthorized))))
            (is (= 5
                   (:fuel (ex-data unauthorized))))))
        (testing "linked to owner"
          (let [policy-db  @(fluree/wrap-policy db2 {"@graph" [wishlist-create wishlist-modify wishlist-view
                                                               item-create item-modify item-view available]}
                                                ["?$identity" [{"@value" (:id burt) "@type" "@id"}]])
                txn-result @(fluree/update policy-db {"@context" {"a" "http://a.co/"}
                                                      "insert"
                                                      {"@id"    "a:burt-wish1"
                                                       "a:item" {"@id"           "a:burt-wish1-1"
                                                                 "a:title"       "helicopter"
                                                                 "a:description" "flying car, basically"
                                                                 "a:rank"        1}}
                                                      "opts"     {"meta" true}})
                authorized (:db txn-result)
                result     @(fluree/query authorized {"@context" {"a" "http://a.co/"}
                                                      "select"   {"a:burt-wish1-1" ["*"]}
                                                      "opts"     {"meta" true}})]
            (is (nil? (ex-data authorized)))
            (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                    "http://a.co/wishlistModifyPolicy"     {:executed 1, :allowed 1},
                    "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemModifyPolicy" {:executed 3, :allowed 3},
                    "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                    "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                   (:policy txn-result)))
            (is (= 11
                   (:fuel txn-result)))
            (is (= [{"a:title"       "helicopter"
                     "a:description" "flying car, basically"
                     "a:rank"        1}]
                   (:result result)))
            (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                    "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                    "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                    "http://a.co/wishlistItemViewPolicy"   {:executed 3, :allowed 3},
                    "http://a.co/availableModifyPolicy"    {:executed 0, :allowed 0}}
                   (:policy result)))
            (is (= 3
                   (:fuel result)))))))
    (testing "item availability"
      (let [db2 @(fluree/update db1 {"@context" {"a" "http://a.co/"}
                                     "insert"
                                     {"@id" (:id burt)
                                      "a:wishlist"
                                      {"@id"       "a:burt-wish1"
                                       "a:name"    "Burt's Birthday"
                                       "a:summary" "My birthday wishlist"
                                       "a:item"
                                       [{"@id"           "a:burt-wish1-1"
                                         "a:title"       "helicopter"
                                         "a:description" "for enhanced mobility in the sky"
                                         "a:rank"        1
                                         "a:available"   true}
                                        {"@id"           "a:burt-wish1-2"
                                         "a:title"       "pogo stick"
                                         "a:description" "for enhanced mobility on the ground"
                                         "a:rank"        2
                                         "a:available"   false}]}}})]
        (testing "owners own item available status"
          (let [policy-db    @(fluree/wrap-policy db2 {"@graph" [wishlist-create wishlist-modify wishlist-view
                                                                 item-create item-modify item-view available]}
                                                  ["?$identity" [{"@value" (:id burt) "@type" "@id"}]])
                unauthorized @(fluree/update policy-db {"@context" {"a" "http://a.co/"}
                                                        "retract"  {"@id" "a:burt-wish1-2" "a:available" false}
                                                        "insert"   {"@id" "a:burt-wish1-2" "a:available" true}
                                                        "opts"     {"meta" true}})]
            (testing "cannot be modified by owner"
              (is (= "User cannot modify available status on their own items."
                     (ex-message unauthorized)))
              (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                      "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                      "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                      "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                      "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                      "http://a.co/wishlistItemViewPolicy"   {:executed 0, :allowed 0},
                      "http://a.co/availableModifyPolicy"    {:executed 1, :allowed 0}}
                     (:policy (ex-data unauthorized))))
              (is (= 3
                     (:fuel (ex-data unauthorized)))))
            (testing "cannot be viewed by owner"
              (let [result @(fluree/query policy-db {"@context" {"a" "http://a.co/"}
                                                     "select"   {"a:burt-wish1-2" ["*"]}
                                                     "opts"     {"meta" true}})]
                (is (= [{"a:title"       "pogo stick"
                         "a:description" "for enhanced mobility on the ground"
                         "a:rank"        2}]
                       (:result result)))
                (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                        "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                        "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemViewPolicy"   {:executed 3, :allowed 3},
                        "http://a.co/availableModifyPolicy"    {:executed 1, :allowed 0}}
                       (:policy result)))
                (is (= 6
                       (:fuel result)))))))
        (testing "non-owners item available status"
          (let [policy-db  @(fluree/wrap-policy db2 {"@graph" [wishlist-create wishlist-modify wishlist-view
                                                               item-create item-modify item-view available]}
                                                ["?$identity" [{"@value" (:id charles) "@type" "@id"}]])
                authorized @(fluree/update policy-db {"@context" {"a" "http://a.co/"}
                                                      "retract"  {"@id" "a:burt-wish1-1" "a:available" true}
                                                      "insert"   {"@id" "a:burt-wish1-1" "a:available" false}})]
            (testing "can be modified by non-owner"
              (is (nil? (ex-message authorized))))
            (testing "can be viewed by non-owner"
              (let [result @(fluree/query policy-db {"@context" {"a" "http://a.co/"}
                                                     "select"   {"a:burt-wish1-1" ["*"]}
                                                     "opts"     {"meta" true}})]
                (is (= [{"a:title"       "helicopter"
                         "a:description" "for enhanced mobility in the sky",
                         "a:rank"        1,
                         "a:available"   true}]
                       (:result result)))
                (is (= {"http://a.co/wishlistCreatePolicy"     {:executed 0, :allowed 0},
                        "http://a.co/wishlistModifyPolicy"     {:executed 0, :allowed 0},
                        "http://a.co/wishlistViewPolicy"       {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemCreatePolicy" {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemModifyPolicy" {:executed 0, :allowed 0},
                        "http://a.co/wishlistItemViewPolicy"   {:executed 3, :allowed 3},
                        "http://a.co/availableModifyPolicy"    {:executed 1, :allowed 1}}
                       (:policy result)))
                (is (= 6
                       (:fuel result)))))))))))

(deftest policy-class-test
  (let [conn   @(fluree/connect-memory)
        db0    @(fluree/create conn "policy/target")

        default-policy
        {"@id"      "ex:defaultAllowView"
         "@type"    ["f:AccessPolicy" "ex:UnclassPolicy" "http://example.org/ns/DoublePropertyPolicy"]
         "f:action" {"@id" "f:view"}
         "f:query"  {"@type"  "@json"
                     "@value" {}}}

        classification-policy
        {"@id"             "ex:unclassRestriction"
         "@type"           ["f:AccessPolicy" "ex:UnclassPolicy"]
         "f:required"      true
         "f:targetSubject" {"@type"  "@json"
                            "@value" {"@context" {"ex" "http://example.org/ns/"}
                                      "where"    [{"@id" "?$target" "@type" "ex:Data"}]}}
         "f:action"        [{"@id" "f:view"}, {"@id" "f:modify"}]
         "f:query"         {"@type"  "@json"
                            "@value" {"@context" {"ex" "http://example.org/ns/"}
                                      "where"    [{"@id" "?$this" "ex:classification" "?c"}
                                                  ["filter", "(< ?c 1)"]]}}}

        double-property-policy
        {"@id"              "ex:doublePropertyRestriction"
         "@type"            ["f:AccessPolicy" "http://example.org/ns/DoublePropertyPolicy"]
         "f:required"       true
         "f:onProperty" [{"@id" "http://example.org/ns/secretProperty"} {"@id" "http://example.org/ns/secretPropertyTwo"}]
         "f:action"         [{"@id" "f:view"}, {"@id" "f:modify"}]
         "f:query"          {"@type"  "@json"
                             "@value" {"where" [["filter" "(not= 1 1)"]]}}}

        db1 @(fluree/update db0 {"@context" {"ex" "http://example.org/ns/"
                                             "f"  "https://ns.flur.ee/ledger#"}
                                 "insert"
                                 [{"@id"               "ex:data-0",
                                   "@type"             "ex:Data",
                                   "ex:classification" 0}
                                  {"@id"                  "ex:data-1",
                                   "@type"                "ex:Data",
                                   "ex:classification"    1
                                   "ex:secretProperty"    "secret 1"
                                   "ex:secretPropertyTwo" "second secret 1"}
                                  {"@id"                  "ex:data-2",
                                   "@type"                "ex:Data",
                                   "ex:classification"    2
                                   "ex:secretProperty"    "secret 2"
                                   "ex:secretPropertyTwo" "second secret 2"}
                                 ;; note below is of class ex:Other, not ex:Data
                                  {"@id"               "ex:other",
                                   "@type"             "ex:Other",
                                   "ex:classification" -99}
                                 ;; a node that refers to items in ex:Data which, if
                                 ;; pulled in a graph crawl, should still be restricted
                                  {"@id"          "ex:referred",
                                   "@type"        "ex:Referrer",
                                   "ex:referData" [{"@id" "ex:data-0"}
                                                   {"@id" "ex:data-1"}
                                                   {"@id" "ex:data-2"}]}

                                  classification-policy double-property-policy]})]
    (testing "without default allow"
      (is (= [{"@type"             "ex:Data"
               "ex:classification" 0
               "@id"               "ex:data-0"}]
             @(fluree/query db1 {"@context" {"ex" "http://example.org/ns/"
                                             "f"  "https://ns.flur.ee/ledger#"},
                                 "where"    {"@id"   "?s",
                                             "@type" "ex:Data"},
                                 "select"   {"?s" ["*"]}
                                 "opts"     {"policyClass" "ex:UnclassPolicy"}}))
          "only data with classification < 1 should be visible when using opts.policyClass")
      (is (= []
             @(fluree/query db1 {"@context" {"ex" "http://example.org/ns/"
                                             "f"  "https://ns.flur.ee/ledger#"},
                                 "where"    {"@id"   "?s",
                                             "@type" "ex:Other"},
                                 "select"   {"?s" ["*"]}
                                 "opts"     {"policyClass" "ex:UnclassPolicy"}}))
          "ex:Other class should not be restricted"))
    (testing "with default allow"
      (let [db2 @(fluree/update db1 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"}
                                     "insert"   [default-policy]})]
        (testing "using opts.policyClass"
          (is (= [{"@type"             "ex:Data"
                   "ex:classification" 0
                   "@id"               "ex:data-0"}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"},
                                     "where"    {"@id"   "?s",
                                                 "@type" "ex:Data"},
                                     "select"   {"?s" ["*"]}
                                     "opts"     {"policyClass" "ex:UnclassPolicy"}}))
              "only data with classification < 1 should be visible when using opts.policyClass")
          (is (= [{"@id"               "ex:other",
                   "@type"             "ex:Other",
                   "ex:classification" -99}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"},
                                     "where"    {"@id"   "?s",
                                                 "@type" "ex:Other"},
                                     "select"   {"?s" ["*"]}
                                     "opts"     {"policyClass" "ex:UnclassPolicy"}}))
              "ex:Other class should not be restricted")

          (is (= [{"@id"          "ex:referred"
                   "@type"        "ex:Referrer"
                   "ex:referData" [{"@id"               "ex:data-0"
                                    "@type"             "ex:Data"
                                    "ex:classification" 0}]}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"},
                                     "where"    {"@id"   "?s",
                                                 "@type" "ex:Referrer"},
                                     "select"   {"?s" ["*" {"ex:referData" ["*"]}]}
                                     "opts"     {"policyClass" "ex:UnclassPolicy"}}))
              "in graph crawl ex:Data is still restricted")
          (is (= [{"@id"               "ex:data-0"
                   "@type"             "ex:Data"
                   "ex:classification" 0}
                  {"@id"               "ex:data-1"
                   "@type"             "ex:Data"
                   "ex:classification" 1}
                  {"@id"               "ex:data-2"
                   "@type"             "ex:Data"
                   "ex:classification" 2}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"},
                                     "where"    {"@id"   "?s" "@type" "ex:Data"}
                                     "select"   {"?s" ["*"]}
                                     "opts"     {"policyClass" "ex:DoublePropertyPolicy"}}))
              "all properties besides secretProperty and secretPropertyTwo should be visible when using opts.policyClass"))
        (testing "using opts.policy"
          (is (= [{"@type"             "ex:Data"
                   "ex:classification" 0
                   "@id"               "ex:data-0"}]
                 @(fluree/query db2 {"@context" {"ex" "http://example.org/ns/"
                                                 "f"  "https://ns.flur.ee/ledger#"},
                                     "where"    {"@id"   "?s",
                                                 "@type" "ex:Data"},
                                     "select"   {"?s" ["*"]}
                                     "opts"     {"policy" [default-policy classification-policy]}}))
              "only data with classification < 1 should be visible when using opts.policy"))))))

(deftest ^:integration target-policy-exception
  (testing "Invalid policy target queries throw exceptions"
    (let [conn   @(fluree/connect-memory)
          db0    @(fluree/create conn "policy/target-policy-exception")
          db1    @(fluree/update db0 {"@context" {"a" "http://a.co/"}
                                      "insert"
                                      [{"@id"    (:id arnold)
                                        "a:name" "Arnold"}
                                       {"@id"    (:id burt)
                                        "a:name" "Burt"}
                                       {"@id"    (:id charles)
                                        "a:name" "Chuck"}]})
          bad-policy [{"@context" {"ex" "http://example.org/ns/"
                                   "f"  "https://ns.flur.ee/ledger#"}
                       "@id"             "ex:badSubjectPolicy"
                       "@type"           ["f:AccessPolicy"]
                       "f:targetSubject" {"@type"  "@json"
                                          "@value" {"blah" {[] []}}}
                       "f:action"        [{"@id" "f:view"}, {"@id" "f:modify"}]
                       "f:query"         {"@type"  "@json"
                                          "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                    "where"    [{"@id" "?$this" "ex:classification" "?c"}
                                                                ["filter", "(< ?c 1)"]]}}}]
          bad-db @(fluree/wrap-policy db1 bad-policy)]

      (is (util/exception? bad-db)))))
