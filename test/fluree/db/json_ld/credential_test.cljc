(ns fluree.db.json-ld.credential-test
  (:require #?@(:clj  [[clojure.test :as t :refer [deftest testing is]]
                       [fluree.crypto :as crypto]
                       [fluree.db.api :as fluree]
                       [fluree.db.test-utils :as test-utils]]
                :cljs [[cljs.test :as t :refer [deftest is] :include-macros true]])
            [clojure.core.async :as async]
            [fluree.db.did :as did]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.util :as util]))

(def kp
  {:public  "a9fd267bed77dc320e54c7006ef33f61e7cbf9e6b81625a4b6a88d6a4c6b08d2"
   :private "27ee972212ecf6f1810b11ece94bb85487b4694580bcc189f731d54f0a242429"})
(def auth (did/private->did-map (:private kp)))

(def pleb-kp
  {:private "fb9fb212adbd3f803081a207e84998058a34add87deefe220c254d6cadb77322",
   :public  "a2bc9a6e6db53d037a9ba18dfd4673be0bad2b044c5fef0e0bb6903276b3607a"})
(def pleb-auth (did/private->did-map (:private pleb-kp)))

(def other-kp
  {:private "54d301972f19dc79c46733cce86d1f8561fd641d1eb9d1311912430ed285330f",
   :public  "2f72d6588192a9617049b5a4d5a6af95b6c5516a7fc9cae8784e73b22c8125e6"})
(def other-auth (did/private->did-map (:private other-kp)))

(def example-cred-subject {"@context" {"a" "http://a.com/"} "a:foo" "bar"})
(def example-issuer (:id auth))

(def clj-generated-jws
  "eyJhbGciOiJFZERTQSIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..QphLnTFJUZAqudIJQHWge3MdsjOWEeoZmveoD4L7k_wLXGMjoclsjnLuBHQqN_xzH9fFhChJMHk_HljcCpuvDQ==")

(def cljs-generated-jws
  "eyJhbGciOiJFZERTQSIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..QphLnTFJUZAqudIJQHWge3MdsjOWEeoZmveoD4L7k_wLXGMjoclsjnLuBHQqN_xzH9fFhChJMHk_HljcCpuvDQ==")

(def example-credential
  {"@context"          "https://www.w3.org/2018/credentials/v1"
   "id"                ""
   "type"              ["VerifiableCredential" "CommitProof"]
   "issuer"            (:id auth)
   "issuanceDate"      "1970-01-01T00:00:00.00000Z"
   "credentialSubject" example-cred-subject
   "proof"             {"type"               "Ed25519Signature2018"
                        "created"            "1970-01-01T00:00:00.00000Z"
                        "verificationMethod" "did:key:z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR"
                        "proofPurpose"       "assertionMethod"
                        "jws"                #?(:clj  clj-generated-jws
                                                :cljs cljs-generated-jws)}})

#?(:clj
   (deftest credential-test
     (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
       (testing "generate"
         (let [cred (async/<!! (cred/generate example-cred-subject (:private auth)))]
           (is (= (dissoc example-credential "proof")
                  (dissoc cred "proof")))
           (is (= "Ed25519Signature2018" (get-in cred ["proof" "type"])))
           (is (string? (get-in cred ["proof" "jws"])))))

       (testing "verify correct signature"
         (let [generated-cred (async/<!! (cred/generate example-cred-subject (:private auth)))
               verify-result  (async/<!! (cred/verify generated-cred))]
           (is (= {:subject example-cred-subject :did example-issuer} verify-result))))

       (testing "verify incorrect signature"
         (let [generated-cred (async/<!! (cred/generate example-cred-subject (:private auth)))
               wrong-cred (assoc generated-cred "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
           (is (= "Verification failed, invalid credential."
                  (-> (async/<!! (cred/verify wrong-cred))
                      (Throwable->map)
                      (:cause))))))

       (testing "verify not a credential"
         (let [non-cred example-cred-subject]
           (is (util/exception? (async/<!! (cred/verify non-cred)))))))))

#?(:cljs
   (deftest generate
     (t/async done
              (async/go
                (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [cred (async/<! (cred/generate example-cred-subject (:private auth)))]
                    (is (= (dissoc example-credential "proof")
                           (dissoc cred "proof")))
                    (is (= "Ed25519Signature2018" (get-in cred ["proof" "type"])))
                    (is (string? (get-in cred ["proof" "jws"])))
                    (done)))))))

#?(:cljs
   (deftest verify-correct-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [generated-cred (async/<! (cred/generate example-cred-subject (:private auth)))
                        verify-result  (async/<! (cred/verify generated-cred))]
                    (is (= {:subject example-cred-subject :did example-issuer} verify-result))
                    (done)))))))

#?(:cljs
   (deftest verify-incorrect-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [wrong-cred (assoc example-credential "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
                    (is (= "Verification failed, invalid credential."
                           (.-message (async/<! (cred/verify wrong-cred)))))
                    (done)))))))
#?(:cljs
   (deftest verify-non-credential
     (t/async done
              (async/go
                (with-redefs [fluree.db.util/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [non-cred example-cred-subject]
                    (is (util/exception? (async/<! (cred/verify non-cred))))
                    (done)))))))

#?(:clj
   (deftest ^:integration cred-wrapped-transactions-and-queries
     (let [conn      @(fluree/connect-memory)
           ledger-id "credentialtest"
           context   (merge test-utils/default-str-context
                            {"ct" "ledger:credentialtest/"})
           db0       @(fluree/create-with-txn conn {"@context" context
                                                    "ledger"   ledger-id
                                                    "insert"   {"@id" "ct:open" "ct:foo" "bar"}})

           root-user {"id"            (:id auth)
                      "type"          "ct:User"
                      "ct:name"       "Daniel"
                      "f:policyClass" {"id" "ct:DefaultUserPolicy"}
                      "ct:favnums"    [1 2 3]}
           pleb-user {"id"            (:id pleb-auth)
                      "type"          "ct:User"
                      "ct:name"       "Plebian"
                      "f:policyClass" {"@id" "ct:DefaultUserPolicy"}}
           policy    {"@context"    {"f"  "https://ns.flur.ee/ledger#"
                                     "ct" "ledger:credentialtest/"}
                      "@id"         "ct:userPolicy"
                      "@type"       ["f:AccessPolicy" "ct:DefaultUserPolicy"]
                      "f:required"  true
                      "f:targetSubject"
                      {"@type" "@json"
                       "@value"
                       {"@context" {"ct" "ledger:credentialtest/"}
                        "where" [{"@id" "?$target" "@type" {"@id" "ct:User"}}]}}
                      "f:action"    [{"@id" "f:view"}, {"@id" "f:modify"}]
                      "f:exMessage" "Users can only manage their own data."
                      "f:query"     {"@type"  "@json"
                                     "@value" {"where" [["filter" "(= ?$this ?$identity)"]]}}}
           d-policy  {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                  "ct" "ledger:credentialtest/"}
                      "@id"      "ct:defaultAllowViewModify"
                      "@type"    ["f:AccessPolicy" "ct:DefaultUserPolicy"]
                      "f:action" [{"@id" "f:view"}, {"@id" "f:modify"}]
                      "f:query"  {"@type"  "@json"
                                  "@value" {}}}
           tx        [root-user pleb-user policy d-policy]
           ;; can't use credentials until after an identity with a role has been created
           db1       @(fluree/update! conn {"@context" context
                                            "ledger"   ledger-id
                                            "insert"   tx})

           mdfn {"@context" context
                 "ledger"   ledger-id
                 "delete"   {"@id"        (:id auth)
                             "ct:name"    "Daniel"
                             "ct:favnums" 1}
                 "insert"   {"@id"        (:id auth)
                             "ct:name"    "D"
                             "ct:favnums" [4 5 6]}}

           db2 @(fluree/credential-update! conn (async/<!! (cred/generate mdfn (:private auth))))

           db0 @(fluree/load conn ledger-id)

           query {"@context" context
                  "select"   {(:id auth) ["*"]}}]
       (is (= [{"id" "ct:open", "ct:foo" "bar"}]
              @(fluree/query db0 {"@context" context
                                  "select"   {"ct:open" ["*"]}}))
           "can see everything when no identity is asserted")

       (is (= [root-user]
              @(fluree/query db1 query)))

       (is (= [{"id"            (:id auth)
                "type"          "ct:User"
                "ct:name"       "D"
                "ct:favnums"    [2 3 4 5 6]
                "f:policyClass" {"id" "ct:DefaultUserPolicy"}}]
              @(fluree/query db2 query))
           "modify transaction in credential")

       (is (= []
              @(fluree/credential-query
                db0
                (async/<!! (cred/generate query (:private pleb-auth)))))
           "query credential w/ policy forbidding access")

       (is (= [{"id"            (:id auth)
                "type"          "ct:User"
                "ct:name"       "D"
                "ct:favnums"    [2 3 4 5 6]
                "f:policyClass" {"id" "ct:DefaultUserPolicy"}}]
              @(fluree/credential-query
                db2
                (async/<!! (cred/generate query (:private auth)))))
           "query credential w/ policy allowing access, but only to ct:User class")

       (is (= []
              @(fluree/credential-query
                db2
                (async/<!! (cred/generate query (:private other-auth)))))
           "query credential w/ no roles")

       (is (= [{"f:t"       2,
                "f:assert"  [{"id"            (:id auth)
                              "type"          "ct:User"
                              "ct:name"       "Daniel"
                              "ct:favnums"    [1 2 3]
                              "f:policyClass" {"id" "ct:DefaultUserPolicy"}}],
                "f:retract" []}

               {"f:t"       3,
                "f:assert"  [{"ct:name" "D", "ct:favnums" [4 5 6], "id" (:id auth)}],
                "f:retract" [{"ct:name" "Daniel", "ct:favnums" 1, "id" (:id auth)}]}]
              @(fluree/credential-history
                conn ledger-id
                (async/<!! (cred/generate {:context context
                                           :history (:id auth)
                                           :t       {:from 1}}
                                          (:private auth)))))
           "history query credential - allowing access")
       (is (= []
              @(fluree/credential-history
                conn ledger-id
                (async/<!! (cred/generate {:history (:id auth)
                                           :t       {:from 1}}
                                          (:private pleb-auth)))))
           "history query credential - forbidding access")

       (let [sparql (str "PREFIX ct: <ledger:credentialtest/>
                        SELECT ?name
                        FROM <" ledger-id ">
                        WHERE { \"" (:id auth) "\" ct:name ?name }")]
         (is (= [["D"]]
                @(fluree/credential-query
                  db2
                  (crypto/create-jws sparql (:private auth) {:include-pubkey true})
                  {:format :sparql}))
             "SPARQL query credential - allowing access")

         (is (= []
                @(fluree/credential-query
                  db0
                  (crypto/create-jws sparql (:private pleb-auth) {:include-pubkey true})
                  {:format :sparql}))
             "SPARQL query credential - forbidding access")

         (is (= [["D"]]
                @(fluree/credential-query-connection
                  conn
                  (crypto/create-jws sparql (:private auth) {:include-pubkey true})
                  {:format :sparql}))
             "SPARQL query connection credential - allowing access")

         (is (= []
                @(fluree/credential-query-connection
                  conn
                  (crypto/create-jws sparql (:private pleb-auth) {:include-pubkey true})
                  {:format :sparql}))
             "SPARQL query connection credential - forbidding access")))))

(comment
  #?(:cljs

     (cljs.test/run-tests)))
