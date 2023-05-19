(ns fluree.db.json-ld.credential-test
  (:require [fluree.db.json-ld.credential :as cred]
            [clojure.core.async :as async]
            #?(:clj [clojure.test :as t :refer [deftest testing is]]
               :cljs [cljs.test :as t :refer [deftest testing is] :include-macros true])
            [fluree.db.json-ld.api :as fluree]
            [fluree.json-ld :as json-ld]
            [fluree.db.test-utils :as test-utils]))

(def auth
  {:id "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   :public "03b160698617e3b4cd621afd96c0591e33824cb9753ab2f1dace567884b4e242b0"
   :private "509553eece84d5a410f1012e8e19e84e938f226aa3ad144e2d12f36df0f51c1e"})

(def example-cred-subject {"@context" {"a" "http://a.com/"} "a:foo" "bar"})
(def example-issuer "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh")

(def clj-generated-jws
  "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..HDBFAiEA80-G5gUH6BT9D1Mc-YyWbjuwbL7nKfWj6BrsHS6whQ0CIAcjzJvo0sW52FIlgvxy0hPBKNWolIwLvoedG_4HQu_V")

(def cljs-generated-jws
  "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..HDBFAiEAy9MuRjVn_vwvEgQlsCJNnSYwCJEWU_UOg5U_R8--87wCID-qficJv-aCUotctcFGX-xTky1E08W2Y7utUCJZ3AZY")

(def example-credential
  {"@context"          "https://www.w3.org/2018/credentials/v1"
   "id"                ""
   "type"              ["VerifiableCredential" "CommitProof"]
   "issuer"            example-issuer
   "issuanceDate"      "1970-01-01T00:00:00.00000Z"
   "credentialSubject" example-cred-subject
   "proof"             {"type"               "EcdsaSecp256k1RecoverySignature2020"
                        "created"            "1970-01-01T00:00:00.00000Z"
                        "verificationMethod" "did:key:z6DuABnw7XPbMksZo5wY4HweN8wPkEd7rCQM4YGgu8hPqrd5"
                        "proofPurpose"       "assertionMethod"
                        "jws"                #?(:clj clj-generated-jws
                                                :cljs cljs-generated-jws)}})

#?(:clj
   (deftest credential-test
     (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
       (testing "generate"
         (let [cred (async/<!! (cred/generate example-cred-subject (:private auth)))]
           (is (= example-credential
                  cred))))

       (testing "verify correct signature"
         (let [clj-result (async/<!! (cred/verify example-credential))
               cljs-result (async/<!! (cred/verify (assoc-in example-credential ["proof" "jws"] cljs-generated-jws)))]
           (is (= {:subject example-cred-subject :issuer example-issuer} clj-result))
           (is (= {:subject example-cred-subject :issuer example-issuer} cljs-result))))

       (testing "verify incorrect signature"
         (let [wrong-cred (assoc example-credential "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
           (is (= "Verification failed."
                  (-> (async/<!! (cred/verify wrong-cred))
                      (Throwable->map)
                      (:cause))))))

       (testing "verify not a credential"
         (let [non-cred example-cred-subject]
           (is (nil? (async/<!! (cred/verify non-cred)))))))))

#?(:cljs
   (deftest generate
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [cred (async/<! (cred/generate example-cred-subject (:private auth)))]
                    (is (= example-credential
                           cred))
                    (done)))))))

#?(:cljs
   (deftest verify-correct-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [cljs-result (async/<! (cred/verify example-credential))
                        clj-result  (async/<! (cred/verify (assoc-in example-credential ["proof" "jws"] clj-generated-jws)))]
                    (is (= {:subject example-cred-subject :issuer example-issuer} cljs-result))
                    (is (= {:subject example-cred-subject :issuer example-issuer} clj-result))
                    (done)))))))

#?(:cljs
   (deftest verify-incorrect-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [wrong-cred (assoc example-credential "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
                    (is (= "Verification failed."
                           (-> (async/<! (cred/verify wrong-cred))
                               (.-message e))))
                    (done)))))))
#?(:cljs
   (deftest verify-non-credential
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [non-cred example-cred-subject]
                    (is (nil? (async/<! (cred/verify non-cred))))
                    (done)))))))

#?(:clj
   (deftest ^:integration cred-wrapped-transactions-and-queries
     (let [conn   @(fluree/connect {:method :memory})
           ledger @(fluree/create conn "credentialtest" {:defaultContext {"@base" "ledger:credentialtest/" "@vocab" ""}})
           db0    (fluree/db ledger)

           tx  {"@id" "dan" "name" "Daniel" "favnums" [1 2 3]}
           db1 @(test-utils/transact ledger (async/<!! (cred/generate tx (:private auth))))

           mdfn {"delete" [["?s" "name" "Daniel"]
                           ["?s" "favnums" 1]]
                 "insert" [["?s" "name" "D"]
                           ["?s" "favnums" 4]
                           ["?s" "favnums" 5]
                           ["?s" "favnums" 6]]
                 "where"  [["?s" "@id" "dan"]]}
           db2  @(test-utils/transact ledger (async/<!! (cred/generate mdfn (:private auth))))

           query {"select" {"?s" ["*"]} "where" [["?s" "@id" "dan"]]}]

       (is (= [tx]
              @(fluree/query db1 query))
           "insert transaction credential")
       (is (= [{"@id" "dan" "name" "D" "favnums" [2 3 4 5 6]}]
              @(fluree/query db2 query))
           "modify transaction credential")

       (is (= [{"@id" "dan" "name" "D" "favnums" [2 3 4 5 6]}]
              @(fluree/query db2 (async/<!! (cred/generate query (:private auth)))))
           "query credential")

       (is (= {"nums" [2 3 4 5 6],
               "dan" [{"@id" "dan", "name" "D", "favnums" [2 3 4 5 6]}]}
              @(fluree/multi-query db2
                                   (async/<!! (cred/generate {"nums" {"select" "?nums" "where" [["?s" "favnums" "?nums"]]} "dan" query}
                                                             (:private auth)))))
           "multiquery credential")
       (is (= [{"https://ns.flur.ee/ledger#t" 1,
                "https://ns.flur.ee/ledger#assert" [{"@id" "dan", "name" "Daniel", "favnums" [1 2 3], :id "dan"}],
                "https://ns.flur.ee/ledger#retract" []}

               {"https://ns.flur.ee/ledger#t" 2,
                "https://ns.flur.ee/ledger#assert"  [{"name" "D", "favnums" [4 5 6], :id "dan"}],
                "https://ns.flur.ee/ledger#retract" [{"name" "Daniel", "favnums" 1, :id "dan"}]}]
              @(fluree/history ledger (async/<!! (cred/generate {:history "dan" :t {:from 1}} (:private auth)))))
           "history query credential"))))

(comment
  #?(:cljs

     (cljs.test/run-tests))

  ,)
