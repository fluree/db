(ns fluree.db.json-ld.credential-test
  (:require [fluree.db.json-ld.credential :as cred]
            [clojure.core.async :as async]
            #?(:clj [clojure.test :as t :refer [deftest testing is]]
               :cljs [cljs.test :as t :refer [deftest testing is] :include-macros true])))

(def auth
  {:id "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   :public "03b160698617e3b4cd621afd96c0591e33824cb9753ab2f1dace567884b4e242b0"
   :private "509553eece84d5a410f1012e8e19e84e938f226aa3ad144e2d12f36df0f51c1e"})

(def example-cred-subject {"@context" {"a" "http://a.com/"} "a:foo" "bar"})

(def clj-generated-jws
  "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..HDBFAiEA80-G5gUH6BT9D1Mc-YyWbjuwbL7nKfWj6BrsHS6whQ0CIAcjzJvo0sW52FIlgvxy0hPBKNWolIwLvoedG_4HQu_V")

(def cljs-generated-jws
  "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..HDBFAiEAy9MuRjVn_vwvEgQlsCJNnSYwCJEWU_UOg5U_R8--87wCID-qficJv-aCUotctcFGX-xTky1E08W2Y7utUCJZ3AZY")

(def expected-credential
  {"@context"          "https://www.w3.org/2018/credentials/v1"
   "id"                ""
   "type"              ["VerifiableCredential" "CommitProof"]
   "issuer"            "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   "issuanceDate"      "1970-01-01T00:00:00.00000Z"
   "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "bar"}
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
           (is (= expected-credential
                  cred))))
       (testing "verify correct signature"
         (let [clj-result (async/<!! (cred/verify expected-credential))
               cljs-result (async/<!! (cred/verify (assoc-in expected-credential ["proof" "jws"] cljs-generated-jws)))]
           (is (= true clj-result))
           (is (= true cljs-result))))
       (testing "verify incorrect signature"
         (let [wrong-cred (assoc expected-credential "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
           (is (= "Unverifiable credential"
                  (-> (async/<!! (cred/verify wrong-cred))
                      (Throwable->map)
                      (:cause)))))))))

#?(:cljs
   (deftest generate
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [cred (async/<! (cred/generate example-cred-subject (:private auth)))]
                    (is (= expected-credential
                           cred))
                    (done)))))))

#?(:cljs
   (deftest verify-correct-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [cljs-result (async/<! (cred/verify expected-credential))
                        clj-result  (async/<! (cred/verify (assoc-in expected-credential ["proof" "jws"] clj-generated-jws)))]
                    (is (= true cljs-result))
                    (is (= true clj-result))
                    (done)))))))

#?(:cljs
   (deftest verify-incorrect-signature
     (t/async done
              (async/go
                (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:00:00.00000Z")]
                  (let [wrong-cred (assoc expected-credential "credentialSubject" {"@context" {"a" "http://a.com/"} "a:foo" "DIFFERENT!"})]
                    (is (= "Unverifiable credential"
                           (-> (async/<! (cred/verify wrong-cred))
                               (.-message e))))
                    (done)))))))

(comment
  #?(:cljs

     (cljs.test/run-tests)
     )
  ,)
