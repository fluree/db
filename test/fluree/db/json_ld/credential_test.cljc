(ns fluree.db.json-ld.credential-test
  (:require #?(:clj [clojure.test :as t :refer [deftest testing is]]
               :cljs [cljs.test :as t :include-macros true])
            [fluree.db.json-ld.credential :as cred]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [alphabase.core :as alphabase]))

(def keypair
  {:private "32fcc1cad13694e8ae70f14c6fcd9c407437f9769616d8824cf9c1efa5cf2e30",
   :public "02807c7f9399b74a4a7e81c70ee5ad51aabf7d23a3b9f440202a26fcb9fa0dcccf"})
(def wrong-keypair
  {:private "08dbc8463b44c406c89de79335fde4a08abe309783a30f8430346cff361b08c9",
   :public "03eb8faba3c499827a7edbb300f9889fda240ba5405a82aca140996ec4b5c48b1b"})
(def issuer (str "did:fluree:" (crypto/account-id-from-public (:public keypair))))
(def credential-subject {"@context" ["https://flur.ee/ns/block"
                                     {"id" "@id" ,
                                      "type" "@type" ,
                                      "rdfs" "http://www.w3.org/2000/01/rdf-schema#" ,
                                      "schema" "http://schema.org/" ,
                                      "wiki" "https://www.wikidata.org/wiki/" ,
                                      "schema:isBasedOn" {"@type" "@id"} ,
                                      "schema:author" {"@type" "@id"}}] ,
                         "type" ["Commit"] ,
                         "branch" "main" ,
                         "t" 1,
                         "message" "Initial commit" ,
                         "assert" [{"type" "rdfs:Class" , "id" "schema:Movie"}
                                   {"type" "rdfs:Class" , "id" "schema:Book"}
                                   {"type" "rdfs:Class" , "id" "schema:Person"}
                                   {"schema:isBasedOn" "wiki:Q3107329" ,
                                    "schema:titleEIDR" "10.5240/B752-5B47-DBBE-E5D4-5A3F-N" ,
                                    "schema:disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings" ,
                                    "schema:name" "The Hitchhiker's Guide to the Galaxy" ,
                                    "type" "schema:Movie" ,
                                    "id" "wiki:Q836821"}
                                   {"schema:author" "wiki:Q42" ,
                                    "schema:isbn" "0-330-25864-8" ,
                                    "schema:name" "The Hitchhiker's Guide to the Galaxy" ,
                                    "type" "schema:Book" ,
                                    "id" "wiki:Q3107329"}
                                   {"schema:name" "Douglas Adams" , "type" "schema:Person" , "id" "wiki:Q42"}]})

(deftest signing-and-verification
  (testing "signing-input"
    (let [payload                  "hey"
          input                    (cred/signing-input payload)
          [b64-header b64-payload] (str/split input #"\.")]
      (is (= "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19.aGV5" input))
      (is (= cred/jws-header-b64 b64-header))))

  (testing "serialize-jws"
    (is (= "header.payload.signature" (cred/serialize-jws "header.payload" "signature"))))

  (testing "deserialize-jws"
    (let [jws (cred/sign "hey" (:private keypair))]
      (is (= {:header    "{\"alg\":\"ES256K-R\",\"b64\":false,\"crit\":[\"b64\"]}"
              :payload   "hey"
              :signature "1b30440220630eea4c43e4730a0b022c62378b356a49b7185a198ce783fe73d9e82fa4ec1e02201077af5cbcb8ccf729472d97163797dfa13b9998264b15cc80273f1fd9d10959"}
             (cred/deserialize-jws jws)))))

  (testing "sign"
    (is (= "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19.aGV5.GzBEAiBjDupMQ-RzCgsCLGI3izVqSbcYWhmM54P-c9noL6TsHgIgEHevXLy4zPcpRy2XFjeX36E7mZgmSxXMgCc_H9nRCVk="
           (cred/sign "hey" (:private keypair)))))

  (testing "add-proof"
      (with-redefs [fluree.db.util.core/current-time-iso (constantly "2020-01-01T21:26:29.218179Z")]
        (is (= {:credential {"@context" ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"]
                             "issuer" "did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"
                             "type" ["VerifiableCredential"]
                             "issuanceDate" "2020-01-01T21:26:29.218179Z"
                             "credentialSubject" {"some" "data"
                                                  "other" "data"}
                             "proof" {"type" "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020"
                                      "created" "2020-01-01T21:26:29.218179Z"
                                      "verificationMethod" "did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"
                                      "proofPurpose" "assertionMethod"
                                      "jws" "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19.eyJAY29udGV4dCI6WyJodHRwczovL3d3dy53My5vcmcvMjAxOC9jcmVkZW50aWFscy92MSIsImh0dHBzOi8vZmx1ci5lZS9ucy9ibG9jayJdLCJjcmVkZW50aWFsU3ViamVjdCI6eyJvdGhlciI6ImRhdGEiLCJzb21lIjoiZGF0YSJ9LCJpc3N1YW5jZURhdGUiOiIyMDIwLTAxLTAxVDIxOjI2OjI5LjIxODE3OVoiLCJpc3N1ZXIiOiJkaWQ6Zmx1cmVlOlRmREhBdFRZUU1XTjlmWGlFSGJ5TUdpekwzaHJ1R3M5d3VqIiwidHlwZSI6WyJWZXJpZmlhYmxlQ3JlZGVudGlhbCJdfQ==.GzBFAiEAnZhKotqoLaHTfJagZ-0JMfbh0M29yB9JP2BEEkLAhIsCIGecqP15NdpU92gd9-n3LLnauuexJeSVxk94NU1ZrRxA"}}
                :normalized "{\"@context\":[\"https://www.w3.org/2018/credentials/v1\",\"https://flur.ee/ns/block\"],\"credentialSubject\":{\"other\":\"data\",\"some\":\"data\"},\"issuanceDate\":\"2020-01-01T21:26:29.218179Z\",\"issuer\":\"did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj\",\"type\":[\"VerifiableCredential\"]}"}
               (cred/add-proof {"@context"          ["https://www.w3.org/2018/credentials/v1"
                                                     "https://flur.ee/ns/block"]
                                "issuer"            issuer
                                "type"              ["VerifiableCredential"]
                                "issuanceDate"      "2020-01-01T21:26:29.218179Z"
                                "credentialSubject" {"some" "data" "other" "data"}}
                               (:private keypair))))))

  (testing "generate"
    (with-redefs [fluree.db.util.core/current-time-iso (constantly "2020-01-01T21:26:29.218179Z")]
      (is (= {:credential {"@context" ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"]
                           "id" "blah"
                           "type" ["VerifiableCredential"]
                           "issuer" "did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"
                           "issuanceDate" "2020-01-01T21:26:29.218179Z"
                           "credentialSubject" {"some" "data"
                                                "other" "data"}
                           "proof" {"type" "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020"
                                    "created" "2020-01-01T21:26:29.218179Z"
                                    "verificationMethod" "did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"
                                    "proofPurpose" "assertionMethod"
                                    "jws" "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19.eyJAY29udGV4dCI6WyJodHRwczovL3d3dy53My5vcmcvMjAxOC9jcmVkZW50aWFscy92MSIsImh0dHBzOi8vZmx1ci5lZS9ucy9ibG9jayJdLCJjcmVkZW50aWFsU3ViamVjdCI6eyJvdGhlciI6ImRhdGEiLCJzb21lIjoiZGF0YSJ9LCJpZCI6ImJsYWgiLCJpc3N1YW5jZURhdGUiOiIyMDIwLTAxLTAxVDIxOjI2OjI5LjIxODE3OVoiLCJpc3N1ZXIiOiJkaWQ6Zmx1cmVlOlRmREhBdFRZUU1XTjlmWGlFSGJ5TUdpekwzaHJ1R3M5d3VqIiwidHlwZSI6WyJWZXJpZmlhYmxlQ3JlZGVudGlhbCJdfQ==.HDBEAiBO-aK2XXhv768JStpcIC943PSLlFJXwMfJK9tU9fc3uQIgMB8oU446Uy4RMQz-uyh_sSMZdICIXn5R3wQX2OvGjkU="}}
              :normalized "{\"@context\":[\"https://www.w3.org/2018/credentials/v1\",\"https://flur.ee/ns/block\"],\"credentialSubject\":{\"other\":\"data\",\"some\":\"data\"},\"id\":\"blah\",\"issuanceDate\":\"2020-01-01T21:26:29.218179Z\",\"issuer\":\"did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj\",\"type\":[\"VerifiableCredential\"]}"}
             (cred/generate {"some" "data" "other" "data"} {:private (:private keypair)
                                                            :did {:id issuer}})))))

  (testing "verify"
    (with-redefs [fluree.db.util.core/current-time-iso (constantly "2020-01-01T21:26:29.218179Z")]
      (let [{valid-cred :credential} (cred/generate credential-subject {:private (:private keypair)
                                                                        :did {:id issuer}})

            {unknown-header-cred :credential}
            (with-redefs [cred/jws-header-b64 (alphabase/base-to-base "INVALID-HEADER" :string :base64)]
              (cred/generate credential-subject {:private (:private keypair)
                                                 :did {:id issuer}}))

            {mismatched-signature-cred :credential}
            (cred/generate credential-subject {:private (:private wrong-keypair)
                                               :did {:id issuer}})]
        (is (nil? (:errors (cred/verify valid-cred))))
        (is (= [{:error :credential/unknown-signing-algorithm
                 :message "Unsupported jws header in credential: INVALID-HEADER"}
                {:error :credential/invalid-signature
                 :message "Derived did from signature does not match did in 'proof' of credential. Derived: did:fluree:TfK5VYrww2ED7MShxfiDesAuwx66fGWoLbQ, proof verificationMethod: did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"}]
               (:errors (cred/verify unknown-header-cred))))
        (is (= [{:error :credential/invalid-signature
                 :message "Derived did from signature does not match did in 'proof' of credential. Derived: did:fluree:TfAhAXucJEJW1cJMzcpp9mfSQBKYjV535Z3, proof verificationMethod: did:fluree:TfDHAtTYQMWN9fXiEHbyMGizL3hruGs9wuj"}]
               (:errors (cred/verify mismatched-signature-cred))))))))
