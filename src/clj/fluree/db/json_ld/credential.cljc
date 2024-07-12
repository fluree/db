(ns fluree.db.json-ld.credential
  (:require [alphabase.core :as alphabase]
            [clojure.string :as str]
            #?(:cljs [cljs.core.async.interop :refer-macros [<p!]])
            [fluree.crypto :as crypto]
            [fluree.db.did :as did]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.json-ld.processor.api :as jld-processor]))

#?(:clj (set! *warn-on-reflection* true))

(def jws-header
  "The JWS header for a secp256k1 signing key."
  ;; https://github.com/decentralized-identity/EcdsaSecp256k1RecoverySignature2020
  {"alg"  "ES256K-R"
   "b64"  false
   "crit" ["b64"]})

(def jws-header-json (json-ld/normalize-data jws-header {:algorithm :basic
                                                         :format    :application/json}))

(def jws-header-b64 (alphabase/base-to-base jws-header-json :string :base64url))

(defn serialize-jws
  "Serialize a JWS according to the JOSE specification, compact form."
  [signature]
  (str jws-header-b64 ".." signature))

(defn deserialize-jws
  "Deserialize a compact JWS into its component parts"
  [jws]
  (let [[header payload sig] (str/split jws #"\.")]
    {:header    (alphabase/base-to-base header :base64url :string)
     :payload   (alphabase/base-to-base payload :base64url :string)
     :signature (alphabase/base-to-base sig :base64url :hex)}))

(defn sign
  "Given a payload and a signing key, returns a JOSE JSON Web Signature."
  [payload signing-key]
  (let [signature     (crypto/sign-message payload signing-key)
        b64-signature (alphabase/base-to-base signature :hex :base64url)]
    (serialize-jws b64-signature)))

(defn create-proof
  "Given the sha256 hash of the canonicalized credential-subject, create a proof for it."
  [credential-subject-hash did-key signing-key]
  {"type"               "EcdsaSecp256k1RecoverySignature2020"
   "created"            (util/current-time-iso)
   "verificationMethod" did-key
   "proofPurpose"       "assertionMethod"
   "jws"                (sign credential-subject-hash signing-key)})

(defn generate
  "Generate a VerifiableCredential given a subject and some issuer opts."
  ([credential-subject private]
   (generate credential-subject private (did/private->did-map private)))
  ([credential-subject private did]
   (go-try
    (let [canonicalized #?(:clj (jld-processor/canonize credential-subject)
                           :cljs (<p! (jld-processor/canonize credential-subject)))

           ;; TODO: assert this once our credential subjects are proper json-ld
           ;; _ (when (= "" canonicalized) (throw (ex-info "Unsupported credential subject" {:credential-subject credential-subject})))

          did-key (did/encode-did-key (:public did))
          proof (create-proof (crypto/sha2-256 canonicalized)
                              did-key
                              private)]
      {"@context"          "https://www.w3.org/2018/credentials/v1"
       "id"                ""
       "type"              ["VerifiableCredential" "CommitProof"]
       "issuer"            (:id did)
       "issuanceDate"      (util/current-time-iso)
       "credentialSubject" credential-subject
       "proof"             proof}))))

(defn verify-credential
  "Takes a credential and returns the credential subject and signing did if it
  verifies. If credential does not have a jws returns nil. If the credential is invalid
  an exception will be thrown."
  [credential]
  (go-try
   (when-let [jws (get-in credential ["proof" "jws"])]
     (let [subject (get credential "credentialSubject")
           {:keys [header signature]} (deserialize-jws jws)

           signing-input #?(:clj (-> (jld-processor/canonize subject)
                                     (crypto/sha2-256))
                            :cljs (<p! (-> (jld-processor/canonize subject)
                                           (.then (fn [res] (crypto/sha2-256 res))))))

           proof-did     (get-in credential ["proof" "verificationMethod"])
           pubkey        (did/decode-did-key proof-did)
           id            (crypto/account-id-from-public pubkey)
           auth-did      (did/auth-id->did id)]
       (when (not= jws-header-json header)
         (throw (ex-info "Unsupported jws header in credential."
                         {:error :credential/unknown-signing-algorithm
                          :supported-header jws-header-json
                          :header header
                          :credential credential})))

       (when (not (crypto/verify-signature pubkey signing-input signature))
         (throw (ex-info "Verification failed." {:error :credential/invalid-signature :credential credential})))
        ;; everything is good
       {:subject subject :did auth-did}))))

(defn verify-jws
  [jws]
  (let [{:keys [payload pubkey]} (crypto/verify-jws jws)
        id                       (crypto/account-id-from-public pubkey)
        auth-did                 (did/auth-id->did id)]
    {:subject (json/parse payload false) :did auth-did}))

(defn jws?
  [signed-transaction]
  (string? signed-transaction))

(defn verify
  "Verifies a signed query/transaction. Returns keys:
  {:subject <original tx/cmd> :did <did>}

  Will throw if no :did is detected."
  [signed-command]
  (go-try
   (let [result (if (jws? signed-command)
                  (verify-jws signed-command)
                  (<? (verify-credential signed-command)))]
     (if (:did result)
       result
       (throw (ex-info "Signed message could not be verified to an identity"
                       {:status 401
                        :error  :db/invalid-credential}))))))
