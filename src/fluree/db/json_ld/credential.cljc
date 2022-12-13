(ns fluree.db.json-ld.credential
  (:require [alphabase.core :as alphabase]
            [fluree.db.util.async :refer [go-try]]
            [clojure.string :as str]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]
            #?(:cljs [cljs.core.async.interop :refer-macros [<p!]])
            [fluree.json-ld.processor.api :as jld-processor]
            [fluree.db.did :as did]))

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
  ([credential-subject private] (generate credential-subject private (did/private->did-map private)))
  ([credential-subject private did]
   (go-try
     {"@context"          "https://www.w3.org/2018/credentials/v1"
      "id"                ""
      "type"              ["VerifiableCredential" "CommitProof"]
      "issuer"            (:id did)
      "issuanceDate"      (util/current-time-iso)
      "credentialSubject" credential-subject
      "proof"             #?(:clj (create-proof (-> (jld-processor/canonize credential-subject)
                                                    (crypto/sha2-256))
                                                (did/encode-did-key (:public did))
                                                private)
                             :cljs (let [canonicalized (<p! (jld-processor/canonize credential-subject))]
                                     (create-proof (crypto/sha2-256 canonicalized)
                                                   (did/encode-did-key (:public did))
                                                   private)))})))

(defn verify
  "Takes a credential and returns the credential subject and issuer id if it verifies. If
  credential does not have a jws returns the credential without verifying it. If the
  credential is invalid an exception will be thrown."
  [credential]
  (go-try
    (if-let [jws (get-in credential ["proof" "jws"])]
      (try*
        (let [subject (get credential "credentialSubject")
              issuer  (get credential "issuer")
              {:keys [header signature]} (deserialize-jws jws)

              signing-input #?(:clj (-> (jld-processor/canonize subject)
                                        (crypto/sha2-256))
                               :cljs (<p! (-> (jld-processor/canonize subject)
                                              (.then (fn [res] (crypto/sha2-256 res))))))

              proof-did     (get-in credential ["proof" "verificationMethod"])
              pubkey        (did/decode-did-key proof-did)]
          (when (not= jws-header-json header)
            (throw (ex-info "Unsupported jws header in credential."
                            {:error :credential/unknown-signing-algorithm
                             :supported-header jws-header-json
                             :header header
                             :credential credential})))

          (when (not (crypto/verify-signature pubkey signing-input signature))
            (throw (ex-info "Verification failed." {:error :credential/invalid-signature :credential credential})))
          ;; everything is good
          {:subject subject :issuer issuer})
        (catch* e
                (throw (ex-info "Unverifiable credential"
                                {:credential credential
                                 :error :credential/unverifiable
                                 :message e}))))
      {:subject credential})))
