(ns fluree.db.json-ld.credential
  (:require [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [alphabase.core :as alphabase]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

(def jws-header
  "The JWS header for a secp256k1 signing key."
  ;; https://github.com/decentralized-identity/EcdsaSecp256k1RecoverySignature2020
  {"alg"  "ES256K-R"
   "b64"  false
   "crit" ["b64"]})

(def jws-header-json (json-ld/normalize-data jws-header {:algorithm :basic
                                                         :format    :application/json}))
;; TODO - below encoding should be :base64URL once supported
(def jws-header-b64 (alphabase/base-to-base jws-header-json :string :base64))

(defn credential-json
  "Takes final credential response object (as returned by sign-credential)
  and formats JSON document ready for publishing."
  [credential-object]
  (let [{:keys [credential normalized]} credential-object
        proof            (get credential "proof")
        ;; TODO: implement URDNA2015
        proof-normalized (json-ld/normalize-data proof)]
    (str (subs normalized 0 (dec (count normalized)))       ;; remove trailing '}', then add back
         ",\"proof\":" proof-normalized "}")))

(defn signing-input
  "JOSE JWS signing input is b64URL of header, + '.' + b64URL of json to be signed.
  The resulting input is hashed with SHA-256, and that result is what is signed."
  [payload]
  ;; TODO: below should be :base64URL once supported
  (str jws-header-b64 "." (alphabase/base-to-base payload :string :base64)))

(defn serialize-jws
  "Serialize a JWS according to the JOSE specification, compact form."
  [signing-input signature]
  (str signing-input "." signature))

(defn deserialize-jws
  "Deserialize a compact JWS into its component parts"
  [jws]
  (let [[header payload sig] (str/split jws #"\.")]
    ;; TODO: convert from base64URL
    {:header    (alphabase/base-to-base header :base64 :string)
     :payload   (alphabase/base-to-base payload :base64 :string)
     :signature (alphabase/base-to-base sig :base64 :hex)}))

(defn sign
  "Given a payload and a signing key, returns a JOSE JSON Web Signature."
  [payload signing-key]
  (let [signing-input (signing-input payload)
        signature     (crypto/sign-message signing-input signing-key)
        ;; TODO: use base64URL encoding
        b64-signature (alphabase/base-to-base signature :hex :base64)]
    (serialize-jws signing-input b64-signature)))

(defn add-proof
  "Adds a proof document containing a JWS to a VerifiableCredential."
  [credential private-key]
  ;; TODO: this is using a custom proof @type, and it should use EcdsaSecp256k1RecoverySignature2020 which requires RDF normalization
  (let [payload-json  (json-ld/normalize-data credential)
        did           (get credential "issuer")
        proof         {"type"               "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020"
                       "created"            (util/current-time-iso)
                       "verificationMethod" did
                       "proofPurpose"       "assertionMethod"
                       "jws"                (sign payload-json private-key)}]
    {:credential (assoc credential "proof" proof)
     :normalized payload-json}))

(defn generate
  "Generate a VerifiableCredential given a subject and some issuer opts."
  [credential-subject opts]
  (let [{:keys [did private]} opts
        did* (or (:id did)
                 (str "did:fluree:" (crypto/account-id-from-private private)))]
    (add-proof {"@context" ["https://www.w3.org/2018/credentials/v1"
                            "https://flur.ee/ns/block"]
                ;; TODO: use a useful id
                "id" "blah"
                "type" ["VerifiableCredential"]
                "issuer" did*
                "issuanceDate" (util/current-time-iso)
                "credentialSubject" credential-subject}

               private)))

(defn decode-credential
  [credential-json]
  (json/parse credential-json false))

(defn verify
  "Takes a credential"
  [credential]
  (let [proof-did     (get-in credential ["proof" "verificationMethod"])
        jws           (get-in credential ["proof" "jws"])
        {:keys [header payload signature]} (deserialize-jws jws)
        signing-input (jws-signing-input payload)
        key-id        (crypto/account-id-from-message signing-input signature)
        derived-did   (str "did:fluree:" key-id)]
    (cond-> {:credential credential}
      (not= jws-header-json header)
      (update :errors (fnil conj []) (str "Unsupported jws header in credential: " header))

      (not= derived-did proof-did)
      (update :errors (fnil conj [])
              (str "Derived did from signature does not match did in 'proof' of credential. Derived: " derived-did ", proof verificationMethod: " proof-did)))))

(comment
  (def kp (crypto/generate-key-pair))
  (def payload {"@context"          ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"],
                "id"                "blah",
                "type"              ["VerifiableCredential" "Commit"],
                "issuer"            "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
                "issuanceDate"      "SOMEDATE",
                "credentialSubject" {"@context" ["https://flur.ee/ns/block"
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
                                               {"schema:name" "Douglas Adams" , "type" "schema:Person" , "id" "wiki:Q42"}]}})

  (def cred (sign payload (:private kp)))

  (verify-jws (:credential cred))

  (def jws (-> cred :credential (get "proof") (get "jws")))

  (= (json-ld/normalize-data payload)
     (:payload (jose-deserialize-compact jws)))

  (verify-jws (:credential cred))
  ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6" "did:fluree:Tf25LUDWuX57vWi2ZddXxmkQLWv5TD5UR5h"]




  (-> (:credential cred)
      (json/stringify )
      (json/parse false)
      (get "proof")
      (dissoc "jws"))
  {"type" "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020", "created" "2022-01-26T17:41:18.374232Z", "verificationMethod" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6", "proofPurpose" "assertionMethod"}


  (:normalized cred)



  (verify cred)
  ,)
